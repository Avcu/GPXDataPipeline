import os
import json
import gpxpy
import gpxpy.gpx

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from fit2gpx import StravaConverter

from functions import *

# Constants
_RUN_KM_H_THRESHOLD = 15 # this is how running/walking activities are separated from biking
LOCATIONS = {
    "Los Angeles, CA": [(34.01842, -118.29528), 'los_angeles'],
    "Seattle, WA": [(47.60785, -122.33651), 'seattle'],
    "Toronto, ON": [(43.67282, -79.40001), 'toronto'],
    "New York, NY": [(40.72544, -73.99678), 'new_york']
    }
GEO_DATA_INPUT_PATH = os.path.join("geo_data", "gpx_input_files")
STRAVA_DATA_PATH = os.path.join(GEO_DATA_INPUT_PATH, "strava")
STRAVA_GPX_DATA_PATH = os.path.join(GEO_DATA_INPUT_PATH, "strava_gpx")
NIKE_GPX_DATA_PATH = os.path.join(GEO_DATA_INPUT_PATH, "nike_run_club")

GEO_DATA_JSON_OUTPUT_PATH = os.path.join("geo_data", "results", "json_files")
GEO_DATA_KML_OUTPUT_PATH = os.path.join("geo_data", "results", "kml_files")

def task_process_geo_data():
    # STEP-1 - Process activities from different sources: Strava, Nike Run Club etc and normalize
    print("# STEP-1 - Process activities from different sources: Strava, Nike Run Club etc and normalize")
    strava_conv = StravaConverter(dir_in=STRAVA_DATA_PATH, dir_out=STRAVA_GPX_DATA_PATH)

    # Unzip the zipped files
    strava_conv.unzip_activities()

    # Add metadata to existing GPX files
    strava_conv.add_metadata_to_gpx()

    # Convert FIT to GPX files
    strava_conv.strava_fit_to_gpx()

    # STEP-2 - Find the list of generated GPX files
    print("STEP-2 - Find the list of generated GPX files")
    gpx_source_dirs = [STRAVA_GPX_DATA_PATH, NIKE_GPX_DATA_PATH]
    print("Looking for gpx files in the following paths: {}".format(gpx_source_dirs))
    gpx_files = [
        os.path.join(src_dir, f)
        for src_dir in gpx_source_dirs
        if os.path.isdir(src_dir)
        for f in os.listdir(src_dir)
        if f.lower().endswith('.gpx')
    ]

    if len(gpx_files) == 0:
        assert 1==0, "No gpx files have been found!"
    else:
        print("{} gpx files have been found!".format(len(gpx_files)))

    # STEP-3 - Read GPX files
    print("STEP-3 - Read GPX files")

    num_file = len(gpx_files)
    track_data = {}
    for file_idx in range(num_file):
        track_data[file_idx] = {}
        track_data[file_idx]['file_name'] = os.path.basename(gpx_files[file_idx])
        print('File number: {} and name: {}'.format(file_idx, gpx_files[file_idx]))

        # load the data
        cur_gpx_file = open(gpx_files[file_idx], 'r')
        cur_gpx = gpxpy.parse(cur_gpx_file)

        # iterate through tracks
        track_data[file_idx]['tracks'] = None
        for track in cur_gpx.tracks:

            track_dict = {}
            track_dict['points'] = []

            # Use gpxpy's built-in get_moving_data
            moving_data = track.get_moving_data()
            if moving_data and moving_data.moving_time > 0:
                tot_distance = moving_data.moving_distance / 1000
                tot_time = moving_data.moving_time
            else:
                tot_distance = track.length_2d() / 1000
                duration = track.get_duration()
                tot_time = duration if duration else 0

            # Collect points from all segments to preserve the full route map
            loc_ = None
            for segment in track.segments:
                for point in segment.points:
                    track_dict['points'].append((point.latitude, point.longitude, point.time.timestamp() if point.time else 0))

            # Determine the starting location from the first coordinate
            if len(track_dict['points']) > 0:
                first_pt = track_dict['points'][0]
                loc_ = find_location(first_pt[0], first_pt[1], LOCATIONS)

            track_dict['location'] = loc_
            track_dict['tot_distance_km'] = tot_distance
            track_dict['tot_time_min'] = tot_time / 60
            track_dict['ave_speed_km_h'] = tot_distance / (tot_time / 3600) if tot_time > 0 else 0

            # Safely fetch year, month, and day of the track's start
            start_time, _ = track.get_time_bounds()
            if start_time:
                track_dict['year'] = start_time.year
                track_dict['month'] = start_time.month
                track_dict['day'] = start_time.day
            elif len(track_dict['points']) > 0 and len(track.segments) > 0 and len(track.segments[0].points) > 0 and track.segments[0].points[0].time:
                first_time = track.segments[0].points[0].time
                track_dict['year'] = first_time.year
                track_dict['month'] = first_time.month
                track_dict['day'] = first_time.day
            else:
                now = datetime.now()
                track_dict['year'] = now.year
                track_dict['month'] = now.month
                track_dict['day'] = now.day

            if track_dict['ave_speed_km_h'] > _RUN_KM_H_THRESHOLD:
                track_dict['type'] = 'biking'
            else:
                track_dict['type'] = 'running'

            track_data[file_idx]['tracks'] = track_dict

            print("--> Location: {}".format(track_dict['location']))
            print("--> Date: {}-{}-{}".format(track_dict['year'], track_dict['month'], track_dict['day']))
            print("--> Total distance in km: {} and total time in min: {}, average speed: {} km/h".format(tot_distance, tot_time/60, tot_distance/(tot_time/3600) if tot_time > 0 else 0))

    # STEP-4 - Extract the statistics and save as JSON file
    print("STEP-4 - Extract the statistics and save as JSON file")

    running_stats = extract_statistics(track_data, LOCATIONS, type='running')
    biking_stats = extract_statistics(track_data, LOCATIONS, type='biking')

    # Ensure the directory exists
    os.makedirs(GEO_DATA_JSON_OUTPUT_PATH, exist_ok=True)

    with open(f"{GEO_DATA_JSON_OUTPUT_PATH}/running_stats.json", "w") as f:
        json.dump(running_stats, f)
    with open(f"{GEO_DATA_JSON_OUTPUT_PATH}/biking_stats.json", "w") as f:
        json.dump(biking_stats, f)

    # STEP-5 - Create KML files by city
    print("STEP-5 - Create KML files by city")

    # Ensure the directory exists
    os.makedirs(GEO_DATA_KML_OUTPUT_PATH, exist_ok=True)

    for k, v in LOCATIONS.items():
        running_kml = create_kml_str(track_data, k, type='running')
        biking_kml = create_kml_str(track_data, k, type='biking')

        print("Saving the KML file for {}.".format(k))
        with open(f"{GEO_DATA_KML_OUTPUT_PATH}/tracks_run_{v[1]}.kml", "w") as f:
            f.write(running_kml)
        with open(f"{GEO_DATA_KML_OUTPUT_PATH}/tracks_bike_{v[1]}.kml", "w") as f:
            f.write(biking_kml)

    
# Define the DAG
with DAG(
    dag_id="simple_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None, # @hourly
    catchup=False
) as dag:

    # Define tasks
    t1 = PythonOperator(
        task_id="task_process_geo_data",
        python_callable=task_process_geo_data,
    )

    t1