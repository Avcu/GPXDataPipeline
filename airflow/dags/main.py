import os
import json
import gpxpy
import gpxpy.gpx

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from functions import *

_OUTPUT_LEN = 60
_RUN_KM_H_THRESHOLD = 15

LOCATIONS = {
    "Los Angeles, CA": (34.01842, -118.29528), 
    # "Ankara, TR": (39.86538, 32.74836), 
    # "Manisa, TR": (38.73484, 27.56861),
    "Seattle, WA": (47.60785, -122.33651),
    "Toronto, ON": (43.67282, -79.40001),
    "New York, NY": (40.72544, -73.99678)
    }
GEO_DATA_INPUT = os.path.join("geo_data", "gpx_input_files")
GEO_DATA_OUTPUT = os.path.join("geo_data", "results")

def task_process_geo_data():
    path_this = os.getcwd()
    src_full_path = os.path.join(path_this, GEO_DATA_INPUT)

    # STEP-0
    print("="*_OUTPUT_LEN)
    print("STEP-0")
    print("-"*_OUTPUT_LEN)
    print("Looking for gpx files in the following path: {}".format(src_full_path))
    gpx_files = [f for f in os.listdir(src_full_path) if f.lower().endswith('.gpx')]

    if len(gpx_files)==0:
        assert 1==0, "No gpx files have been found, please try another source directory"
    else:
        print("{} gpx files have been found!".format(len(gpx_files)))
    print("-"*_OUTPUT_LEN)

    # STEP-1
    print("="*_OUTPUT_LEN)
    print("STEP-1")
    print("-"*_OUTPUT_LEN)
    print("Iterating through the detected gpx files...")

    num_file = len(gpx_files)
    track_data = {}
    for file_idx in range(num_file): # num_file
        track_data[file_idx] = {}
        track_data[file_idx]['file_name'] = gpx_files[file_idx]
        print('File number: {} and name: {}'.format(file_idx, gpx_files[file_idx]))

        # load the data
        cur_gpx_file = open(os.path.join(src_full_path, gpx_files[file_idx]), 'r')
        cur_gpx = gpxpy.parse(cur_gpx_file)

        # iterate through tracks
        track_data[file_idx]['tracks'] = []
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

            track_data[file_idx]['tracks'].append(track_dict)

            print("--> Location: {}".format(loc_))
            print("--> Date: {}-{}-{}".format(track_dict['year'], track_dict['month'], track_dict['day']))
            print("--> Total distance in km: {} and total time in min: {}, average speed: {} km/h".format(tot_distance, tot_time/60, tot_distance/(tot_time/3600) if tot_time > 0 else 0))

    # STEP-2
    print("="*_OUTPUT_LEN)
    print("STEP-2")
    print("-"*_OUTPUT_LEN)
    print("Extract the statistics and save as a JSON file...")

    running_stats = extract_statistics(track_data, LOCATIONS, type='running')
    biking_stats = extract_statistics(track_data, LOCATIONS, type='biking')

    # Ensure the directory exists
    os.makedirs(GEO_DATA_OUTPUT, exist_ok=True)

    with open(f"{GEO_DATA_OUTPUT}/running_stats.json", "w") as f:
        json.dump(running_stats, f)
    with open(f"{GEO_DATA_OUTPUT}/biking_stats.json", "w") as f:
        json.dump(biking_stats, f)

    print("Statistics are saved as JSON file.")

    # STEP-3
    print("="*_OUTPUT_LEN)
    print("STEP-3")
    print("-"*_OUTPUT_LEN)
    print("Create KML file...")

    running_kml = create_kml_str(track_data, type='running')
    biking_kml = create_kml_str(track_data, type='biking')

    print("Saving the KML file...")
    with open(f"{GEO_DATA_OUTPUT}/tracks_run.kml", "w") as f:
        f.write(running_kml)
    with open(f"{GEO_DATA_OUTPUT}/tracks_bike.kml", "w") as f:
        f.write(biking_kml)
    print("KML files are saved")

    
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