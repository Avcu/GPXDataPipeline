from math import radians, cos, sin, asin, sqrt, degrees, pi, atan2

_AVG_EARTH_RADIUS_KM = 6371.0088

# Following code is originated from haversine library: https://github.com/mapado/haversine
def calculateDistanceInKM(point1, point2):
    """ Calculate the great-circle distance between two points on the Earth surface in km.
    Takes two 2-tuples, containing the latitude and longitude of each point in decimal degrees.
    :param point1: first point; tuple of (latitude, longitude) in decimal degrees
    :param point2: second point; tuple of (latitude, longitude) in decimal degrees
    Example:
        lyon = (45.7597, 4.8422)        # lat, long
        paris = (48.8567, 2.3508)       # lat, long

        print("Distance: {}".format(calculateDistanceInKM(lyon, paris)))
    """

    # unpack latitude/longitude
    lat1, lng1 = point1
    lat2, lng2 = point2

    # convert all latitudes/longitudes from decimal degrees to radians
    lat1 = radians(lat1)
    lng1 = radians(lng1)
    lat2 = radians(lat2)
    lng2 = radians(lng2)

    # calculate haversine
    lat = lat2 - lat1
    lng = lng2 - lng1
    d = sin(lat * 0.5) ** 2 + cos(lat1) * cos(lat2) * sin(lng * 0.5) ** 2

    return 2 * _AVG_EARTH_RADIUS_KM * asin(sqrt(d))

def find_location(lat, long, LOCATIONS):
    num_loc = len(LOCATIONS)
    assert num_loc > 0, "Please provide a LOCATIONS array!"
    min_dist = float('inf')
    min_loc = None
    for k, v in LOCATIONS.items():
        cur_dist = calculateDistanceInKM((lat, long), v)
        if cur_dist < min_dist:
            min_dist = cur_dist
            min_loc = k
    return min_loc

def extract_statistics(data_dict, locations, type="running"):
    statistics_dict = {}
    statistics_dict['num_track'] = 0
    statistics_dict['tot_distance_km'] = 0
    statistics_dict['cities'] = {
        city: {
            'tot_distance_km': 0.0,
            'num_track': 0
        }
        for city in locations.keys()
    }
    
    for v in data_dict.values():
        if v.get('tracks') and v['tracks'][0]['type'] == type:
            statistics_dict['num_track'] += 1
            dist = v['tracks'][0]['tot_distance_km']
            statistics_dict['tot_distance_km'] += dist

            loc = v['tracks'][0]['location']
            if loc in statistics_dict['cities']:
                statistics_dict['cities'][loc]['tot_distance_km'] += dist
                statistics_dict['cities'][loc]['num_track'] += 1
            elif loc is not None:
                statistics_dict['cities'][loc] = {
                    'tot_distance_km': dist,
                    'num_track': 1
                }

    return statistics_dict

def create_kml_str(data_dict, type="running"):
    kml_str = ""
    print("-- Adding the head...")
    kml_str += """<?xml version="1.0" encoding="UTF-8"?>
    <kml xmlns="http://earth.google.com/kml/2.1">

    <Document>
    <name>My Tracks</name>
    <description>Running and biking tracks in Los Angeles</description>
    """

    for v in data_dict.values():
        if v['tracks'][0]['type'] == type:
            placemarker_str = "<Placemark>\n"
            placemarker_str += "<name>"
            placemarker_str += "dist: {:.2f}km, time: {:.1f}min".format( 
                                v['tracks'][0]['tot_distance_km'], v['tracks'][0]['tot_time_min'])
            placemarker_str += "</name>\n"

            placemarker_str += """<LineString><altitudeMode>relative</altitudeMode><coordinates>\n"""
            
            for i in range(len(v['tracks'][0]['points'])):
                lat, long, _ = v['tracks'][0]['points'][i]
                placemarker_str += "{},{},{}\n".format(long, lat, 0)

            placemarker_str += """</coordinates></LineString></Placemark>\n\n"""
            kml_str += placemarker_str

    print("-- Adding the tail...")
    kml_str += """</Document>
    </kml>
    """

    return kml_str