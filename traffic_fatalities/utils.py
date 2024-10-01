import re
from geopy.distance import geodesic

def get_bounding_box(lat, lon, box_size_meters=200):

    d = geodesic(meters=box_size_meters / 2)
    north, south = d.destination((lat, lon), 0).latitude, d.destination((lat, lon), 180).latitude
    east, west = d.destination((lat, lon), 90).longitude, d.destination((lat, lon), 270).longitude
    
    return north, south, east, west

def normalize_street_name(string):
    definitions = {
        r'Street': 'ST',
        r'Avenue': 'AV',
        r'Drive': 'DR',
        r'Road': 'RD',
        r'Boulevard': 'BL',
        r'Way': 'WY',
        r'Circle': 'CIR',
        r'Court': 'CT',
        r'Lane': 'LN',
        r'Highway': 'HWY',
        r'Junior': 'JR'
    }
    for pattern, replacement in definitions.items():
        string = re.sub(pattern=pattern, repl=replacement, string=string)
    return string.upper()