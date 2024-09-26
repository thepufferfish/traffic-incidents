import re
from geopy.distance import geodesic

def get_bounding_box(lat, lon, box_size_meters=200):

    d = geodesic(meters=box_size_meters / 2)
    north, south = d.destination((lat, lon), 0).latitude, d.destination((lat, lon), 180).latitude
    east, west = d.destination((lat, lon), 90).longitude, d.destination((lat, lon), 270).longitude
    
    return north, south, east, west

def get_county_name(address):
    match = re.search(r'(?<=, )[A-z\s]+(?=, CA, [0-9]{5}$)', address)
    if match:
        return match.group(0)

def get_city_name(address):
    match = re.search(r'(?<=, )[A-z\s]+(?=, Sacramento, CA, [0-9]{5}$)', address)
    if match:
        return match.group(0)
    
def remove_direction_of_travel(address):
    direction_patterns = [r'\b(north|east|south|west)bound\b', r'\b[nesw]/?b\b']
    for pattern in direction_patterns:
        address = re.sub(pattern, '', address)
    return address.strip()

def clean_address(address):
    if address:
        address = re.sub(r', [A-z\s,]+, Sacramento, CA, [0-9]{5}$', '', address).lower().strip()
        address = remove_direction_of_travel(address)
        return address

def detect_highway(address):
    highway_patterns = [
        r'^s/?r[ \-]?[0-9]{1,3}',
        r'^i-?[580]{1,2}',
        r'^(us )?(hwy|highway) [0-9]{2,3}',
        r'^(us|ca)[\s\-][0-9]{2}',
        r'^interstate [0-9]{1,2}'
    ]
    return any(re.search(pattern, address) for pattern in highway_patterns)

def detect_intersection(address, ):
    intersection_patterns = [
        r'(\bat\b)|&|(\band\b)',
        r'\b0?\.?[0-9]+\'?\s?(ft|feet|yards|mi|miles|mile)?\.?\s[nwse]\.?(orth|est|outh|ast)?\sof\b'
    ]
    return any(re.search(pattern, address) for pattern in intersection_patterns)

def detect_exact_address(address):
    match = re.search(r'[0-9]{3,4} [A-z0-9\s]+\.?$', address)
    return any([match])

def extract_exact_address(address):
    match = re.search(r'[0-9]{3,4} [A-z0-9\s]+\.?$', address)
    if match:
        return match.group(0)
    
def replace_road_abbrevs(address):
    abbreviations = {
        "rd": "road",
        "st": "street",
        "ave": "avenue",
        "blvd": "boulevard",
        "dr": "drive",
        "ln": "lane",
        "ct": "court",
        'wy': 'way'
    }
    for abbr, full_form in abbreviations.items():
        address = re.sub(r"\b" + abbr + r"\b", full_form, address)
    return address

def extract_cross_streets(address):
    address = re.sub('^(at the )?intersection of ', '', address)
    address = re.sub('((at )?the )?intersection (of|with) ', '& ', address)
    pattern = r'(\b[0-9]{0,2}\.?[0-9]+\'?\s?(ft|feet|yards|mi|miles|mile)?\.?\s[nwse]\.?(orth|est|outh|ast)?\sof\b)'
    has_distance = re.search(r'[0-9]{,2}\.?[0-9]+\'?\s?(ft|feet|yards|mi|miles|mile)', address)
    if has_distance:
        distance = has_distance.group(0)
        unit = re.search('(\'|ft|feet|yards|mi|miles|mile)', distance)
        if unit:
            unit = unit.group(0)
        distance = re.search(r'[0-9]{,2}\.?[0-9]+', distance)
        if distance:
            distance = distance.group(0)
        if distance and unit:
            distance = float(distance)
            if re.search(r'mi', unit):
                distance = distance * 5280
            if re.search(r'yard', unit):
                distance = distance * 3
    else:
        distance = None
    address = re.sub(pattern, '& ', address)
    address = re.sub(' {2,}', ' ', address)
    address = re.sub('\.', '', address)
    cross_streets = re.split(r',?\s+and\s+|,?\s+&\s+|,?\s+at\s+', address)
    return [replace_road_abbrevs(x).strip() for x in cross_streets], distance

def get_address_for_geocoding(df):
    for index, row in df.iterrows():
        city = row['City']
        zip_code = re.search(r'[0-9]{5}$', row['IncidentAddress'])
        if zip_code:
            zip_code = zip_code.group(0)
        else:
            zip_code = ''
        if row['IsIntersection']:
            cross_streets = ' & '.join(row['CrossStreets'])
            cross_streets = cross_streets
            address = f'{cross_streets}, {city}, CA {zip_code}'
            row['GeocodeAddress'] = address
        elif row['IsExactAddress']:
            exact_address = row['ExactAddress']
            address = f'{exact_address}, {city}, CA {zip_code}'
            row['GeocodeAddress'] = address
        else:
            row['GeocodeAddress'] = None
        df.at[index, 'GeocodeAddress'] = row['GeocodeAddress']
    return df

def find_closest_node(lat, lon, nodes):
    closest_node = None
    min_distance = float('inf')
    for osmid, node in nodes.iterrows():
        node_lat = node['y']
        node_lon = node['x']
        distance = geodesic((lat, lon), (node_lat, node_lon)).feet
        if distance < min_distance:
            min_distance = distance
            closest_node = osmid
    return closest_node, min_distance