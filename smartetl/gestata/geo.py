"""geo相关工具"""


def as_lat(lat_lon: tuple):
    return lat_lon[0]


def as_lon(lat_lon: tuple):
    return lat_lon[1]


def as_lat_lon(row: dict, key_lat_lon: str = 'lat_lon', target_lat: str = 'lat', target_lon: str = 'lon'):
    if key_lat_lon in row:
        lat_lon = row[key_lat_lon]
        if lat_lon:
            row[target_lat] = lat_lon[0]
            row[target_lon] = lat_lon[1]
    
    return row
