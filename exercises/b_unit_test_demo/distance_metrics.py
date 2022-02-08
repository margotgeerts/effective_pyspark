"""
To be used to demonstrate how unit tests can validate the lack of errors
in code.
"""

import math


def great_circle_distance(latitude1, longitude1, latitude2, longitude2):
    """An implementation of the Haversine formula, to calculate the shortest
    distance along the surface of a sphere between two points.
    """
    lat1 = math.radians(latitude1)
    lon1 = math.radians(longitude1)
    lat2 = math.radians(latitude2)
    lon2 = math.radians(longitude2)
    sin_long_half_diff = math.sin((lon2 - lon1) / 2)
    sin_lat_half_diff = math.sin((lat2 - lat1) / 2)
    a = math.pow(sin_lat_half_diff, 2.0) + math.cos(lat1) * math.cos(
        lat2
    ) * math.pow(sin_long_half_diff, 2.0)
    return math.asin(a) * (2 * 6371)
