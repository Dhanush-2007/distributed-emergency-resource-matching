"""Haversine distance calculation."""

import math


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the great circle distance in kilometers between two points

    on the Earth using the Haversine formula with radius 6371.0088 km.
    """
    earth_radius_km = 6371.0088

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    )
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

    return earth_radius_km * c
