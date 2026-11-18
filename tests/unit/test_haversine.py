"""Unit tests for Haversine distance calculation."""

import pytest

from ems_common.haversine import haversine_km


@pytest.mark.unit
def test_haversine_zero_distance():
    # Same point should yield exactly 0.0 distance
    dist = haversine_km(12.9716, 77.5946, 12.9716, 77.5946)
    assert dist == 0.0


@pytest.mark.unit
def test_haversine_known_city_pair():
    # Known city pair: London (51.5074 N, 0.1278 W) to Paris (48.8566 N, 2.3522 E)
    # Great circle distance is approximately ~343.5 km
    lat1, lon1 = 51.5074, -0.1278
    lat2, lon2 = 48.8566, 2.3522

    expected_km = 343.5
    actual_km = haversine_km(lat1, lon1, lat2, lon2)

    # Within 1% tolerance
    relative_error = abs(actual_km - expected_km) / expected_km
    assert relative_error < 0.01, f"Expected ~{expected_km} km, got {actual_km} km"
