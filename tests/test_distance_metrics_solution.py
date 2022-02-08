"""
Exercise: implement a series of tests with which you validate the
correctness (or lack thereof) of the function great_circle_distance.
"""
import math
from random import random

import pytest

from exercises.b_unit_test_demo.distance_metrics_corrected import great_circle_distance


def test_great_circle_distance():
    # Write out at least two tests for the great_circle_distance function.
    # Use these to answer the question: is the function correct?

    # Course participants should have at least one assertion here. Those that are
    # fairly new to testing will most likely create a test that checks the distance
    # between two coordinates which they looked up on the Internet, for example
    # checking in Google Maps what the distance is between Paris and Berlin.
    # While that test is okay, it puts more effort on the next programmer reading
    # your test, because he/she needs to look up that distance (few people know that
    # by heart). And, you have a reliance on a third party (in this case, Google Maps)
    # that that party was also correct. Sure, Google Maps is unlikely to be incorrect.
    # But consider that new services sometimes show issues too. Try to reduce reliance
    # on external parties and services in tests: keep things fairly simple, high-school
    # mathematics should still be fine.
    # If you did this, created a test involving distances between real locations on our
    # planet, do have a look at the non-trivial test below.
    pass


def test_zero_length_distances():
    """The distance from any point to itself should be zero."""

    # Set up the test prerequisites: what are the inputs to the "thing" you're
    # testing.
    # Define locations on the sphere using tuples of floating point numbers
    # representing the latitude and longitude (in “spherical coordinates”) of
    # those locations.
    point = (0, 0)

    # Capture the output of the "thing" you're testing. For easier recognition
    # (as all tests follow a similar pattern), you can make this stand out by
    # having it in its own paragraph.
    result = great_circle_distance(point[0], point[1], point[0], point[1])

    # Compare the captured result to the expectation value.
    assert result == 0


def test_the_distance_function_is_commutative():
    """The distance from point A to point B (different from A) is
    the same as the distance from B to A."""
    # Latitudes go from -90° to +90°. Longitudes from -180° to +180° (non incl.)
    # using mathematical notation: latitude ∈ [-90°, +90°], longitude ∈ [-180°, +180°)
    # The function `random` generates a number ∈ [0, 1), which you can transform to
    # such ranges (small exception for the +90° angle here which will be non-inclusive).
    pointA, pointB = (
        (random() * 180 - 90, random() * 360 - 180) for _ in range(2)
    )

    distanceAB = great_circle_distance(
        pointA[0], pointA[1], pointB[0], pointB[1]
    )
    distanceBA = great_circle_distance(
        pointB[0], pointB[1], pointA[0], pointA[1]
    )
    math.abs(distanceBA - distanceAB ) < 1e-8
    assert distanceAB == pytest.approx(distanceBA)


def test_non_trivial_great_circle_distance():
    """The distance between any pole to a location on the equator, should
    equal a quarter of the circle with a radius of the sphere."""
    # If you forgot your high school math: the circumference of a circle is
    # given by 2*pi*radius
    northpole = (90, -22)  # on the poles, longitude has no meaning
    point_on_equator = (0, 89)

    expected = math.pi / 2

    result = great_circle_distance(
        latitude1=northpole[0],
        longitude1=northpole[1],
        latitude2=point_on_equator[0],
        longitude2=point_on_equator[1],
        # If you did not improve the function by making it work for spheres of
        # any radius, you must adjust the expectation value with a factor equal
        # to the radius that is assumed inside the function. This requires you
        # to look into the function's implementation, which is different from
        # black-box-testing. If you improved the function, as shown here, you
        # simplify the test, because it's much easier to reason on the simpler
        # problem (a sphere with unit radius) rather than one with a specific
        # radius (where does that magic number 6371 come from anyway & why
        # should it be hardcoded in the test? → It shouldn't.)
        radius=1,
    )

    # Any time you deal with floating point numbers, expect small accuracy
    # errors. The reason it wasn't a problem for the zero-length distance test,
    # is because the values chosen (0 and 90) are ideal arguments for the sines
    # and cosines that are used in the Haversine formula, making them equal to
    # 0 or 1, which can be represented exactly on computers.
    assert result == pytest.approx(expected)


# This is a basic example of tests written with the Pytest framework in mind.
# It's simple and suffices for the purpose of this lecture.
