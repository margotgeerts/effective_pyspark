"""
This test module is not meant to be an exhaustive test suite for
clean_flights.py. Rather, it is there to give you an understanding how the two
functions that deal with the decoupled time indication deal with the task at
hand.
"""
import datetime as dt

from pyspark.sql import SparkSession

from .clean_flights import (
    combine_local_date_with_local_hour_minute_indication,
    combine_date_with_overflowed_minutes,
)


def test_combine_date_with_minutes_overflows_for_values_over2400():
    """The function combine_date_with_minutes works fine for well-behaved
    values, but fails for time indications over 2400 (incl.).
    """
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(
        [
            ("2020-12-31", "340", dt.datetime(2020, 12, 31, 3, 40, 0)),
            ("2020-12-31", "1340", dt.datetime(2020, 12, 31, 13, 40, 0)),
            ("2020-12-31", "2400", None),
            ("2020-12-31", "2410", None),
        ],
        schema=("date", "time_indication", "expected"),
    )
    out = df.withColumn("result", combine_local_date_with_local_hour_minute_indication("date", "time_indication"))
    assert out.filter(out["result"].eqNullSafe(out["expected"])).count() == 4


def test_combine_date_with_overflowed_minutes():
    """The function combine_date_with_overflowed_minutes can handle time values
    over 2400 (though it won't properly handle DST switches, so beware).
    """
    # Fortunately, DST behavior is not the goal of the exercise
    # Also, note that it won't work for values over 9999 either (can you see
    # why and do you see a way to remedy this?)
    spark = SparkSession.builder.getOrCreate()
    end_of_year = dt.date(2020, 12, 31)
    df = spark.createDataFrame(
        [
            (end_of_year, "340", dt.datetime(2020, 12, 31, 3, 40, 0)),
            (end_of_year, "1340", dt.datetime(2020, 12, 31, 13, 40, 0)),
            (end_of_year, "2400", dt.datetime(2021, 1, 1, 0, 0, 0)),
            (end_of_year, "3610", dt.datetime(2021, 1, 1, 12, 10, 0)),
        ],
        schema=("date", "time_indication", "expected"),
    )
    out = df.withColumn(
        "result", combine_date_with_overflowed_minutes("date", "time_indication")
    )
    assert out.filter(out["result"] == out["expected"]).count() == 4
