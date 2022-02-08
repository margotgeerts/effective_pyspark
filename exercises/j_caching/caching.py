import time

# if you want to see some real impact from caching for our toy-datasets,
# ensure that Spark needs to do a lot of disk calls. You could force that by
# repartitioning by origin prior to writing and using partitionBy on
# "flight_date" and"origin" in clean_flights.py.
from pyspark.sql import SparkSession

from exercises.h_cleansers.clean_airports import main
from exercises.i_catalog.catalog import load_frame_from_catalog

spark = SparkSession.builder.getOrCreate()

# Get Spark warmed up, to reduce bias in the subsequent experiment.
main()


def effect_of_cache(cache_enabled: bool = False) -> None:
    df = load_frame_from_catalog("raw_flights")
    df2 = df.groupBy("ORIGIN").count()

    if cache_enabled:
        df2.cache()  # one of the rare times when the Spark API is not using immutability.
    t1 = time.time()
    # This answers the question "how many different origins are there"
    df2.count()
    t2 = time.time()
    df2.count()
    t3 = time.time()
    for label, duration in (("first", t2 - t1), ("second", t3 - t2)):
        print(f"duration of {label} action: {duration} seconds")


for use_caching in (False, True):
    effect_of_cache(use_caching)
# Allow for some time to keep the SparkSession alive, so you can observe the Spark UI.
# Of particular interest are the Storage tab and the green dots in the visual
# representation of a job's execution.
time.sleep(600)
