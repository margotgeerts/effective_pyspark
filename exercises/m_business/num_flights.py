from pprint import pprint

from pyspark.sql.functions import col, dayofweek, avg, sum, year
from pyspark.sql.types import ByteType

from exercises.i_catalog.catalog import load_frame_from_catalog

frame = load_frame_from_catalog("master_flights")
operational_year = 2011
american_airlines_flights_in_2011 = frame.filter(
    col("CARRIER_NAME").like("%American Airlines%")
    & (year("flight_date") == operational_year)
).cache()

print(
    "Number of flights operated by American Airlines in {}: {}".format(
        operational_year, american_airlines_flights_in_2011.count()
    )
)
print(
    "Of those, {} arrived less than (or equal to) 10 minutes later".format(
        american_airlines_flights_in_2011.filter(
            col("arr_delay") <= 10
        ).count()
    )
)


relative_departures_by_dayofweek = (
    frame.withColumn("has_delayed_departure", col("dep_delay") > 0)
    .groupBy(dayofweek("flight_date").alias("weekday"))
    .agg(
        avg(col("has_delayed_departure").cast(ByteType())).alias(
            "relative departures"
        )
    )
).collect()

print(
    "The average number of delayed departures, sorted by weekday (1 = Su, 7=Sa):"
)
pprint(sorted(relative_departures_by_dayofweek))
print("The largest number here is for:")
print(max(relative_departures_by_dayofweek, key=lambda x: x[1]))
print("which confirms the data scientist’s hunch")

delay_categories = [
    f"{cause}_delay_in_minutes"
    for cause in (
        "carrier",
        "weather",
        "nas",
        "security",
        "late_aircraft",
    )
]
frame = american_airlines_flights_in_2011.select(*delay_categories)
expressions = [
    (
        sum((col(category) > 0).cast(ByteType())).alias(
            f"nbr_of_positive_delays_due_to_{category}"
        )
    )
    for category in delay_categories
]
# Note: the ability to _generate_ SQL expressions like this, with a simple for
# loop is incredibly useful, as there are less chances for errors and you can
# keep the amount of lines very small
print("")
print("The following table shows that NAS_Delay should be focussed on.")
frame.agg(*expressions).show()
# This is of course assuming the incidents get labelled without bias. It
# is perfectly possible for ground personel to be fed up with a certain kind of
# problem and therefore flag those more often.
