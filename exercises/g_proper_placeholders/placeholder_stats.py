"""
This script shows that placeholders for unknown data should be represented by
None/null. Any statistics library worth its salt will correctly handle
null-data. Do NOT ever make up data (unless you're “imputing” data for machine
learning). That's how real scientists get stripped of their PhD degrees 😉.
"""
import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql.functions import max, mean

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(
    [
        ("BE", dt.date(2020, 10, 1), 999),
        ("BE", dt.date(9999, 12, 31), 15),
        ("FR", dt.date(2020, 1, 1), 9),
        ("FR", dt.date(2020, 1, 11), 5),
        ("FR", dt.date(2020, 1, 21), 1),
    ],
    schema=("country", "entry date", "car age"),
)

df2 = spark.createDataFrame(
    [
        ("BE", dt.date(2020, 10, 1), None),
        ("BE", None, 15),
        ("FR", dt.date(2020, 1, 1), 3),
        ("FR", dt.date(2020, 1, 21), 1),
    ],
    schema=df.columns,
)

for frame in (df, df2):
    frame.groupby("country").agg(max("entry date"), mean("car age")).show()
    print(
        frame.approxQuantile("car age", [0.5], relativeError=0)
    )  # computes the median
