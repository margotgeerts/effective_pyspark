"""
Windowing functions can sometimes replace specific sequences of groupbys and
self-joins.
"""
import time

from pyspark.sql import SparkSession, Window
import datetime as dt
from pyspark.sql.functions import max, col, first, row_number, mean, sum, monotonically_increasing_id

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
df = spark.createDataFrame(
    [
        ("Sarah", dt.date(2020, 1, 1), 30),
        ("Sarah", dt.date(2020, 1, 5), 20),
        ("John", dt.date(2020, 1, 1), 60),
        ("Marcus", dt.date(2020, 1, 5), 5),
        ("Marcus", dt.date(2020, 1, 6), 8),
        ("John", dt.date(2020, 2, 3), 40),
        ("Marcus", dt.date(2020, 1, 7), 7),
    ],
    schema=("person", "check-in date", "minutes online"),
).cache()

last_online = (
    df.groupBy("person").agg(max(df.columns[1]).alias(df.columns[1])).cache()
)
last_online.show()
out = df.join(last_online, on=df.columns[:2], how="inner")
out.show()
out.count()  # see below


login_dates_by_person = Window.partitionBy("person").orderBy(
    col(df.columns[1]).desc()
)
df2 = (
    df.withColumn(
        "last online duration",
        first("minutes online").over(
            login_dates_by_person.rowsBetween(
                Window.unboundedPreceding, Window.unboundedFollowing
            )
        ),
    )
    .withColumn("rownum", row_number().over(login_dates_by_person))
)
df2.show()

df3 = df2.filter(col("rownum") == 1)

df3.show()
df3.count()  # to make it easier to find in the Spark UI.


time.sleep(600)
# now have a look at the Spark UI for the number of stages in each.


# This one is merely a variation on the example in the slides.
df = spark.createDataFrame(
    [
        ("Sarah", dt.date(2020, 1, 1), 10),
        ("Sarah", dt.date(2020, 1, 2), 20),
        ("Sarah", dt.date(2020, 1, 3), 30),
        ("Sarah", dt.date(2020, 1, 4), 1),
        ("Marcus", dt.date(2020, 1, 6), 10),
        ("Marcus", dt.date(2020, 2, 9), 20),
        ("Marcus", dt.date(2020, 2, 10), 15),
    ],
    schema=df.columns,
)

df.withColumn(
    "sum",
    sum(df.columns[-1]).over(
        Window.partitionBy("person")
        .orderBy(df.columns[1])
        .rowsBetween(-2, Window.currentRow)
    ),
).show()
