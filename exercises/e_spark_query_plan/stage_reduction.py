"""
This is an advanced concept. Reducing stages in a Spark job's query plan
typically improves performance. Finding these places to do so requires ingenuity
and especially developer time. Don't try to remove every shuffle, many
operations require at least one shuffle.
"""
import time

from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.functions import col, row_number, max, when

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")


def split_and_join(df: DataFrame) -> DataFrame:
    letters_sorted_by_time = Window.partitionBy("id").orderBy(
        col("time").desc()
    )
    df2 = df.filter(df["flag"]).withColumn(
        "last", row_number().over(letters_sorted_by_time)
    )
    df3 = df2.filter(df2["last"] == 1).select(
        col("id"), col("value").alias("maxvalue")
    )

    return df.join(df3, on=["id"], how="left")


def direct_window(df: DataFrame) -> DataFrame:
    letters_sorted_by_time = Window.partitionBy("id").orderBy(
        col("time").desc()
    )
    return df.withColumn(
        "maxvalue", max("value").over(letters_sorted_by_time)
    ).withColumn("maxvalue", when(col("flag"), col("maxvalue")))


if __name__ == "__main__":
    df = spark.createDataFrame(
        [
            ("A", 1, 10, True),
            ("A", 2, 20, True),
            ("B", 1, 15, True),
            ("C", 1, 16, False),
        ],
        schema=("id", "time", "value", "flag"),
    )
    for func in (split_and_join, direct_window):
        z = func(df).orderBy("id", "time").cache()
        z.show()
        z.count()
        z.explain()
    time.sleep(600)  # Check out the Spark UI to understand the stages.
