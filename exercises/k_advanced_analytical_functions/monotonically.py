"""In this small example, the function monotonically_increasing_id is used to
generate a unique key per row in the dataframe.  It _could_ be used for
splitting a dataframe and afterwards rejoining the transformed pieces, though
you'll want to tread cautiously, since the function assigns values based on
partition numbers, which you don't control.
"""
from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.functions import (
    monotonically_increasing_id,
    lower,
    spark_partition_id,
)


def load_df(num_partitions: int = 4) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    return (
        spark.createDataFrame(
            [(n, "left", "right") for n in "ABCDEF"], schema=("id", "l", "r")
        )
        .repartition(num_partitions)
        .withColumn("partition_id", spark_partition_id())
    )


df = load_df(6)
df.show()
df2 = df.withColumn("inc_id", monotonically_increasing_id())
df2.show()
print(df2.rdd.getNumPartitions())
left = df2.select("inc_id", "id", "l").cache()
right = df2.select("inc_id", "id", "r", "partition_id")
print(
    "left and right frames have the same monotonically increasing id, "
    "because they're on the same partition:"
)
left.show()
right.show()

augmented = (
    right.filter(right["id"] > "B")
    .withColumn("lower", lower(right["id"]))
    .repartition(df2.rdd.getNumPartitions() * 2)
    .withColumn("2nd_partition_id", spark_partition_id())
)
augmented.show()
result = left.join(augmented, on=["inc_id"], how="left")
# Observe that monotonically_increasing_id blocks Catalyst from further
# optimizations: the filter on "id" is not "pushed down".
result.explain()
result.show()
