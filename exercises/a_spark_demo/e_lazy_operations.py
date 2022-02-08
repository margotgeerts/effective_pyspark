"""
When you ask Spark to run operations on data, it will defer these operations
until really called for. This is known as "lazy evaluation".
"""
import time

from pyspark.sql import SparkSession

with_action = False

t1 = time.time()
spark = SparkSession.builder.getOrCreate()
t2 = time.time()
print(f"{t2 - t1} seconds elapsed for the creation of the SparkSession")


t1 = time.time()
df = spark.range(99999)
t2 = time.time()
print(f"{t2 - t1} seconds elapsed for the creation of a DataFrame")

df2 = df.withColumn("is_even", df["id"] % 2 == 0)
t3 = time.time()
print(f"{t3 - t2} seconds elapsed for adding a column")
# Note that even though we asked Spark to add a column with some logic to it,
# no work was actually done on the executors: this method call
# (`DataFrame.withColumn`) returned quasi instantly.
df2.printSchema()
# And yet, something was done: Spark is clearly aware that at least
# one column was added.

if with_action:
    t4 = time.time()
    df2.collect()
    t5 = time.time()
    print(f"{t5 - t4} seconds elapsed for collecting the DataFrame.")
