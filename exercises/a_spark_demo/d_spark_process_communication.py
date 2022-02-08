"""
Illustrating the creation of the sparkcontext (implicitly through the
SparkSession builder since Spark2.x) and which processes run where.

Also, touching on RDDs and the MapReduce concepts, though in practice you
should rarely use RDDs and both map and reduce have different names when you use
DataFrames.
"""
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, upper

spark = SparkSession.builder.getOrCreate()

dataframe = spark.range(1)
dataframe.show()  # performs an action on the DataFrame

t1 = time.time()
dataframe = spark.range(5).withColumn("foo", lit("bar"))  # transformations
print(time.time() - t1)
dataframe.printSchema()
print(
    dataframe.count()
)  # This is also an action: it triggers the executors to do work.
t2 = time.time()

print(t2 - t1)
dataframe.show()

# Different ways of interacting with Columns
for variable in (lit("bar"), dataframe.foo, dataframe["foo"], col("foo")):
    print(type(variable))

df = spark.createDataFrame(
    data=[
        ("hello", "sunny", "world"),
        (None, "sunshine", "rainbows!"),
        ("One", "Two", "Three"),
        ("One", "Two", "Three"),
    ],
    schema=("a", "b", "c"),
)


df.printSchema()
df.show()


print(df.rdd.map(lambda row: row[1].upper()).collect())


def titular_mapping(row):
    print(
        "This is all executed on the workers, this is not the driver context."
    )
    # The next line uses both position-based indexing, as well as indexing using named attributes.
    return row["c"].title(), row[1].lower()


print(df.rdd.map(titular_mapping).collect())
# This is functionally the same as what you have on line 10 in this script, but
# *much* more efficient, as it keeps the transformations in the Java VM.
df.select(upper(df["b"])).show()
