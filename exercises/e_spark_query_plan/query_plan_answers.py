import time
import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_add, lit, to_date, upper, when
from pyspark.sql.types import DateType, IntegerType, StringType, StructType, StructField

spark = SparkSession.builder.getOrCreate()
# Because this script uses 2 small DataFrames, a join operation will be done
# more efficiently by automatically broadcasting one DataFrame, that is have its
# data replicated on each worker node. We want to disable this to understand
# the concept of a stage boundary, as a consequence of a join operation.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

frame2 = spark.createDataFrame(
    [
        (1, None, None),
        (2, "TWO", dt.date(2019, 3, 12)),
        (3, "THREE", None),
    ],
    schema=StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("label", StringType(), nullable=True),
            StructField("mydate", DateType(), nullable=True),
        ]
    ),
    verifySchema=False,
)

frame = (
    spark.range(5)
    .withColumn("foo", col("id") % 2)
    .withColumn("bar", lit("2").cast("tinyint"))  # alternatively: cast(ByteType())
    .withColumn("baz", when(col("id") * 5 + 10 > 22, col("bar") * col("foo")))
    .withColumn("fixed_date", date_add(to_date(lit("20181025"), "yyyyMMdd"), 1))
    .join(frame2, on=["id"], how="left")
    .filter(upper(col("label")) == "ONE")
    .filter(col("id") >= 1)
    .filter(col("mydate") <= "2020-10-18")
)

frame.explain(True)
frame.show()
time.sleep(7 * 60)

# What does the query plan tell you about the order of the filter operations?
# → Spark's Catalyst moves them, because it “sees” it can do the order of
# operations more efficiently.

# What does it tell you about the date filter? Do you see an easy performance
# boost there?
# In Spark versions before v3.0.1: There's a cast to string. That's not very
# efficient because string comparisons are more computationally expensive than
# e.g. integer comparisons.  Behind the scenes, dates are stored as a numeric
# type (e.g. the number of days since some reference). Indeed, change the date
# filter to e.g.  `.filter(col("mydate") <= dt.date(2018, 10, 17) )` and
# observe the filter changing to 17821, which is indeed the number of days
# since Jan 1st, 1970, which is a common reference in computer science.
# In newer Spark versions, this is no longer the case: it's the right-hand
# side, the string, which seems to get converted automatically. Advice is to
# always be precise and use the type system properly.

# How does changing the nullable flag of the column "mydate" impact the queryplan?
# → The notnull check is dropped, meaning slightly less work needs to be done.
# Be careful with this, because you might unintentionally force
# NullPointerExceptions (NPEs)! Uncomment the filter operation on the label,
# for example. Does it still generate this NPE when the filter is moved to the
# end of the transformation pipeline? Explain why/why not?
