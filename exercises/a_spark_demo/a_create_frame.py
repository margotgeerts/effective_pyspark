"""
Illustrate several ways to create small, toy-example dataframes.
This is incredibly useful in tests.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.getOrCreate()

# The verbose way
fields = [
    StructField("name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
]
users = spark.createDataFrame(
    data=[
        ("Wim", 1),
        (None, 2),
    ],
    schema=StructType(fields),
)

# A shorter way, with implicit assumptions: Spark will attempt to infer the datatypes.
# They will typically be chosen overly large.
currencies = spark.createDataFrame(
    data=[
        ("Euro", 1.0, 1),
        ("USD", 1.2, 1),
    ],
    schema=("currency", "value", "random"),
)

for frame in (users, currencies):
    frame.show()  # An action.
    frame.printSchema()  # Not an action.
