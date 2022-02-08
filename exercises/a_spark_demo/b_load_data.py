"""
Illustrating how data can be read from files.
CSVs are ubiquitous and have many flavors, so the number of options for the CSV
reader is larger than that for other sources, like JSON and Parquet.
Also introducing the filter and orderBy methods.
When datasets get small enough that they might fit in the memory of the driver,
you can call collect or toPandas on them. From that moment on, you're no longer
working on distributed data.
"""

from pathlib import Path

import pyspark.sql.functions as psf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

csv_file_path = Path(__file__).parents[1] / "resources" / "pms_under_elizabeth_2nd.csv"

# Python-esque:
frame = spark.read.csv(
    str(csv_file_path),
    sep=";",
    header=True,
)
# Scala-like (uses the "builder" pattern, which is a programming design pattern):
frame = (
    # Wrapping code in parentheses like this, allows you to split long lines.
    # An alternative is using the backslash, '\', but that will not allow you
    # to add comments after it, as in the line below that specifies the keyword
    # argument 'header="True"'
    spark.read.options(
        header="true",  # you could choose here to write it as "true" or simply True
        sep=";",
        inferSchema=True,
    ).csv(str(csv_file_path))
)
print(frame.schema)
frame.show()
frame.printSchema()

frame.select("consecutive_terms").withColumn("foo", psf.col("consecutive_terms") + 10).show()

new_frame = frame.filter(frame["title"].isNotNull())

print(new_frame.count())

frame2 = frame.orderBy(psf.col("left_office_on").asc()).cache()
frame2.show()
result = frame2.collect()
print(result)  # A list of Row objects
# Python uses zero-based indexing: the zeroth element is the _first_ in any sequence
print(type(result[0]))
# Access row attributes dynamically
print(result[0]["name"])  # alternatively: result[0].name
# Or using their position
print(result[-1][0])  # the minus in -1 means: start from the back. The element at -1 is the last element.


# From now on, the piece of code that is used to access the dataset of prime
# ministers is "factored out" into the module exercises.shared
