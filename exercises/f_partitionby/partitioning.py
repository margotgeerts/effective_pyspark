import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(
    [
        (1, "H", "7"),
        (1, "H", "K"),
        (2, "S", "A"),
        (2, "S", "7"),
        (2, "S", "J"),
        (2, "C", "8"),
        (3, "S", "10"),
        (3, "H", "Q"),
        (3, "D", "8"),
        (3, "D", "10"),
        (4, "H", "2"),
        (4, "C", "4"),
        (4, "C", "J"),
        (4, "D", "4"),
    ],
    schema=("partition", "suit", "value"),
).repartition("partition").cache()

df.show()
tempdir = Path(tempfile.gettempdir()) / "example"

# now write it to the filesystem
for index, frame in enumerate((df, df.repartition("suit"))):
    dir = str(tempdir) + str(index)
    print(f"saving to {dir}")
    frame.write.partitionBy("suit").csv(dir, mode="overwrite", header=True)
