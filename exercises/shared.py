from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

resources_dir = Path(__file__).parent / "resources"
csv_file_path = resources_dir / "pms_under_elizabeth_2nd.csv"

ministers = spark.read.csv(
    str(csv_file_path),
    sep=";",
    header=True,
)
