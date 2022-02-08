import time
from pathlib import Path

import pyspark.sql.functions as psf
import requests
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

datafile = "fhvhv_tripdata_2020-05.csv"

resources_dir = Path(__file__).parents[1] / "resources"
csv_path = Path(resources_dir / datafile)
parquet_path = csv_path.with_name(datafile[:-4])

spark = SparkSession.builder.getOrCreate()


def download_file(src: str, dest: Path) -> None:
    response = requests.get(src)
    dest.write_bytes(response.content)


def create_csv_and_parquet_versions():
    try:
        df = spark.read.csv(str(csv_path), header=True).cache()
    except AnalysisException:
        url = f"https://nyc-tlc.s3.amazonaws.com/trip+data/{datafile}"
        # Don't repeat this too often: it's a 367MB download
        download_file(src=url, dest=csv_path)
        df = spark.read.csv(str(csv_path), header=True)
    df.write.parquet(str(parquet_path), mode="overwrite")

    return df


def show_query_plans_with_pushdown():
    for path, format in ((parquet_path, "parquet"), (csv_path, "csv")):
        print(format)
        df = spark.read.format(format).option("header", True).load(str(path))
        df.select(
            "hvfhs_license_num",
            "dispatching_base_num",
            "pickup_datetime",
        ).filter(psf.col("hvfhs_license_num") == "HV0003").explain()

    return df


if __name__ == "__main__":
    df0 = create_csv_and_parquet_versions()
    df0.printSchema()
    df = show_query_plans_with_pushdown()

    time.sleep(300)
