"""
A simple implementation of a data catalog, used to illustrate how it abstracts
details like location, format, and format-specific options.
"""

from pathlib import Path
from typing import Dict, Mapping, NamedTuple

from pyspark.sql import DataFrame, SparkSession

RESOURCES_DIR = Path(__file__).parents[1] / "resources"
TARGET_DIR = Path(__file__).parents[1] / "target"
TARGET_DIR.mkdir(exist_ok=True)


class DataLink(NamedTuple):
    format: str
    path: Path  # for our use cases, a path to a local file system. You will want a string when using AWS S3 or Azure Datalake Storage.
    options: Mapping[str, str]


csv_options = {"header": "true",
               "inferschema": "false"}
catalog = {
    "raw_flights": DataLink("csv", RESOURCES_DIR / "flights", csv_options),
    # source: Aviation Support Tables : Master Coordinate
    # https://www.transtats.bts.gov/DL_SelectFields.asp?gnoyr_VQ=FLL
    "raw_airports": DataLink(
        "csv", RESOURCES_DIR / "airports.csv", csv_options
    ),
    # source: Aviation Support Tables : Carrier Decode
    # https://www.transtats.bts.gov/DL_SelectFields.asp?gnoyr_VQ=GDH
    # Modified to avoid overlapping time ranges.
    "raw_carriers": DataLink(
        "csv", RESOURCES_DIR / "carriers.csv", csv_options
    ),
    "clean_flights": DataLink("parquet", TARGET_DIR / "cleaned_flights", {}),
    "clean_airports": DataLink("parquet", TARGET_DIR / "clean_airports", {}),
    "clean_carriers": DataLink("parquet", TARGET_DIR / "clean_carriers", {}),
    "master_flights": DataLink("parquet", TARGET_DIR / "master_flights", {}),
}


def load_frame_from_catalog(
    dataset_name: str, catalog: Mapping[str, DataLink] = catalog
) -> DataFrame:
    """Given the name of a dataset, load that dataset (with the options and
    whereabouts specified in the catalog) as a Spark DataFrame.
    """
    # note: the default argument in the function parameters is mutable, which is
    # not recommended practice. In a production setting, you would have the
    # catalog become a class (instead of a dictionary) and this function
    # become one of its methods.
    spark = SparkSession.builder.getOrCreate()
    link = catalog[dataset_name]
    return (
        spark.read.options(**link.options)
        .format(link.format)
        .load(str(link.path))
    )


if __name__ == "__main__":
    # to illustrate how this can be used:
    frame = load_frame_from_catalog("clean_flights")
    frame.show()
