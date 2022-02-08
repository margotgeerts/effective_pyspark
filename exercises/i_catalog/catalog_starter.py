"""
A simple implementation of a data catalog, used to illustrate how it abstracts
details like location, format, and format-specific options.
"""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

RESOURCES_DIR = Path(__file__).parents[1] / "resources"
TARGET_DIR = Path(__file__).parents[1] / "target"
TARGET_DIR.mkdir(exist_ok=True)


catalog = {
    "raw_flights": (),
}


def load_frame_from_catalog(dataset_name, catalog=catalog) -> DataFrame:
    """Given the name of a dataset, load that dataset (with the options and
    whereabouts specified in the catalog) as a Spark DataFrame.
    """
    pass


if __name__ == "__main__":
    # to illustrate how this can be used:
    frame = load_frame_from_catalog("clean_flights")
    frame.show()
