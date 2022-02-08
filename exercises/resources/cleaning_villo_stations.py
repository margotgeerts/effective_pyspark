from pathlib import Path

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import when, to_timestamp, upper, trim, split
from pyspark.sql.types import ShortType, FloatType


def convert_true_false_strings(
    column, true_like=("TRUE", "YES"), false_like=("FALSE", "NO")
):
    # Note: spark can automatically cast some strings to Booleantypes as well,
    # like 1 and 0 (even as integers). It doesn't work in a general sense
    # though as Spark doesn't have a notion abut everything that you could
    # consider to be True/False. The French word "oui" (meaning "yes") can be
    # considered equivalent to True, but you can't expect Spark to automatically
    # cast that as a Boolean value. In such cases, a function like this could
    # help you out.

    # Standardize strings, both in the input and in the comparison values!
    c = upper(trim(column))
    true_like, false_like = (
        set(s.strip().upper() for s in collection)
        for collection in (true_like, false_like)
    )

    # note that the next line allows you to also return None/null
    # automatically, in case nothing is matching.
    return when(c.isin(*true_like), True).otherwise(when(c.isin(*false_like), False))


def split_gps_coords(
    df: DataFrame,
    column: str,
    separator: str = ",",
    latitude_pos: int = 0,
    longitude_pos: int = 1,
) -> DataFrame:
    """Split a string column containing GPS coordinates, by the separator into
    its latitude and longitude components.

    The original column will be dropped.
    """
    c1 = split(df[column], separator)
    for coord_kind, index in (("latitude", latitude_pos), ("longitude", longitude_pos)):
        df = df.withColumn(coord_kind, c1.getItem(index).cast(FloatType()))
    return df.drop(column)


def clean(df: DataFrame) -> DataFrame:
    cols_that_could_be_shorts = (
        "number",
        "Bike Stands",
        "Available Bike Stands",
        "Available Bikes",
    )
    for colname in cols_that_could_be_shorts:
        df = df.withColumn(colname, df[colname].cast(ShortType()))

    cols_that_could_be_bools = ("banking", "bonus", "status")
    better_names = ("has_banking", "has_bonus", "is_closed")
    true_like_values = ("True", "closed")
    false_like_values = ("False", "open")
    for new_name, old_name in zip(better_names, cols_that_could_be_bools):
        df = df.withColumn(
            old_name,
            convert_true_false_strings(
                old_name, true_like=true_like_values, false_like=false_like_values
            ),
        ).withColumnRenamed(old_name, new_name)

    ts_col = "Last Update"
    df = df.withColumn(ts_col, to_timestamp(ts_col, format="yyyy-MM-dd'T'HH:mm:ssXXX"))

    df = split_gps_coords(df, "position")

    return df


def download_small_file(url: str, destination: Path) -> None:
    response = requests.get(url)
    destination.write_bytes(response.content)


if __name__ == "__main__":

    inputfile = Path(__file__).with_name("villo")
    download_small_file(
        url="https://opendata.brussel.be/explore/dataset/villo-stations-availability-in-real-time/download/?format=csv&timezone=Europe/Berlin&lang=nl&use_labels_for_header=true&csv_separator=%3B",
        destination=inputfile,
    )

    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv(str(inputfile), header=True, sep=";")
    result = clean(df)
    result.show()
    result.printSchema()
