"""Another cleaning exercise, this time focusing on functionality that you
haven't seen yet, but should, by now be able to look up by yourself and apply
functions you have not encountered before."""

from pyspark.sql import Column, DataFrame, SparkSession
import pyspark.sql.functions as psf
from pyspark.sql.types import ShortType, StringType, FloatType

from exercises.h_cleansers.clean_flights import to_bool
from exercises.i_catalog.catalog import load_frame_from_catalog

MILLISECONDS_PER_SECOND = 1000


def extract_gps_attribute(
    col: Column = psf.col("position"),
    delimiter: str = ",",
    position: int = 0,
    alias: str = "latitude",
) -> Column:
    return (
        psf.split(col, delimiter, 2).getItem(position).cast(FloatType()).alias(alias)
    )


def clean(df: DataFrame) -> DataFrame:
    # The instructor will have mentioned that such files are used in
    # cartographic applications. You should not focus on the GeoJSON (the
    # geometry column), which is used to show things nicely on a map. That you
    # can leave as a String.  Of interest here is the last column, which
    # contains latitudes and longitudes together. There are applications where
    # you want to filter for data that is in a certain region, in which case you
    # could use these as rough lines to cut (in Belgium, Flanders and Wallonia
    # are mostly separated by a line at latitude 50.754420).

    # Column names are uniform and quite clear.
    # Data types can be improved. Now is the moment to reuse functionality from
    # earlier exercises.
    # 1. split latitude and longitude
    # 2. convert last_update to a timestamp
    # 3. map "banking" and "bonus" to booleans
    df = df.withColumn(
        "position",
        # We'll split the latitude and longitude and place them in a nested column.
        # Most people prefer a flat list of columns though. That's too easy at this
        # point in the course though.
        psf.struct(
            extract_gps_attribute(alias="latitude", position=0),
            extract_gps_attribute(alias="longitude", position=1),
        ),
    ).withColumn(
        "last_update",
        psf.from_unixtime(psf.col("last_update") / MILLISECONDS_PER_SECOND),
    )

    for colname, better_colname in {
        ("banking", "is_banking"),
        ("bonus", "has_bonus"),
    }:
        # Reusing functionality that you had already written, is a boost to
        # your productivity. It also makes code maintenance easier.
        df = df.withColumn(
            better_colname, to_bool(colname, {"oui"}, {"non"})
        ).drop(colname)

    desired_types = {
        ShortType: {
            "number",
            "bike_stands",
            "available_bike_stands",
            "available_bikes",
        },
        StringType: {"contract_name", "name", "address", "status"},
    }

    for datatype, colnames in desired_types.items():
        for colname in colnames:
            df = df.withColumn(colname, psf.col(colname).cast(datatype()))
    return df


if __name__ == "__main__":
    # note: you could use a catalog, which is better than putting links everywhere.
    # make sure to add this to the data catalog
    # {"raw_villos": DataLink(
    #     "csv",
    #     RESOURCES_DIR / "velos",
    #     dict(encoding="iso-8859-1", sep=";", **csv_options),
    #     ),
    # }
    df = load_frame_from_catalog("raw_villos")
    spark = SparkSession.builder.getOrCreate()
    df  = spark.read.format("csv").option("header", True).load(pathname)
    df = spark.read.csv(pathname, sep=";")
    df.show()
    # make sure to add iso-8859-1 as the encoding option to your csv reader options
    result = clean(df)
    result.show()
    result.printSchema()
