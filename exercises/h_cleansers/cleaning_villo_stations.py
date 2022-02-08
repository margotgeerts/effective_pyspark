"""Another cleaning exercise, this time focusing on functionality that you
haven't seen yet, but should, by now be able to look up by yourself and apply
functions you have not encountered before."""
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from exercises.i_catalog.catalog import load_frame_from_catalog


def clean(df: DataFrame) -> DataFrame:
    # The instructor will have mentioned that such files are used in
    # cartographic applications. You should not focus on the GeoJSON (the
    # geometry column), which is used to show things nicely on a map. That you
    # can leave as a String.  Of interest here is the last column, which
    # contains latitudes and longitudes together. There are applications where
    # you want to filter for data that is in a certai region, in which case you
    # could use these as rough lines to cut (in Belgium, Flanders and Wallony
    # are mostly separated by a line at latitude 50.754420).

    return df


if __name__ == "__main__":
    path_to_exercises = Path(__file__).parents[1]
    resources_dir = path_to_exercises / "resources"

    spark = SparkSession.builder.getOrCreate()

    # note: you could use a catalog, which is better than putting links everywhere.
    df = load_frame_from_catalog("villos")
    result = clean(df)
    result.show()
    result.printSchema()
