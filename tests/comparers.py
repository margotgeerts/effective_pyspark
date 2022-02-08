from pyspark.sql import DataFrame


def assert_frames_functionally_equivalent(
    df1: DataFrame, df2: DataFrame, check_nullability=True
):
    """
    Validate if two non-nested dataframes have identical schemas, and data,
    ignoring the ordering of both.
    """
    assert False
