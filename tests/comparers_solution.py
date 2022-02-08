import logging

from pyspark.sql import DataFrame


def assert_frames_functionally_equivalent(
    df1: DataFrame, df2: DataFrame, check_nullability=True
):
    """
    Validate if two non-nested dataframes have identical schemas, and data,
    ignoring the ordering of both.
    """
    # This is what we call an “early-out”: here it is computationally cheaper
    # to validate that two things are not equal, rather than finding out that
    # they are equal.
    try:
        if check_nullability:
            assert set(df1.schema.fields) == set(df2.schema.fields)
        else:
            assert set(df1.dtypes) == set(df2.dtypes)
    except AssertionError:
        logging.warning(df1.schema)
        logging.warning(df2.schema)
        raise

    sorted_rows = df2.select(df1.columns).orderBy(*df1.columns).collect()
    assert df1.orderBy(*df1.columns).collect() == sorted_rows
