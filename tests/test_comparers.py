import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, IntegerType, StructType

from .comparers import assert_frames_functionally_equivalent

spark = SparkSession.builder.master("local[*]").getOrCreate()


def test_non_equal_frames_not_functionally_equivalent():
    frame1 = spark.createDataFrame([(1,), (2,)])
    frame2 = spark.createDataFrame([(1,), (2,), (2,)])

    with pytest.raises(AssertionError):
        assert_frames_functionally_equivalent(frame1, frame2)


def test_identical_frames_are_identical():
    df = spark.range(1)
    assert_frames_functionally_equivalent(df, df)


def test_column_order_is_irrelevant_for_functional_equivalence():
    fields = [
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ]
    frame1 = spark.createDataFrame(
        [("Wim", 1), ("Conrad", 2)], schema=StructType(fields)
    )

    frame2 = spark.createDataFrame(
        [(1, "Wim"), (2, "Conrad")], schema=StructType(fields[::-1])
    )
    assert_frames_functionally_equivalent(frame1, frame2)


def test_ordering_of_data_is_irrelevant_for_functional_equivalence():
    fields = [
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ]
    frame1 = spark.createDataFrame(
        [("Wim", 1), ("Conrad", 2)], schema=StructType(fields)
    )

    frame2 = spark.createDataFrame(
        [("Conrad", 2), ("Wim", 1)], schema=StructType(fields)
    )
    assert_frames_functionally_equivalent(frame1, frame2)


def test_functional_equivalence_testing_works_with_nones():
    # in Python 3, None is not sortable wrt to other types,
    # i.e. None < 3 errors out
    fields = [
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ]

    df = spark.createDataFrame(
        [(None, 1), ("Christina", 2)], schema=StructType(fields)
    )
    assert_frames_functionally_equivalent(df, df)


def test_functional_equivalence_still_means_same_values():
    df1 = spark.createDataFrame([("a", "b")])
    df2 = spark.createDataFrame([("a", "c")])

    with pytest.raises(AssertionError):
        assert_frames_functionally_equivalent(df1, df2)
