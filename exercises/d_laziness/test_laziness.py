import pytest
from pyspark.sql import SparkSession

from .date_helper import convert_date

spark = SparkSession.builder.getOrCreate()


def test_date_helper_doesnt_work_as_intended():
    df = spark.createDataFrame(
        [
            ("2020-01-19", "2020-01", "2020"),
            ("20200119", "202001", "202"),
            ("20200119", "202001", "unexpected"),
        ],
        schema=("a", "b", "c"),
    )

    for col in df.columns:
        df = df.withColumn(f"{col} converted", convert_date(col))

    with pytest.raises(Exception):
        # it should have generated an exception, because the c column is full
        # of rubbish, but it doesn't, so this test will fail, and thus also
        # show the stdout, which is for many people easier than adding a flag
        # to pytest (GUI configurations).
        df.show()
