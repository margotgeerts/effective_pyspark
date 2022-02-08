"""Illustrate how UDFs lose the generic character that their pure Python
counterparts exhibit.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, IntegerType


def square(x):
    print(f"This is element: {x}")
    return x ** 2


spark = SparkSession.builder.getOrCreate()


def pure_python():
    for description, value in (
        ("an integer", 2),
        ("a floating point value, without fractional value", 2.0),
        ("a floating point value with a significant fractional part", 2.5),
    ):
        print(
            f"{value} squared is {square(value)}. The input was a {description}."
        )


def using_udfs(df, return_datatype=IntegerType()):
    square_udf_int = udf(square, return_datatype)

    df.select(
        "a",
        square_udf_int("a").alias("a_squared"),
        "b",
        square_udf_int("b").alias("b_squared"),
    ).show()


def using_standard_pyspark_functionality(df):
    df.select(
        df["a"],
        df["a"] ** 2,
        df["a"] * df["a"],
        df["b"],
        df["b"] ** 2,
        df["b"] * df["b"],
    ).show()


if __name__ == "__main__":
    # In pure Python, the input data types define the return value.
    # The function is generic, it works for integers and floating point values.
    pure_python()

    df = spark.createDataFrame(
        [
            (1, 2.0),
            (3, 4.9),
            (5, 6.9),
        ],
        schema=("a", "b"),
    )
    # With UDFs, this generic behaviour is broken: as you must enforce a
    # return type on an otherwise normal Python function, some values come out
    # in a surprising way.
    using_udfs(df, IntegerType())
    # Big surprise here: while one could say that floats are more general than integers,
    # the function should work for both. This demo shows that it only works properly
    # on floating point inputs.
    using_udfs(df, FloatType())

    using_standard_pyspark_functionality(df)
