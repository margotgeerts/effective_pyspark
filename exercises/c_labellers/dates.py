import datetime

from pyspark.sql import DataFrame


def is_belgian_holiday(date: datetime.date) -> bool:
    pass


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:
    pass


def label_holidays(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Add a column indicating whether or not the column `colname`
    is a holiday."""
    pass


def label_holidays2(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    pass


def label_holidays3(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Add a column indicating whether or not the column `colname`
    is a holiday."""
    pass
