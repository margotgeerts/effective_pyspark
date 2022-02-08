"""The function `convert_date` was assumed to be correct, but it's not, because
it ignores two aspects of the Spark API. What aspects does it ignore?

This module is intended to show what is wrong, and how it can be resolved.
"""

from pyspark.sql.functions import to_date, last_day
from pyspark.sql.types import DateType


def convert_date(date):
    """
    Function to convert a data to a Spark data format
     Following types of input date formats are allowed:
     - string yyyyMM (last day of month will be returned)
     - string yyyyMMdd
     - date
    """
    try:
        converted_date = last_day(to_date(date, "yyyyMM"))
    except ValueError:
        try:
            converted_date = to_date(date, "yyyyMMdd")
        except ValueError:
            try:
                converted_date = date.cast(DateType())
            except ValueError:
                message = 'date must have format DateType() or StringType() "yyyyMM" or "yyyyMMdd" '
                raise Exception(message)
    return converted_date
