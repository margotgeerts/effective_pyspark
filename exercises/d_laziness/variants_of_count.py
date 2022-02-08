"""
This module is there for people who wish to understand how `count` sometimes
leads to a DataFrame (and is thus a transformation), and sometimes to a number
(and is then an action). It is asked in the quiz.
"""

import pyspark.sql.functions as psf

from exercises.shared import ministers

grouped_data = ministers.groupBy("party")
dataframe = grouped_data.count()
row_count = (
    dataframe.count()
)  # Notice that this is basically grouped_data.count().count()
for var in (grouped_data, dataframe, row_count):
    print(type(var))


print(row_count)
dataframe.show()
# If you're interested simply in the number of different parties, you should use countDistinct.
ministers.agg(psf.countDistinct("party")).show()
# Yes, aggregations can be run directly on the entire DataFrame. These functions are not exclusive to GroupedData.

ministers.agg(psf.max("entered_office_on")).show()
print(ministers.agg(psf.max("entered_office_on")).first()[0])

# These two are equivalent. Which do you prefer writing?
print(ministers.agg(psf.count(psf.lit(1))).first()[0])
print(ministers.count())
