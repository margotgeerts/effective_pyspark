"""Introduces a few more very common DataFrame operations, especially common
in business logic.
"""

import pyspark.sql.functions as psf
from pyspark.sql import Column, DataFrame

from exercises.shared import ministers, spark

########################################
# GROUPBY
########################################
# Both "groupBy" and "groupby" exist. The latter is a reference to the
# former, to accommodate user complaints that groupBy isn't PEP8 compliant.
(
    ministers.groupby("party")
    .agg(psf.sum("consecutive_terms").alias("total_number_of_terms"))
    .show()
)

ministers.groupby("party").count().show()

########################################
# JOIN (a non-equi join)
########################################
uk_monarchs = spark.createDataFrame(
    data=[
        ("Queen Elizabeth II", 1952, None),
        ("King George VI", 1936, 1952),
        ("King Edward VIII", 1936, 1936),
    ],
    schema=("monarch", "reign start", "reign stop"),
)

uk_monarchs.show()
uk_monarchs.printSchema()


def monarch_reign_overlaps_with_minister_office(
    monarchs: DataFrame, ministers: DataFrame
) -> Column:
    """Returns a column of boolean values stating whether the reign of
    a monarch overlaps with the office of a prime minister."""
    # There is a degree of error here, as we're only using the years to
    # compare, not the actual dates. Keep in mind, this is just an exercise.
    pm_start, pm_stop = (
        psf.year(psf.to_date(ministers[col]))
        for col in ("entered_office_on", "left_office_on")
    )

    return ranges_overlap(
        start1=monarchs["reign start"],
        stop1=monarchs["reign stop"],
        start2=pm_start,
        stop2=pm_stop,
    )


def ranges_overlap(
    start1: Column, stop1: Column, start2: Column, stop2: Column
) -> Column:
    """Validates whether the two ranges, given by [start1, stop1] and [start2, stop2] overlap.
    Either range can be open-ended."""
    # With open-ended ranges, the result of the comparison will be null.
    # One must then use Boolean logic to know that null | True == True,
    # and null | False == null, just like null | null. The last two cases
    # are dealt with by coalesce: pick the first non-null value from a
    # sequence of columns. It's much more concise than writing
    # `when(col1.isNotNull(), col1).otherwise(col2)`. Moreover, coalesce
    # easily extends to more than 2 columns, unlike when and otherwise.
    return psf.coalesce(
        ~((start1 > stop2) | (stop1 < start2)),
        psf.lit(True),
    )


ministers.join(
    other=uk_monarchs,
    on=monarch_reign_overlaps_with_minister_office(uk_monarchs, ministers),
    how="left",
).orderBy("entered_office_on", "reign start").show()

# # For those finding the logic behind `ranges_overlap` complex, uncomment the following lines
# # to see all of the cases that are handled. Note the symmetry too: this DataFrame could've been
# # reduced to half the size, though that's more of a "hey look how beautiful mathematics is".
# (
#     spark.createDataFrame(
#         [
#             (0, 5, -5, -1),
#             (0, 5, -5, 2),
#             (0, 5, -5, 7),
#             (0, 5, 1, 7),
#             (0, 5, 6, 7),
#             (0, 5, 2, 3),
#             (0, 5, -5, None),
#             (0, 5, 1, None),
#             (0, 5, 6, None),
#             (0, None, -5, -1),
#             (0, None, -5, 1),
#             (0, None, 5, 10),
#         ],
#         schema=list("abcd"),
#     )
#     .withColumn(
#         "are overlapping ranges",
#         ranges_overlap(*[psf.col(x) for x in list("abcd")]),
#     )
#     .show()
# )
#
# # For those wanting to understand boolean binary logic when faced with null values
# (
#     spark.createDataFrame(
#         [
#             (None, True),
#             (None, False),
#             (None, None),
#             (True, True),
#             (True, False),
#             (True, None),
#             (False, True),
#             (False, False),
#             (False, None),
#         ],
#         schema=list("ab"),
#     )
#     .withColumn("a or b", psf.col("a") | psf.col("b"))
#     .withColumn("a and b", psf.col("a") & psf.col("b"))
#     .withColumn("not a", ~psf.col("a"))
#     .show()
# )
