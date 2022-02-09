# Exercise:
# “clean” a CSV file using PySpark.
# * Grab sample data from https://packages.revolutionanalytics.com/datasets/AirlineSubsetCsv.tar.gz
#   A copy of the data can be found on our S3 bucket (link shared in class).
# * Inspect the columns: what type of data do they hold?
# * Create an ETL job with PySpark where you read in the csv file, and perform
#   the cleansing steps mentioned in the classroom:
#   - improve column names (subjective)
#   - fix data types
#   - flag missing or unknown data
#   - remove redundant data
# * Write the data to a parquet file. How big is the parquet file compared
#   to the compressed csv file? And compared to the uncompressed csv file?
#   How long does your processing take?
# * While your job is running, open up the Spark UI and get a feel for what's
#   there (together with the instructor).
# For explanations on the columns, check https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGK
"""
The main point of this exercise is to realize that a good choice of data types
can go a long way to make a dataset more self descriptive, less ambiguous, and
that work you do in the clean layer of a data lake is work someone else will
not have to perform again (often faulty).

You must make an assumption about which fields add value for the majority of
your downstream users and which are only noise (example: in the dataset given,
I would not trust DEP_DELAY, and simply recompute it as the difference between
DEP_TIME and CRS_DEP_TIME).
Additionally, in a clean layer, try to avoid duplicity: having a proper date
column for events, means you shouldn't bother adding a column for year, month,
day of month and all kinds of derived features. When someone really needs that
in their business project (the data lake zone after the clean zone), they'll
generate those. Don't assume you need to give downstream users everything but
the kitchen sink.
"""
from pathlib import Path
from typing import Collection, Mapping, Union

from pyspark.sql import Column, DataFrame, SparkSession
import pyspark.sql.functions as psf
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DateType,
    IntegerType,
    ShortType,
    StringType,
)

column_or_str = Union[Column, str]


def combine_local_date_with_local_hour_minute_indication(
    datecolumn: column_or_str, hour_minute_column: column_or_str
) -> Column:
    """Combines values of "2020-10-16", with 1405 into "2020-10-16 14:05:00."""

    hms_string = psf.rpad(psf.lpad(hour_minute_column, 4, "0"), 6, "0")
    return psf.to_timestamp(
        psf.concat(psf.date_format(datecolumn, "yyyy-MM-dd"), hms_string),
        format="yyyy-MM-ddHHmmss",
    )


def combine_date_with_overflowed_minutes(
    datecolumn: column_or_str, hour_minute_column: column_or_str
) -> Column:
    # An alternative to the earlier function, but this one will handle
    # hour_minute values of 2400, however it will have issues with daylight
    # savings time.

    # Also note that this function assumes the datecolumn has an associated
    # data type of date. If you pass it a string, it won't work, because
    # unix_timestamp has a default choice for its format specification.

    # The key aspect to note is that "when" an event occurred is a specific
    # timestamp, given by a combination of a date, time and timezone. The
    # timezone is often implicit (which isn't good form). You should always be
    # explicit and provide all these fields. Do not disconnect time from the date,
    # because saying "my train left too early at 11:03" will not help the people
    # at the railway services, without also providing the date (and train number)
    daystart_as_seconds_since_epoch = psf.unix_timestamp(datecolumn)
    hour_minutes = psf.lpad(hour_minute_column, 4, "0")
    hours = psf.substring(hour_minutes, 1, 2)
    minutes = psf.substring(hour_minutes, 3, 2)
    total_seconds = hours * 3600 + minutes * 60
    return psf.from_unixtime(daystart_as_seconds_since_epoch + total_seconds)


def read_data(path: Path):
    spark = SparkSession.builder.getOrCreate()
    return spark.read.csv(
        str(path),
        # For a CSV, `inferSchema=False` means every column stays of the string
        # type. There is no time wasted on inferring the schema, which is
        # arguably not something you would depend on in production either.
        inferSchema=False,
        header=True,
        # The dataset mixes two values for null: sometimes there's an empty attribute,
        # which you will see in the CSV file as two neighboring commas. But there are
        # also literal "null" strings, like in this sample: `420.0,null,,1.0,`
        # The following option makes a literal null-string equivalent to the empty value.
        nullValue="null",
    )


def naive_clean(frame: DataFrame) -> DataFrame:
    # This cleaning function is a GOOD implementation: it does what is expected.
    # Most beginners will start by writing something like this. However, it is a
    # naive approach: it's not directly clear what happens where (renames occur at
    # the same time of casting the data types) and there's more typing than in the
    # alternative `a_shorter_cleaning_function`, which readers are recommended to
    # check out.

    # First, get the majority of columns “fixed”, i.e. their data types improved.
    df2 = (
        frame.withColumn("YEAR", psf.col("YEAR").cast(ShortType()))
        .withColumn("MONTH", psf.col("MONTH").cast(ByteType()))
        .withColumn("DAY_OF_MONTH", psf.col("DAY_OF_MONTH").cast(ByteType()))
        .withColumn("DAY_OF_WEEK", psf.col("DAY_OF_WEEK").cast(ByteType()))
        .withColumn("FL_DATE", psf.col("FL_DATE").cast(DateType()))
        .withColumn(
            "UNIQUE_CARRIER", psf.col("UNIQUE_CARRIER").cast(StringType())
        )
        .withColumn("TAIL_NUM", psf.col("TAIL_NUM").cast(StringType()))
        .withColumn(
            "FL_NUM", psf.col("FL_NUM").cast(ShortType())
        )  # values are between 1-8880, according to DataFrame.describe(), so this is fine
        .withColumn(
            "ORIGIN_AIRPORT_ID", psf.col("ORIGIN_AIRPORT_ID").cast(ShortType())
        )  # values are between 10k-17K
        .withColumn("ORIGIN", psf.col("ORIGIN").cast(StringType()))
        .withColumn(
            "ORIGIN_STATE_ABR", psf.col("ORIGIN_STATE_ABR").cast(StringType())
        )
        .withColumn(
            "DEST_AIRPORT_ID", psf.col("DEST_AIRPORT_ID").cast(IntegerType())
        )
        .withColumn("DEST", psf.col("DEST").cast(StringType()))
        .withColumn(
            "DEST_STATE_ABR", psf.col("DEST_STATE_ABR").cast(StringType())
        )
        .withColumn("CRS_DEP_TIME", psf.col("CRS_DEP_TIME").cast(StringType()))
        .withColumn("DEP_TIME", psf.col("DEP_TIME").cast(StringType()))
        .withColumn("DEP_DELAY", psf.col("DEP_DELAY").cast(ShortType()))
        .withColumn(
            "DEP_DELAY_NEW", psf.col("DEP_DELAY_NEW").cast(ShortType())
        )
        .withColumn("DEP_DEL15", psf.col("DEP_DEL15").cast(ShortType()))
        .withColumn(
            "DEP_DELAY_GROUP", psf.col("DEP_DELAY_GROUP").cast(ShortType())
        )
        .withColumn("TAXI_OUT", psf.col("TAXI_OUT").cast(ShortType()))
        .withColumn("WHEELS_OFF", psf.col("WHEELS_OFF").cast(StringType()))
        .withColumn("WHEELS_ON", psf.col("WHEELS_ON").cast(StringType()))
        .withColumn("TAXI_IN", psf.col("TAXI_IN").cast(ShortType()))
        .withColumn("CRS_ARR_TIME", psf.col("CRS_ARR_TIME").cast(StringType()))
        .withColumn("ARR_TIME", psf.col("ARR_TIME").cast(StringType()))
        .withColumn("ARR_DELAY", psf.col("ARR_DELAY").cast(ShortType()))
        .withColumn(
            "ARR_DELAY_NEW", psf.col("ARR_DELAY_NEW").cast(ShortType())
        )
        .withColumn("ARR_DEL15", psf.col("ARR_DEL15").cast(ShortType()))
        .withColumn(
            "ARR_DELAY_GROUP", psf.col("ARR_DELAY_GROUP").cast(ShortType())
        )
        .withColumn(
            "IS_CANCELLED",  # that's a personal preference: whenever I see a
            # boolean column, I believe it answers the question is_... or has_...
            # example: a column "broke" could indicate several things (what was
            # broken?), but a column "is_broke" simply answers the question
            # whether someone is financially bankrupt.
            psf.col("CANCELLED").cast(IntegerType()).cast(BooleanType()),
        )
        .withColumn(
            "CANCELLATION_CODE",
            psf.col("CANCELLATION_CODE").cast(StringType()),
        )
        .withColumn(
            "IS_DIVERTED",  # same explanation as CANCELLED
            psf.col("DIVERTED").cast(IntegerType()).cast(BooleanType()),
        )
        .withColumn(
            "CRS_ELAPSED_TIME", psf.col("CRS_ELAPSED_TIME").cast(ShortType())
        )
        .withColumn(
            "ACTUAL_ELAPSED_TIME",
            psf.col("ACTUAL_ELAPSED_TIME").cast(ShortType()),
        )
        .withColumn("AIR_TIME", psf.col("AIR_TIME").cast(ShortType()))
        .withColumn("FLIGHTS", psf.col("FLIGHTS").cast(ByteType()))
        .withColumn(
            "DISTANCE_IN_MILES", psf.col("DISTANCE").cast(IntegerType())
        )
        .withColumn(
            "DISTANCE_GROUP", psf.col("DISTANCE_GROUP").cast(ByteType())
        )  # values between 1-11 (DISTANCE_IN_MILES by 250 mile groups)
        # all delays are given in minutes
    ).drop(
        "_c44",  # this column is just empty, and unlabeled.
        # there's a column with no values in it, nor a name in the header (simply
        # a comma at the end of the header line)
        "CANCELLED",
        "DIVERTED",
    )

    # The tons of `withColumn("x", col("x").cast(some_data_type))` calls above are tedious.
    # A small improvement in terms of maintainability is this:
    for c in (
        "CARRIER_DELAY",
        "WEATHER_DELAY",
        "NAS_DELAY",
        "SECURITY_DELAY",
        "LATE_AIRCRAFT_DELAY",
    ):
        df2 = df2.withColumn(c, psf.col(c).cast(ShortType()))
    # And you could expand much more on this solution! See
    # 'a_shorter_cleaning_function', which is much shorter in terms of text
    # but might have the same number of lines as earlier, because of how
    # things get indented. Don't let yourself be fooled by that though:
    # it's the ease with which things can be changed and looked up that you
    # can gauge the quality of code.
    # Also notice how the code above does two things at the same time: did
    # you notice that some columns didn't just get a different data type
    # assigned, but also got their name changed? This can be confusing for
    # whoever is looking for the place where the columns got renamed.

    # Now, the columns that couldn't be dealt with by a mere “cast” can get
    # special care.
    # 1. Having year, month, day of month, day of week as columns that can be
    # derived from "fl_date" is ridiculous in a cleaned dataset. Such
    # derivations could be useful in later stages, like machine learning
    # pipelines, but then it's the *application owner* who should control the
    # transformations and give him/her the feeling that the data quality is high,
    # because he/she derives these new columns from source data. As an example:
    # do you know for sure that the combination of year-month-day_of_month
    # maches fl_date in every row?
    df3 = df2.drop("YEAR", "MONTH", "DAY_OF_MONTH", "DAY_OF_WEEK")

    flight_date_col = df3["FL_DATE"]
    for c in ("ARR_TIME", "DEP_TIME", "TAXI_OUT", "TAXI_IN"):
        df3 = df3.withColumn(
            c,
            combine_local_date_with_local_hour_minute_indication(
                flight_date_col, df3[c]
            ),
        )

    for colname in (
        "WHEELS_OFF",
        "WHEELS_ON",
        # There's at least one value in CRS_ARR_TIME that's at 2400. With a bit
        # of insight, you'll see then that CRS_DEP_TIME should also be handled
        # in the same way, even though in the sample you downloaded, this
        # column's values are below 2400.
        "CRS_ARR_TIME",
        "CRS_DEP_TIME",
    ):
        df3 = df3.withColumn(
            colname,
            combine_date_with_overflowed_minutes(
                flight_date_col, df3[colname]
            ),
        )
    # ARR_DELAY, ARR_DELAY_NEW and ARR_DEL15 are derived columns.
    # They're the result of the formulas:
    # ARR_DELAY = ARR_TIME - CRS_ARR_TIME
    # ARR_DELAY_NEW = max(0, ARR_DELAY)
    # ARR_DELAY_15 = ARR_DELAY_NEW >= 15
    # ARR_DELAY_GROUP is also derivable: every (15-minutes from < -15 to >180)

    return df3.drop(
        "DEP_DELAY",
        "DEP_DELAY_NEW",
        "DEP_DEL15",
        "DEP_DELAY_GROUP",
        "ACTUAL_ELAPSED_TIME",
        "AIR_TIME",
    )


def to_bool(
    c: str, true_values: Collection[str], false_values: Collection[str]
) -> Column:
    """If the values in the column named c match any of the values in the
    collection of true_values, the value will become True. Otherwise, if it
    matches any of those in the Collection of false_Values, it will convert the
    value to False. If neither matches, return null."""
    return psf.when(psf.col(c).isin(true_values), True).otherwise(
        psf.when(psf.col(c).isin(false_values), False)
    )


def drop_redundant_columns(frame: DataFrame) -> DataFrame:
    easy_drops = {
        "YEAR",
        "MONTH",
        "DAY_OF_MONTH",
        "DAY_OF_WEEK",
        "DEP_DELAY_NEW",
        "DEP_DEL15",
        "DEP_DELAY_GROUP",
        "ARR_DELAY_NEW",
        "ARR_DEL15",
        "ARR_DELAY_GROUP",
        "DISTANCE_GROUP",
        "FLIGHTS",  # always 1.0, probably a remnant from some grouping logic
        # _c44 is present because of the empty column at the end: Spark
        # automatically assigns column names. This is the 45th column, Spark
        # also starts counting from 0.
        "_c44",
    }
    return frame.drop(*easy_drops)


def correct_datatypes(frame: DataFrame) -> DataFrame:
    mapping = {
        ShortType: {
            "FL_NUM",  # 1-8880
            "CRS_ARR_TIME",
            "CRS_DEP_TIME",
            "ARR_TIME",
            "DEP_TIME",
            "WHEELS_OFF",  # 1-2400, always leftpadded to 4 digits
            "WHEELS_ON",  # 1-2400, always letpadded to 4 digits
            "ORIGIN_AIRPORT_ID",  # numbers between 10135 and 16218
            "DEST_AIRPORT_ID",  # same comment as ORIGIN_AIRPORT_ID
            "TAXI_OUT",  # values between 1.00 and 173.00, all with zeroes behind the period
            "TAXI_IN",  # Same, range between 1.00 and 161.00
            "DISTANCE",  # Earth's circumference in miles is 24901, which still fits in a Short.
            # The various x_DELAY columns are expressed in minutes. With the
            # maximum value for a short (2^15-1), one could easily get to 22
            # days that way. It is unlikely any airline would resume a flight
            # after 22 days: they'd create a new one.
            "CARRIER_DELAY",
            "WEATHER_DELAY",
            "NAS_DELAY",
            "SECURITY_DELAY",
            "LATE_AIRCRAFT_DELAY",
            "DEP_DELAY",
            "ARR_DELAY",
            # The following x_TIME columns follow a similar reasoning as the _DELAY columns
            "CRS_ELAPSED_TIME",
            "ACTUAL_ELAPSED_TIME",
            "AIR_TIME",
        },
        DateType: {
            "FL_DATE",
        },
        StringType: {
            "UNIQUE_CARRIER",  # always 2 characters
            "TAIL_NUM",  # nullable, 5 or 6 chars
            "DEST",
            "ORIGIN",  # 3 chars
            "DEST_STATE_ABR",
            "ORIGIN_STATE_ABR",  # 2 chars, in principle derivable from ORIGIN
            "CANCELLATION_CODE",  # 1 char
            "CANCELLED",  # to be converted to bool with a special function
            "DIVERTED",  # to be converted to bool with a special function
        },
    }

    for datatype, colnames in mapping.items():
        for colname in colnames:
            frame = frame.withColumn(
                colname, psf.col(colname).cast(datatype())
            )

    for column_name in ("CANCELLED", "DIVERTED"):
        # Spark can "automatically" convert values like 1 and 0, even "y" and "n" to bool, but not 1.0 and 0.0
        frame = frame.withColumn(
            column_name,
            to_bool(column_name, true_values={"1.0"}, false_values={"0.0"}),
        )
    return frame


def combine_date_with_time_with_graceful_time_overflow(
    frame: DataFrame, date_col: str, time_col: str
) -> DataFrame:
    # some of the time_col columns go from 0 to 2400 (inclusive), which Spark
    # will rightfully claim is not a correct time format. Those edge cases are
    # solved with a when-clause. The non-edge cases are dealt with by simply
    # concatenating the pieces and parsing it using the correct timestamp
    # format.
    return frame.withColumn(
        time_col,
        psf.when(frame[time_col] == 2400, psf.date_add(date_col, 1)).otherwise(
            combine_local_date_with_local_hour_minute_indication(
                date_col, time_col
            )
        ),
    )


def make_timestamps_out_of_time_notations(frame: DataFrame) -> DataFrame:
    # Remark: all times are given in the local time of the airport. That would
    # be okay, if we'd also have the flight date in local time, which is not
    # the case. We don't have the date of arrival in local time and there is no
    # straightforward way to derive it, as we'd need the local time zone info:
    # you could add the flight duration to the departure time, but then you'd
    # get the arrival time in the departure timezone, which you could convert
    # to the arrival timezone, if you had both names!
    # For this exercise, we ignore time zones, as we'd need to have a table
    # giving the Olson timezone information (e.g. Europe/Brussels) per airport.
    # We also assume (erroneously!) that the local arrival date is the same as
    # the local departure date.

    for col in (
        "scheduled_departure_time",
        "dep_time",
        "wheels_off",
        "scheduled_arrival_time",
        "arr_time",
        "wheels_on",
    ):
        frame = combine_date_with_time_with_graceful_time_overflow(
            frame, "flight_date", col
        )
    return frame


def a_shorter_cleaning_function(frame: DataFrame) -> DataFrame:
    # Note that this is not "shorter" because everything has pushed into
    # 3 functions. If you would combine those three functions here, you still
    # get less lines of code than the naive_clean(). However, splitting the
    # functionality into smaller blocks provides clarity: just from reading the
    # lines below, you get immediately an idea of what is happening, without
    # needing to read lots of details. This is known as choosing the right levels
    # of abstraction.
    for transformation in (
        drop_redundant_columns,
        correct_datatypes,
        improve_columns_names,
        make_timestamps_out_of_time_notations,
    ):
        frame = transformation(frame)
    return frame

    # An alternative, which uses method chaining, which some programmers prefer,
    # though it does require redundant typing of "transform", similar to chaining
    # withColumn a dozen times:
    # return (
    #     frame.transform(drop_redundant_columns)
    #     .transform(correct_datatypes)
    #     .transform(batch_rename_columns)
    # )
    # This is a *preference*: Spark will return the same query plan, so there's no
    # performance win.


def improve_columns_names(frame: DataFrame) -> DataFrame:
    renames = {
        "FL_DATE": "flight_date",
        # Something that contains letters as well digits should not be called
        # a "number", that's just plain confusing:
        "TAIL_NUM": "tail_code",
        "FL_NUM": "flight_number",
        # You could take two airports and cross-reference the distance between
        # them. In principle, "distance" here isn't clear enough: is it the
        # distance between the airports, or is it the distance flown
        # (an airplane does not need to follow a straight line)? In this case,
        # it's the former. And then one could actually derive that. However,
        # such a lookup (or even the great-circle-distance calculation) isn't
        # exactly cheap either. A lookup requires a join and the great-circle-
        # distance a lot of math involving trigonometric functions which
        # require more CPU cycles than a simple sum or a square.
        "DISTANCE": "distance_between_airports_in_miles",
        "CRS_ARR_TIME": "scheduled_arrival_time",
        "CRS_DEP_TIME": "scheduled_departure_time",
        # boolean columns are better served with an 'is_' or 'has_' prefix.
        "CANCELLED": "is_cancelled",
        "DIVERTED": "is_diverted",
        "CRS_ELAPSED_TIME": "planned_duration_in_minutes",
        "ACTUAL_ELAPSED_TIME": "actual_duration_in_minutes",
        "AIR_TIME": "in_air_duration_in_minutes",
    }
    duration_renames = {
        k.upper(): f"{k}_in_minutes"
        for k in (
            "carrier_delay",
            "weather_delay",
            "nas_delay",
            "security_delay",
            "late_aircraft_delay",
        )
    }
    renames.update(duration_renames)
    frame = batch_rename_columns(frame, renames)
    # let's also lowercase all column names, to offer a similar style (and less pressing of SHIFT with the pinky)
    return lowercase_column_names(frame)


def lowercase_column_names(frame: DataFrame) -> DataFrame:
    mapping = {c: c.lower() for c in frame.columns}
    return batch_rename_columns(frame, mapping)


def batch_rename_columns(
    df: DataFrame, mapping: Mapping[str, str]
) -> DataFrame:
    for old_name, new_name in mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


if __name__ == "__main__":
    # use relative paths, so that the location of this project on your system
    # won't mean editing paths
    path_to_exercises = Path(__file__).parents[1]
    resources_dir = path_to_exercises / "resources"
    target_dir = path_to_exercises / "target"
    # Create the folder where the results of this script's ETL-pipeline will
    # be stored.
    target_dir.mkdir(exist_ok=True)

    # Extract
    frame = read_data(resources_dir / "flights")
    # Transform
    cleaned_frame = a_shorter_cleaning_function(frame)
    cleaned_frame.explain()  # instructional to see how Spark optimized the operations we requested.
    # Load
    cleaned_frame.show()
    cleaned_frame.printSchema()
    cleaned_frame.write.parquet(
        path=str(target_dir / "cleaned_flights"),
        mode="overwrite",
        # Exercise: how much bigger are the files when the compression codec is set to "uncompressed"? And 'gzip'?
        compression="snappy",
    )
    # assuming 8 files get made:
    # source files (for comparison): 333MB
    # uncompressed: 138MB
    # snappy: 91MB
    # gzip: 74MB
    # note that ordering of the rows also plays a role (run-length encoding at play here)
