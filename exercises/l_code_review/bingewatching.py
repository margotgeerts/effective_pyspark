import pyspark.sql.functions as fun
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

data_catalog = {"foo": "some_path"}

spark = SparkSession.builder.getOrCreate()

tv_sessions = spark.read.parquet(data_catalog["foo"])
tv_sessions.createOrReplaceTempView("tv_session")
tv_sessions = (
    tv_sessions.filter(
        (tv_sessions.year == 2018)
        & (tv_sessions.month >= 1)
        & (tv_sessions.mediaType == "series-videos")
        & (tv_sessions.reach60Srd == 1)
    )
    .filter(tv_sessions.source.isin("box", "corder"))
    .select(
        "customerNumber",
        "sessionRecordStartTime",
        "programGenreLevel",
        "programSeriesName",
        "programSeasonEpisode",
    )
    .withColumn(
        "date",
        fun.from_unixtime(
            fun.unix_timestamp(tv_sessions.sessionRecordStartTime),
            "yyyy-MM-dd",
        ),
    )
)

# tv_sessions.sessionRecordStartTime is a _timestamp_ type.

import pyspark.sql.functions as sparkfun


def substring_f(startpos, length):
    return sparkfun.udf(
        lambda column: column[startpos - 1 : startpos - 1 + length]
    )


tv_sessions = (
    tv_sessions.withColumn(
        "watch_month", substring_f(startpos=6, length=2)(tv_sessions.date)
    )
    .withColumn(
        "main_genre",
        substring_f(startpos=1, length=3)(tv_sessions.programGenreLevel),
    )
    .withColumn(
        "season",
        substring_f(startpos=16, length=2)(tv_sessions.programSeasonEpisode),
    )
    .withColumn(
        "episode",
        substring_f(startpos=19, length=4)(tv_sessions.programSeasonEpisode),
    )
)


tv_sessions = tv_sessions.filter(
    tv_sessions.watch_month.isin("01", "02", "03", "04", "05", "06")
).filter(
    (tv_sessions.main_genre == "13.")
    & (tv_sessions.programGenreLevel != "13.6")
)  # no soaps

### When did you see the first episode of a season of a series
firstepisodes = tv_sessions.where(tv_sessions.episode == "0001").select(
    col("customerNumber"),
    col("programSeriesName"),
    col("sessionRecordStartTime").alias("first_sessionRecordStartTime"),
    col("date").alias("first_date"),
    col("season"),
)

maxepisodes = tv_sessions.groupBy(
    "customerNumber", "programSeriesName", "season"
).agg(fun.max(tv_sessions.episode).alias("max_episode"))

maxdate = tv_sessions.join(
    maxepisodes,
    (tv_sessions["customerNumber"] == maxepisodes["customerNumber"])
    & (tv_sessions["programSeriesName"] == maxepisodes["programSeriesName"])
    & (tv_sessions["season"] == maxepisodes["season"])
    & (tv_sessions["episode"] == maxepisodes["max_episode"]),
    "inner",
).select(
    tv_sessions["customerNumber"],
    tv_sessions["programSeriesName"],
    tv_sessions["season"],
    maxepisodes["max_episode"],
    tv_sessions["sessionRecordStartTime"].alias("last_sessionRecordStartTime"),
    tv_sessions["last_date"].alias("last_date"),
)

joined = firstepisodes.join(
    maxdate, ["customerNumber", "programSeriesName", "season"], "left outer"
).drop("season")

joined_and_filtered = joined.withColumn(
    "days_spread",
    fun.datediff(
        joined.last_sessionRecordStartTime, joined.first_sessionRecordStartTime
    ),
)
joined_and_filtered = joined_and_filtered.where(
    joined_and_filtered.days_spread <= 7
)

joined_and_filtered.cache()
print(joined_and_filtered.take(2))
joined_and_filtered.write.parquet("bingewatchers")
