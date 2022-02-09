"""
This module provides two functionally equivalent ways to produce a wide table,
directly usable for reporting (see num_flights.py for some derived stats).
The first alternative, `produce360view` shows how some people would initially
approach the problem. It's very straightforward and is functionally correct.
The second alternative shows an approach that is arguably more maintainable and
programmer-friendly. It starts by describing the high level tasks, and each
high-level task is in its own function, thereby making pieces easier to test.
Some of those tasks call on yet smaller pieces of functions, which could be
reused across different projects. Re-use of tested code allows you to create
value faster.
"""
from typing import Callable

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col

from exercises.i_catalog.catalog import catalog, load_frame_from_catalog


def produce360view(
    flights: DataFrame, airports: DataFrame, carriers: DataFrame
) -> DataFrame:
    return (
        flights.join(
            airports,
            on=airports["AIRPORT"] == flights["origin"],
            how="left",
        )
        .drop(airports["AIRPORT"])
        .withColumnRenamed("DISPLAY_AIRPORT_NAME", "origin_description")
        .join(
            airports,
            on=airports["AIRPORT"] == flights["dest"],
            how="left",
        )
        .drop(airports["AIRPORT"])
        .withColumnRenamed("DISPLAY_AIRPORT_NAME", "destination_description")
        .join(
            carriers,
            on=(
                (carriers["CARRIER"] == col("unique_carrier"))
                & (carriers["START_DATE_SOURCE"] <= flights["flight_date"])
                & (
                    (carriers["THRU_DATE_SOURCE"] >= flights["flight_date"])
                    | carriers["THRU_DATE_SOURCE"].isNull()
                )
            ),
        )
        .drop("CARRIER", "THRU_DATE_SOURCE", "START_DATE_SOURCE")
    )


def join_with(
    airports: DataFrame, on: str
) -> Callable[[DataFrame], DataFrame]:
    """This is an example of a curried function"""

    def inner(df: DataFrame) -> DataFrame:
        return df.join(airports, airports["AIRPORT"] == df[on], "left").drop(
            airports["AIRPORT"]
        )

    return inner


def event_in_period(
    event: Column, period_start: Column, period_stop: Column
) -> Column:
    """Checks if event took place between period_start and period_stop.
    Period_stop can be open-ended."""
    return (period_start <= event) & (
        (event <= period_stop) | period_stop.isNull()
    )


def lookup_carrier_names(
    carriers: DataFrame,
) -> Callable[[DataFrame], DataFrame]:
    def inner(df: DataFrame) -> DataFrame:
        has_same_carrier_code = carriers["CARRIER"] == df["unique_carrier"]
        return df.join(
            carriers,
            on=(
                has_same_carrier_code
                & event_in_period(
                    event=df["flight_date"],
                    period_start=carriers["START_DATE_SOURCE"],
                    period_stop=carriers["THRU_DATA_SOURCE"],
                )
            ),
        ).drop("CARRIER", "THRU_DATE_SOURCE", "START_DATE_SOURCE")

    return inner


def produce360view_alternative(
    flights: DataFrame, airports: DataFrame, carriers: DataFrame
) -> DataFrame:
    code = "DISPLAY_AIRPORT_NAME"
    airports_origin, airports_destination = [
        airports.withColumnRenamed(code, f"{src_dest}_description")
        for src_dest in ("origin", "destination")
    ]

    return (
        flights.transform(join_with(airports_origin, on="origin"))
        .transform(join_with(airports_destination, on="dest"))
        .transform(lookup_carrier_names(carriers))
    )


if __name__ == "__main__":

    flights = load_frame_from_catalog("clean_flights")
    airports = load_frame_from_catalog("clean_airports")
    carriers = load_frame_from_catalog("clean_carriers")

    master_table = produce360view(flights, airports, carriers)

    master = catalog["master_flights"]
    master_table.write.save(
        str(master.path), mode="overwrite", format=master.format
    )
