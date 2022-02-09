import pyspark.sql.functions as psf

from exercises.i_catalog.catalog import catalog, load_frame_from_catalog


def main():
    df = load_frame_from_catalog("raw_carriers")
    for colname in ("START_DATE_SOURCE", "THRU_DATE_SOURCE"):
        df = df.withColumn(colname, psf.to_date(colname))
    df = df.drop("_c4").distinct()

    datalink = catalog["clean_carriers"]
    df.write.save(
        path=str(datalink.path),
        mode="overwrite",
        format=datalink.format,
    )


if __name__ == "__main__":
    main()
