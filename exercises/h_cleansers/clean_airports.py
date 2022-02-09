from pyspark.sql import SparkSession

from exercises.i_catalog.catalog import catalog, load_frame_from_catalog


def main():
    df = load_frame_from_catalog("raw_airports").drop("_c2").dropDuplicates(["AIRPORT"])

    datalink = catalog["clean_airports"]
    df.write.save(
        path=str(datalink.path),
        mode="overwrite",
        format=datalink.format,
    )


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    # These dimension tables are so small, that Spark isn't the best tool to
    # process them.  However, it does offer a lot of convenience functions,
    # especially when working in a cloud environment. Consider pandas for local
    # development on such tiny datasets or even pure Python.

    # Here, we set this option explicitly, so that we don't create 200 tiny 
    # files from the call to distinct()
    import time
    time.sleep(60)
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    main()
