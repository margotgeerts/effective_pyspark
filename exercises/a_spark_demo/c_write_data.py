"""
If you can read data from a file system, you can also write data to a file
system. The syntax is very symmetric.

The concept of partitions is introduced.
"""

import pyspark.sql.functions as psf

from exercises.shared import ministers, resources_dir

ministers.printSchema()

better_frame = ministers.withColumn(
    "entered_office_on", psf.to_date("entered_office_on")
)
better_frame.show()
better_frame.printSchema()
better_frame.repartition(3).write.parquet(
    str(resources_dir / "as_parquet"), mode="overwrite"
)
# Now look up how many parquet files were written to that location. Is there a
# correlation?
