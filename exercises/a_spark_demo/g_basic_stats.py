"""
Often you will want to describe a DataFrame, without going in too much detail.
For example, listing the number of rows, or the number of columns, and what the
datatypes are for those columns. These attributes are either attributes of the
DataFrame instance, or they need to be computed, like `DataFrame.count()`.

`DataFrame.describe()` computes some simple metrics on each column of a
DataFrame, while still being a single-pass operation. It is often useful when
you want to know a bit about the distribution of data in a column.  Note that
the mean and stddev are not specified for columns of datatypes where that does
not make sense, like strings and dates. Also observe that `count` gives the
non-null count.
"""

from exercises.shared import ministers

# Recap
ministers.show()
ministers.printSchema()

################################################################################
# Attributes that don't trigger work on the executors
################################################################################
# The schema property is an attribute of a DataFrame. Unlike the `printSchema`
# method, you can do something with this property's (return) value.
print(ministers.schema)
# For example, you can access its fields, which is the sequence of objects
# containing column name, data type and nullability information.
print(ministers.schema.fields)
# If you're only interested in the column names, simply use DataFrame.columns.
# Behind its simplistic accessor hides a function that goes back to the fields.
# Navigate to the source code to verify this!
print(ministers.columns)
# You could also be interested in the column names and respective data types,
# for example when you need to modify only the string columns (like but them all
# in lowercase).
print(ministers.dtypes)

################################################################################
# Attributes that trigger work on the executors
################################################################################
# Unlike the previous commands, which access pre-computed attributes of the
# class instance, DataFrame.count() is an action, so it takes time to compute.
print(ministers.count())
# Similarly, `DataFrame.describe()` requires a pass over the data, but it's
# lazy (it generates a new DataFrame), so you'll have to call an action on it,
# to see its results.
ministers.describe().show(truncate=False)
