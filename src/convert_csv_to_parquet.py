# read the CSV data to a PySpark DataFrame and write it out in the Parquet format
# require spark 3.0+ and python (3.6-3.8)
# run example: pyspark < src/convert_csv_to_parquet.py
# 

from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .master("local") \
  .appName("paceline_convert_csv_to_parquet") \
  .getOrCreate()

# set input file and output folder
raw_file='raw/openpowerlifting.csv'
curated_dir='curated/openpowerlifting'

# read data into data frame
df = spark.read.csv(raw_file, header = True)

# Manual/sanity check of dataframe column names, data types, statistic, counts
df.dtypes
# Return first 1 rows
df.head(1)
# Return first 3 rows
df.show(3)
# Counts the number of rows and distinc rows in dataframe
df.count()
df.distinct().count()
# Counts the number of distinct Names in dataframe. Should be 412574
df.select("Name").distinct().count()
# Computes summary statistics: use for checking
df.describe().show()

# check if Name is null. Should be 0
df.filter(df["Name"].isNull()).count();

# write data in parque format into dir with one partition
df.repartition(1).write.mode('overwrite').parquet(curated_dir)
