# read the CSV data to a PySpark DataFrame and write it out in the Parquet format
# require spark 3.0+ and python (3.6-3.8)
# run example: pyspark < src/convert_csv_to_parquet.py > log/convert_csv_to_parquet.log 2>&1
# 

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, FloatType
from pyspark.sql import functions as F
from pyspark.sql import Row

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
print("dtypes: ", df.dtypes)
print("Computes summary statistics: ")
df.describe().show()
print("Show first 3 rows: ")
df.show(3)
# Counts the number of rows and distinc rows in dataframe
print("All row count: ", df.count())
print("Distinct row count: ", df.distinct().count())
print("Check distinct column Name row count. Should be 412574: ", df.select("Name").distinct().count())
print("Check if column Name contains is null value. Should be 0: ", df.filter(df["Name"].isNull()).count())
print("Check for the squat data where Best3SquatKg does not equal SquatKg 1,2 or 3. Should be 0: ",
	df.filter("Best3SquatKg IS NOT NULL AND Squat1Kg != Best3SquatKg AND Squat2Kg != Best3SquatKg AND Squat3Kg != Best3SquatKg").count()
)
print("Check for the squat data where Best3SquatKg does not equal SquatKg 1,2 or 3. Should be 0: ",
	df.filter("Best3SquatKg IS NOT NULL AND (Squat1Kg = Best3SquatKg OR Squat2Kg = Best3SquatKg OR Squat3Kg = Best3SquatKg)").count()
)

# write data in parque format into dir with one partition
df.repartition(1).write.mode('overwrite').parquet(curated_dir)
