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

# Manual/sanity check of dataframe column names, data types, statistic, counts - STEP 1
print("dtypes: ", df.dtypes)
print("Computes summary statistics: ")
df.describe().show()
print("Show first 3 rows: ")
df.show(3)
# Counts the number of rows and distinc rows in dataframe
print("All row count. Should be 1423354: ", df.count())
print("Distinct row count. Should be 1420270: ", df.distinct().count())
print("Check distinct column Name row count. Should be 412574: ", df.select("Name").distinct().count())
print("Check if column Name contains is null value. Should be 0: ", df.filter(df["Name"].isNull()).count())
print("Best3SquatKg IS NOT NULL. Should be 1031450: ", df.filter("Best3SquatKg IS NOT NULL").count())
print("Squat1Kg IS NOT NULL. Should be 337580: ", df.filter("Squat1Kg IS NOT NULL").count())
print("Squat2Kg IS NOT NULL. Should be 333349: ", df.filter("Squat2Kg IS NOT NULL").count())
print("Squat3Kg IS NOT NULL. Should be 323842: ", df.filter("Squat3Kg IS NOT NULL").count())
print("Check for the squat data where Best3SquatKg does not equal SquatKg 1,2 or 3. Should be 0: ",
	df.filter("Best3SquatKg IS NOT NULL AND Squat1Kg != Best3SquatKg AND Squat2Kg != Best3SquatKg AND Squat3Kg != Best3SquatKg").count()
)
print("Check for the squat data where Best3SquatKg does equal SquatKg 1 or 2 or 3. Should be 328584: ",
	df.filter("Best3SquatKg IS NOT NULL AND (Squat1Kg = Best3SquatKg OR Squat2Kg = Best3SquatKg OR Squat3Kg = Best3SquatKg)").count()
)

# create new columns with convertion string to float or double data type and NULL to 0
df = df.withColumn('wlName', df['Name'].cast(StringType()))
df = df.withColumn('wlSquat1Kg', df['Squat1Kg'].cast(FloatType()))
df = df.withColumn('wlSquat2Kg', df['Squat2Kg'].cast(FloatType()))
df = df.withColumn('wlSquat3Kg', df['Squat3Kg'].cast(FloatType()))
df = df.withColumn('wlBest3SquatKg', df['Best3SquatKg'].cast(FloatType()))
df = df.withColumn('wlPlace', df['Place'].cast(IntegerType()))
#   Assumption 1 - the non-international competition did not provide information about Country
df = df.withColumn('wlCountry', F.coalesce(df['Country'], df['MeetCountry']).cast(StringType()))
df = df.withColumn('wlBest3DeadliftKg', F.coalesce(df['Best3DeadliftKg'], F.lit('0')).cast(FloatType()))

# Manual/sanity check of dataframe column names, data types, statistic, counts - STEP 2
print("dtypes: ", df.dtypes)
print("Computes summary statistics after data conversion: ")
df.describe().show()
print("Show first 3 rows: ")
df.show(3)

# write data in parque format into dir with one partition
df.repartition(1).write.mode('overwrite').parquet(curated_dir)
