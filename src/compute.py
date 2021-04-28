# Read data and do calculations
# example of run: pyspak < compute.py > compute.log 2>&1
#

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, FloatType
from pyspark.sql.functions import *
from pyspark.sql import Row

spark = SparkSession.builder \
  .master("local") \
  .appName("paceline_compute") \
  .getOrCreate()

# set input file/folder
curated_dir='curated/openpowerlifting'
# output folder/files
wl_percsquad_in_123_attemp='analytics/wl_percsquad_in_123_attemp'
first_wl_percsquad_in_123_attemp='analytics/first_wl_percsquad_in_123_attemp'
record_holder_wl_by_country_deadlift='analytics/record_holder_wl_by_country_in_deadlift'

# read parquet file(s) to dataframe
df = spark.read.parquet("curated/openpowerlifting")

# show data to browse
df.select("Name","Sex","Event","Equipment","Age","AgeClass","Division","BodyweightKg","WeightClassKg", \
          "Squat1Kg","Squat2Kg","Squat3Kg","Squat4Kg","Best3SquatKg","Place","Country","MeetCountry","MeetName") \
		.filter("Best3SquatKg IS NOT NULL").show(20);

### Compute the percentage with which weighlifters achieve their best squad result in their first, second and third attempt each.
countBest3SquatKg = df.filter("Best3SquatKg IS NOT NULL").count();
percBestSquat1Kg = round(df.filter("Best3SquatKg IS NOT NULL AND Squat1Kg = Best3SquatKg").count()*100/countBest3SquatKg,2)
percBestSquat2Kg = round(df.filter("Best3SquatKg IS NOT NULL AND Squat2Kg = Best3SquatKg").count()*100/countBest3SquatKg,2)
percBestSquat3Kg = round(df.filter("Best3SquatKg IS NOT NULL AND Squat3Kg = Best3SquatKg").count()*100/countBest3SquatKg,2)
# Define schema, import data to dataframe and export data as csv
percBestSquatSchema = StructType([
    StructField("percBestSquat1Kg", FloatType(), True),
    StructField("percBestSquat2Kg", FloatType(), True),
    StructField("percBestSquat3Kg", FloatType(), True)
])
percBestSquat = [(percBestSquat1Kg,percBestSquat2Kg,percBestSquat3Kg)]
percBestSquatDF = spark.createDataFrame(data=percBestSquat, schema=percBestSquatSchema)
# save as csv
percBestSquatDF.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite') \
	.option("header", "true").save(wl_percsquad_in_123_attemp);

### Compute the same as above, but only considering participants placing first in their competition.
countFirstBest3SquatKg = df.filter("Best3SquatKg IS NOT NULL AND Place = 1").count();
percFirstBestSquat1Kg = round(df.filter("Best3SquatKg IS NOT NULL AND Place = 1 AND Squat1Kg = Best3SquatKg").count()*100/countFirstBest3SquatKg,2)
percFirstBestSquat2Kg = round(df.filter("Best3SquatKg IS NOT NULL AND Place = 1 AND Squat2Kg = Best3SquatKg").count()*100/countFirstBest3SquatKg,2)
percFirstBestSquat3Kg = round(df.filter("Best3SquatKg IS NOT NULL AND Place = 1 AND Squat3Kg = Best3SquatKg").count()*100/countFirstBest3SquatKg,2)
# Define schema, import data to dataframe and export data as csv
percFirstBestSquatSchema = StructType([
    StructField("percFirstBestSquat1Kg", FloatType(), True),
    StructField("percFirstBestSquat2Kg", FloatType(), True),
    StructField("percFirstBestSquat3Kg", FloatType(), True)
])
percFirstBestSquat = [(percFirstBestSquat1Kg,percFirstBestSquat2Kg,percFirstBestSquat3Kg)]
percFirstBestSquatDF = spark.createDataFrame(data=percBestSquat, schema=percBestSquatSchema)
# save as csv
percFirstBestSquatDF.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite') \
	.option("header", "true").save(first_wl_percsquad_in_123_attemp);

### For each country, determine the weighlifter who holds the record in deadlifting over the whole dataset.
# add not null country column to dataset
df = df.withColumn('wlCountry', coalesce(df['Country'], df['MeetCountry']).cast(StringType()))
df = df.withColumn('wlBest3DeadliftKg', coalesce(df['Best3DeadliftKg'], lit('0')).cast(FloatType()))
wlRecordInDeadliftKgDF = df.groupBy("wlCountry").agg(max("wlBest3DeadliftKg").alias("wlBest3DeadliftKg"))
wlNameRecordInDeadliftKgDF = wlRecordInDeadliftKgDF.alias("wldf") \
	.join(df.alias("df"), (col("wldf.wlCountry") == col("df.wlCountry")) \
		& (col("wldf.wlBest3DeadliftKg") == col("df.wlBest3DeadliftKg")),"inner") \
	.select(col("wldf.wlCountry"),col("wldf.wlBest3DeadliftKg"),col("df.Name")) \
	.distinct().sort("wlCountry","Name")
# save as csv
wlNameRecordInDeadliftKgDF.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite') \
	.option("header", "true").csv(record_holder_wl_by_country_deadlift);

