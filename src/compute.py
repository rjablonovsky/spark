# Read data and do calculations
# example of run: pyspark < src/compute.py > log/compute.log 2>&1
#

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, FloatType
from pyspark.sql import functions as F
from pyspark.sql import Row
# to modify output to csv file 
import os, glob, shutil


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
df.select().filter("wlBest3SquatKg IS NOT NULL").show(20);

### Compute the percentage with which weighlifters achieve their best squad result in their first, second and third attempt each.
# Approach 1 - with data quality checking and logic inside calculation:
countBestSquat1Kg = df.filter("wlSquat1Kg IS NOT NULL AND wlSquat1Kg > 0 AND wlSquat1Kg >= coalesce(wlSquat2Kg,0) AND wlSquat1Kg >= coalesce(wlSquat3Kg,0)").count();
countBestSquat2Kg = df.filter("wlSquat2Kg IS NOT NULL AND wlSquat2Kg > 0 AND wlSquat2Kg > coalesce(wlSquat1Kg,0) AND wlSquat2Kg >= coalesce(wlSquat3Kg,0)").count();
countBestSquat3Kg = df.filter("wlSquat3Kg IS NOT NULL AND wlSquat3Kg > 0 AND wlSquat3Kg > coalesce(wlSquat1Kg,0) AND wlSquat3Kg > coalesce(wlSquat2Kg,0)").count();
countBest3SquatKg = countBestSquat1Kg + countBestSquat2Kg + countBestSquat3Kg
percBestSquat1Kg = round(countBestSquat1Kg * 100.0 / countBest3SquatKg, 2)
percBestSquat2Kg = round(countBestSquat2Kg * 100.0 / countBest3SquatKg, 2)
percBestSquat3Kg = round(countBestSquat3Kg * 100.0 / countBest3SquatKg, 2)
#print(countBest3SquatKg, 100, countBestSquat1Kg, percBestSquat1Kg, countBestSquat2Kg, percBestSquat2Kg, countBestSquat3Kg, percBestSquat3Kg)
''' # Approach 2 is more susceptible to data quality issues or require more thorough data cleaning
countBest3SquatKg = df.filter("(wlBest3SquatKg IS NOT NULL AND wlBest3SquatKg > 0 AND (coalesce(wlSquat1Kg,0) = wlBest3SquatKg OR coalesce(wlSquat2Kg,0) = wlBest3SquatKg OR coalesce(wlSquat3Kg,0) = wlBest3SquatKg))").count()
countBestSquat1Kg = df.filter("wlBest3SquatKg IS NOT NULL AND wlBest3SquatKg > 0 AND (coalesce(wlSquat1Kg,0) = wlBest3SquatKg)").count()
countBestSquat2Kg = df.filter("wlBest3SquatKg IS NOT NULL AND wlBest3SquatKg > 0 AND (coalesce(wlSquat2Kg,0) = wlBest3SquatKg)").count()
countBestSquat3Kg = df.filter("wlBest3SquatKg IS NOT NULL AND wlBest3SquatKg > 0 AND (coalesce(wlSquat3Kg,0) = wlBest3SquatKg)").count()
percBestSquat1Kg = round(countBestSquat1Kg * 100.0 / countBest3SquatKg, 2)
percBestSquat2Kg = round(countBestSquat2Kg * 100.0 / countBest3SquatKg, 2)
percBestSquat3Kg = round(countBestSquat3Kg * 100.0 / countBest3SquatKg, 2)
print(countBest3SquatKg, 100, countBestSquat1Kg, percBestSquat1Kg, countBestSquat2Kg, percBestSquat2Kg, countBestSquat3Kg, percBestSquat3Kg)
'''
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
# to modify output to single csv file and remove output dir
for csvfile in glob.glob(wl_percsquad_in_123_attemp+"/*.csv"):
    os.rename(csvfile, wl_percsquad_in_123_attemp+'.csv')
shutil.rmtree(wl_percsquad_in_123_attemp)	
	
### Compute the same as above, but only considering participants placing first in their competition.
# Approach 1 - with data quality checking and logic inside calculation. Assumption is the column wlPlace has correct value for winner of competition.
countFirstBestSquat1Kg = df.filter("wlPlace = 1 AND wlSquat1Kg IS NOT NULL AND wlSquat1Kg > 0 AND wlSquat1Kg >= coalesce(wlSquat2Kg,0) AND wlSquat1Kg >= coalesce(wlSquat3Kg,0)").count();
countFirstBestSquat2Kg = df.filter("wlPlace = 1 AND wlSquat2Kg IS NOT NULL AND wlSquat2Kg > 0 AND wlSquat2Kg > coalesce(wlSquat1Kg,0) AND wlSquat2Kg >= coalesce(wlSquat3Kg,0)").count();
countFirstBestSquat3Kg = df.filter("wlPlace = 1 AND wlSquat3Kg IS NOT NULL AND wlSquat3Kg > 0 AND wlSquat3Kg > coalesce(wlSquat1Kg,0) AND wlSquat3Kg > coalesce(wlSquat2Kg,0)").count();
countFirstBest3SquatKg = countFirstBestSquat1Kg + countFirstBestSquat2Kg + countFirstBestSquat3Kg
percFirstBestSquat1Kg = round(countFirstBestSquat1Kg * 100 / countFirstBest3SquatKg, 2)
percFirstBestSquat2Kg = round(countFirstBestSquat2Kg * 100 / countFirstBest3SquatKg, 2)
percFirstBestSquat3Kg = round(countFirstBestSquat3Kg * 100 / countFirstBest3SquatKg, 2)
#print(countFirstBest3SquatKg, 100, countFirstBestSquat1Kg, percFirstBestSquat1Kg, countFirstBestSquat2Kg, percFirstBestSquat2Kg, countFirstBestSquat3Kg, percFirstBestSquat3Kg)
''' # Approach 2 is more susceptible to data quality issues or require more thorough data cleaning
countFirstBest3SquatKg = df.filter("(wlPlace = 1 AND wlBest3SquatKg IS NOT NULL AND wlBest3SquatKg > 0 AND (coalesce(wlSquat1Kg,0) = wlBest3SquatKg OR coalesce(wlSquat2Kg,0) = wlBest3SquatKg OR coalesce(wlSquat3Kg,0) = wlBest3SquatKg))").count()
countFirstBestSquat1Kg = df.filter("wlPlace = 1 AND wlBest3SquatKg IS NOT NULL AND wlBest3SquatKg > 0 AND (coalesce(wlSquat1Kg,0) = wlBest3SquatKg)").count()
countFirstBestSquat2Kg = df.filter("wlPlace = 1 AND wlBest3SquatKg IS NOT NULL AND wlBest3SquatKg > 0 AND (coalesce(wlSquat2Kg,0) = wlBest3SquatKg)").count()
countFirstBestSquat3Kg = df.filter("wlPlace = 1 AND wlBest3SquatKg IS NOT NULL AND wlBest3SquatKg > 0 AND (coalesce(wlSquat3Kg,0) = wlBest3SquatKg)").count()
percFirstBestSquat1Kg = round(countFirstBestSquat1Kg * 100.0 / countFirstBest3SquatKg, 2)
percFirstBestSquat2Kg = round(countFirstBestSquat2Kg * 100.0 / countFirstBest3SquatKg, 2)
percFirstBestSquat3Kg = round(countFirstBestSquat3Kg * 100.0 / countFirstBest3SquatKg, 2)
print(countFirstBest3SquatKg, 100, countFirstBestSquat1Kg, percFirstBestSquat1Kg, countFirstBestSquat2Kg, percFirstBestSquat2Kg, countFirstBestSquat3Kg, percFirstBestSquat3Kg)
'''
# Define schema, import data to dataframe and export data as csv
percFirstBestSquatSchema = StructType([
    StructField("percFirstBestSquat1Kg", FloatType(), True),
    StructField("percFirstBestSquat2Kg", FloatType(), True),
    StructField("percFirstBestSquat3Kg", FloatType(), True)
])
percFirstBestSquat = [(percFirstBestSquat1Kg,percFirstBestSquat2Kg,percFirstBestSquat3Kg)]
percFirstBestSquatDF = spark.createDataFrame(data=percFirstBestSquat, schema=percFirstBestSquatSchema)
# save as csv
percFirstBestSquatDF.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite') \
	.option("header", "true").save(first_wl_percsquad_in_123_attemp);
# to modify output to single csv file and remove output dir
for csvfile in glob.glob(first_wl_percsquad_in_123_attemp+"/*.csv"):
    os.rename(csvfile, first_wl_percsquad_in_123_attemp+'.csv')
shutil.rmtree(first_wl_percsquad_in_123_attemp)

### For each country, determine the weighlifter who holds the record in deadlifting over the whole dataset.
# add not null country column to dataset and if Country is null replace it with MeetCountry. 
#   Assumption 1 - the non-international competition did not provide information about Country
#   Assumption 2 - the wlBest3DeadliftKg is reliable enough for identifying recordholder 
wlRecordInDeadliftKgDF = df.groupBy("wlCountry").agg(F.max("wlBest3DeadliftKg").alias("wlBest3DeadliftKg"))
wlNameRecordInDeadliftKgDF = wlRecordInDeadliftKgDF.alias("wldf") \
	.join(df.alias("df"), (F.col("wldf.wlCountry") == F.col("df.wlCountry")) \
		& (F.col("wldf.wlBest3DeadliftKg") == F.col("df.wlBest3DeadliftKg")),"inner") \
	.select(F.col("wldf.wlCountry"), F.col("wldf.wlBest3DeadliftKg"), F.col("df.wlName")) \
	.distinct().sort("wlCountry","wlName")
# save as csv
wlNameRecordInDeadliftKgDF.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite') \
	.option("header", "true").csv(record_holder_wl_by_country_deadlift);
# to modify output to single csv file and remove output dir
for csvfile in glob.glob(record_holder_wl_by_country_deadlift+"/*.csv"):
    os.rename(csvfile, record_holder_wl_by_country_deadlift+'.csv')
shutil.rmtree(record_holder_wl_by_country_deadlift)
