# Read data and do calculations
# example of run: pyspark < src/job_analytics.py > log/job_analytics.log 2>&1
#

# to modify output to csv file 
import os, glob, shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, LongType, DoubleType
from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.conf import SparkConf

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.addFile("src/wlUtil.py")
sc.addFile("src/wlCalc.py")

import wlUtil, wlCalc


### globals
# set input file and output folder
curated_dir='curated/openpowerlifting'
# output folder/files
wl_percsquad_in_123_attemp='analytics/wl_percsquad_in_123_attemp.csv'
first_wl_percsquad_in_123_attemp='analytics/first_wl_percsquad_in_123_attemp.csv'
record_holder_wl_by_country_deadlift='analytics/record_holder_wl_by_country_in_deadlift.csv'

### start main calculation
spark = SparkSession.builder \
  .master("local") \
  .appName("paceline_compute") \
  .getOrCreate()

### Read csv data do datatype and default conversions and write data into parque
# read parquet file(s) to dataframe
dfT = spark.read.parquet("curated/openpowerlifting")
# show data to browse - sanity check
dfT.filter("Best3SquatKg IS NOT NULL").show(5);

### Compute the percentage with which weighlifters achieve their best squad result in their first, second and third attempt each.
# Formula 1 - with data quality checking and logic inside calculation:
percBestSquatDF = wlCalc.calcPercBestSquat_Formula1(spark,dfT)
wlUtil.writeDFtoCSVfile(percBestSquatDF, wl_percsquad_in_123_attemp)
# Formula 2 is more susceptible to data quality issues or require more thorough data cleaning
percBestSquatDF2 = wlCalc.calcPercBestSquat_Formula2(spark,dfT)
wlUtil.writeDFtoCSVfile(percBestSquatDF2, wl_percsquad_in_123_attemp+'2')
	
### Compute the same as above, but only considering participants placing first in their competition.
# Formula 1 - with data quality checking and logic inside calculation. Assumption is the column wlPlace has correct value for winner of competition.
percFirstBestSquatDF = wlCalc.calcPercFirstBestSquat_Formula1(spark, dfT)
wlUtil.writeDFtoCSVfile(percFirstBestSquatDF, first_wl_percsquad_in_123_attemp)
# Formula 2 is more susceptible to data quality issues or require more thorough data cleaning
percFirstBestSquatDF2 = wlCalc.calcPercFirstBestSquat_Formula2(spark, dfT)
wlUtil.writeDFtoCSVfile(percFirstBestSquatDF2, first_wl_percsquad_in_123_attemp+'2')

### For each country, determine the weighlifter who holds the record in deadlifting over the whole dataset.
# If Country is null replace it with MeetCountry. 
#   Assumption 1 - the non-international competition did not provide information about Country and MeetCouintry is good substitute
#   Assumption 2 - the Best3DeadliftKg is reliable enough for identifying recordholder 
wlNameRecordInDeadliftKgDF = wlCalc.getNameRecordInDeadliftKg_Formula1(dfT)
wlUtil.writeDFtoCSVfile(wlNameRecordInDeadliftKgDF, record_holder_wl_by_country_deadlift)
# Formula 2 - using window functions
wlNameRecordInDeadliftKgDF = wlCalc.getNameRecordInDeadliftKg_Formula2(dfT)
wlUtil.writeDFtoCSVfile(wlNameRecordInDeadliftKgDF, record_holder_wl_by_country_deadlift+'2')