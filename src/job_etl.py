# Read data and do calculations
# example of run: pyspark < src/job_etl.py > log/job_etl.log 2>&1
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
sc.addFile("src/wlConvert.py")

import wlConvert


### globals
# set input file and output folder
raw_file='raw/openpowerlifting.csv'
curated_dir='curated/openpowerlifting'

### start main calculation
spark = SparkSession.builder \
  .master("local") \
  .appName("job_etl") \
  .getOrCreate()

### Read csv data do datatype and default conversions and write data into parque
# read data into data frame
dfS = spark.read.csv(raw_file, header = True)
# Convert source DF to target DF
dfT = wlConvert.wl_convert(dfS)
# write data in parque format into dir with one partition
dfT.repartition(1).write.mode('overwrite').parquet(curated_dir)

# show 5 rows of data - sanity check
dfT.filter("Best3SquatKg IS NOT NULL").show(5);
# show transformed data describe statistic:
dfT.describe().show()
# check other data statistic:
Squat1Kg_quantiles = dfT.approxQuantile("Squat1Kg", [0.25, 0.5, 0.75], 0.1)
print("Squat1Kg_quantiles: ", Squat1Kg_quantiles)
# show orginal csv file data describe statistic:
dfS.describe('Name','Squat1Kg','Squat2Kg','Squat3Kg','Best3SquatKg','Place','Country','MeetCountry','Best3DeadliftKg').show()
