# Calculate functions
#
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, LongType, DoubleType
from pyspark.sql import functions as F


def calcPercBestSquat_Formula1(spark, dfT):
	### Compute the percentage with which weighlifters achieve their best squad result in their first, second and third attempt each.
	# Approach 1 - with data quality checking and logic inside calculation:
	countBestSquat1Kg = dfT.filter("Squat1Kg IS NOT NULL AND Squat1Kg > 0 AND Squat1Kg >= coalesce(Squat2Kg,0) AND Squat1Kg >= coalesce(Squat3Kg,0)").count();
	countBestSquat2Kg = dfT.filter("Squat2Kg IS NOT NULL AND Squat2Kg > 0 AND Squat2Kg > coalesce(Squat1Kg,0) AND Squat2Kg >= coalesce(Squat3Kg,0)").count();
	countBestSquat3Kg = dfT.filter("Squat3Kg IS NOT NULL AND Squat3Kg > 0 AND Squat3Kg > coalesce(Squat1Kg,0) AND Squat3Kg > coalesce(Squat2Kg,0)").count();
	countBest3SquatKg = countBestSquat1Kg + countBestSquat2Kg + countBestSquat3Kg
	percBestSquat1Kg = round(countBestSquat1Kg * 100.0 / countBest3SquatKg, 2)
	percBestSquat2Kg = round(countBestSquat2Kg * 100.0 / countBest3SquatKg, 2)
	percBestSquat3Kg = round(countBestSquat3Kg * 100.0 / countBest3SquatKg, 2)
	#print(countBest3SquatKg, 100, countBestSquat1Kg, percBestSquat1Kg, countBestSquat2Kg, percBestSquat2Kg, countBestSquat3Kg, percBestSquat3Kg)
	# def schema
	percBestSquatSchema = StructType([
		StructField("countBest3SquatKg", LongType(), True),
		StructField("percBestSquat1Kg", DoubleType(), True),
		StructField("percBestSquat2Kg", DoubleType(), True),
		StructField("percBestSquat3Kg", DoubleType(), True)
	])
	percBestSquat = [(countBest3SquatKg,percBestSquat1Kg,percBestSquat2Kg,percBestSquat3Kg)]
	return spark.createDataFrame(data=percBestSquat, schema=percBestSquatSchema)

def calcPercBestSquat_Formula2(spark, dfT):
	### Compute the percentage with which weighlifters achieve their best squad result in their first, second and third attempt each.
	# Approach 2 is more susceptible to data quality issues or require more thorough data cleaning
	countBest3SquatKg = dfT.filter("(Best3SquatKg IS NOT NULL AND Best3SquatKg > 0 AND (coalesce(Squat1Kg,0) = Best3SquatKg OR coalesce(Squat2Kg,0) = Best3SquatKg OR coalesce(Squat3Kg,0) = Best3SquatKg))").count()
	countBestSquat1Kg = dfT.filter("Best3SquatKg IS NOT NULL AND Best3SquatKg > 0 AND (coalesce(Squat1Kg,0) = Best3SquatKg)").count()
	countBestSquat2Kg = dfT.filter("Best3SquatKg IS NOT NULL AND Best3SquatKg > 0 AND (coalesce(Squat2Kg,0) = Best3SquatKg)").count()
	countBestSquat3Kg = dfT.filter("Best3SquatKg IS NOT NULL AND Best3SquatKg > 0 AND (coalesce(Squat3Kg,0) = Best3SquatKg)").count()
	percBestSquat1Kg = round(countBestSquat1Kg * 100.0 / countBest3SquatKg, 2)
	percBestSquat2Kg = round(countBestSquat2Kg * 100.0 / countBest3SquatKg, 2)
	percBestSquat3Kg = round(countBestSquat3Kg * 100.0 / countBest3SquatKg, 2)
	#print(countBest3SquatKg, 100, countBestSquat1Kg, percBestSquat1Kg, countBestSquat2Kg, percBestSquat2Kg, countBestSquat3Kg, percBestSquat3Kg)
	# def schema
	percBestSquatSchema = StructType([
		StructField("countBest3SquatKg", LongType(), True),
		StructField("percBestSquat1Kg", DoubleType(), True),
		StructField("percBestSquat2Kg", DoubleType(), True),
		StructField("percBestSquat3Kg", DoubleType(), True)
	])
	percBestSquat = [(countBest3SquatKg,percBestSquat1Kg,percBestSquat2Kg,percBestSquat3Kg)]
	return spark.createDataFrame(data=percBestSquat, schema=percBestSquatSchema)

def calcPercFirstBestSquat_Formula1(spark, dfT):
	### Compute the same as calcPercBestSquat, but only considering participants placing first in their competition.
	# Approach 1 - with data quality checking and logic inside calculation. Assumption is the column Place has correct value for winner of competition.
	countFirstBestSquat1Kg = dfT.filter("Place = 1 AND Squat1Kg IS NOT NULL AND Squat1Kg > 0 AND Squat1Kg >= coalesce(Squat2Kg,0) AND Squat1Kg >= coalesce(Squat3Kg,0)").count();
	countFirstBestSquat2Kg = dfT.filter("Place = 1 AND Squat2Kg IS NOT NULL AND Squat2Kg > 0 AND Squat2Kg > coalesce(Squat1Kg,0) AND Squat2Kg >= coalesce(Squat3Kg,0)").count();
	countFirstBestSquat3Kg = dfT.filter("Place = 1 AND Squat3Kg IS NOT NULL AND Squat3Kg > 0 AND Squat3Kg > coalesce(Squat1Kg,0) AND Squat3Kg > coalesce(Squat2Kg,0)").count();
	countFirstBest3SquatKg = countFirstBestSquat1Kg + countFirstBestSquat2Kg + countFirstBestSquat3Kg
	percFirstBestSquat1Kg = round(countFirstBestSquat1Kg * 100 / countFirstBest3SquatKg, 2)
	percFirstBestSquat2Kg = round(countFirstBestSquat2Kg * 100 / countFirstBest3SquatKg, 2)
	percFirstBestSquat3Kg = round(countFirstBestSquat3Kg * 100 / countFirstBest3SquatKg, 2)
	#print(countFirstBest3SquatKg, 100, countFirstBestSquat1Kg, percFirstBestSquat1Kg, countFirstBestSquat2Kg, percFirstBestSquat2Kg, countFirstBestSquat3Kg, percFirstBestSquat3Kg)
	percFirstBestSquatSchema = StructType([
		StructField("countFirstBest3SquatKg", LongType(), True),
		StructField("percFirstBestSquat1Kg", DoubleType(), True),
		StructField("percFirstBestSquat2Kg", DoubleType(), True),
		StructField("percFirstBestSquat3Kg", DoubleType(), True)
	])
	percFirstBestSquat = [(countFirstBest3SquatKg,percFirstBestSquat1Kg,percFirstBestSquat2Kg,percFirstBestSquat3Kg)]
	return spark.createDataFrame(data=percFirstBestSquat, schema=percFirstBestSquatSchema)

def calcPercFirstBestSquat_Formula2(spark, dfT):
	### Compute the same as calcPercBestSquat, but only considering participants placing first in their competition.
    # Approach 2 is more susceptible to data quality issues or require more thorough data cleaning
	countFirstBest3SquatKg = dfT.filter("(Place = 1 AND Best3SquatKg IS NOT NULL AND Best3SquatKg > 0 AND (coalesce(Squat1Kg,0) = Best3SquatKg OR coalesce(Squat2Kg,0) = Best3SquatKg OR coalesce(Squat3Kg,0) = Best3SquatKg))").count()
	countFirstBestSquat1Kg = dfT.filter("Place = 1 AND Best3SquatKg IS NOT NULL AND Best3SquatKg > 0 AND (coalesce(Squat1Kg,0) = Best3SquatKg)").count()
	countFirstBestSquat2Kg = dfT.filter("Place = 1 AND Best3SquatKg IS NOT NULL AND Best3SquatKg > 0 AND (coalesce(Squat2Kg,0) = Best3SquatKg)").count()
	countFirstBestSquat3Kg = dfT.filter("Place = 1 AND Best3SquatKg IS NOT NULL AND Best3SquatKg > 0 AND (coalesce(Squat3Kg,0) = Best3SquatKg)").count()
	percFirstBestSquat1Kg = round(countFirstBestSquat1Kg * 100.0 / countFirstBest3SquatKg, 2)
	percFirstBestSquat2Kg = round(countFirstBestSquat2Kg * 100.0 / countFirstBest3SquatKg, 2)
	percFirstBestSquat3Kg = round(countFirstBestSquat3Kg * 100.0 / countFirstBest3SquatKg, 2)
	#print(countFirstBest3SquatKg, 100, countFirstBestSquat1Kg, percFirstBestSquat1Kg, countFirstBestSquat2Kg, percFirstBestSquat2Kg, countFirstBestSquat3Kg, percFirstBestSquat3Kg)
	percFirstBestSquatSchema = StructType([
		StructField("countFirstBest3SquatKg", LongType(), True),
		StructField("percFirstBestSquat1Kg", DoubleType(), True),
		StructField("percFirstBestSquat2Kg", DoubleType(), True),
		StructField("percFirstBestSquat3Kg", DoubleType(), True)
	])
	percFirstBestSquat = [(countFirstBest3SquatKg,percFirstBestSquat1Kg,percFirstBestSquat2Kg,percFirstBestSquat3Kg)]
	return spark.createDataFrame(data=percFirstBestSquat, schema=percFirstBestSquatSchema)
	
def getNameRecordInDeadliftKg(dfT):	
	### For each country, determine the weighlifter who holds the record in deadlifting over the whole dataset.
	# add not null country column to dataset and if Country is null replace it with MeetCountry. 
	#   Assumption 1 - the non-international competition did not provide information about Country
	#   Assumption 2 - the Best3DeadliftKg is reliable enough for identifying recordholder 
	wlRecordInDeadliftKgDF = dfT.groupBy("Country").agg(F.max("Best3DeadliftKg").alias("Best3DeadliftKg"))
	return wlRecordInDeadliftKgDF.alias("wldf") \
			.join(dfT.alias("dfT"), (F.col("wldf.Country") == F.col("dfT.Country")) \
				& (F.col("wldf.Best3DeadliftKg") == F.col("dfT.Best3DeadliftKg")),"inner") \
			.select(F.col("wldf.Country"), F.col("wldf.Best3DeadliftKg"), F.col("dfT.Name")) \
			.distinct().sort("Country","Name")
