# Convert only needed columns proper datatype and possible save dataframe to Parque format
# require spark 3.0+ and python (3.6-3.8)
# 

from pyspark.sql.types import ArrayType, StructField, StructType, StringType, LongType, DoubleType
from pyspark.sql import functions as F

def wl_convert(dfS):
	return dfS.select(dfS['Name'].cast(StringType()).alias('Name'), \
					  dfS['Squat1Kg'].cast(DoubleType()).alias('Squat1Kg'), \
					  dfS['Squat2Kg'].cast(DoubleType()).alias('Squat2Kg'), \
					  dfS['Squat3Kg'].cast(DoubleType()).alias('Squat3Kg'), \
					  dfS['Best3SquatKg'].cast(DoubleType()).alias('Best3SquatKg'), \
					  dfS['Place'].cast(LongType()).alias('Place'), \
					  # Assumption 1 - the non-international competition did not provide information about Country
					  F.coalesce(dfS['Country'], dfS['MeetCountry']).cast(StringType()).alias('Country'), \
					  F.coalesce(dfS['Best3DeadliftKg'], F.lit('0')).cast(DoubleType()).alias('Best3DeadliftKg') \
			)
