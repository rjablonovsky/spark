# Test data from CSV data conversiomn to datatype and defaults
# require spark 3.0+, python (3.6-3.8), pytest 
# run example: pytest < tests/test_wlConvert.py > log/test_wlConvert.log 2>&1
# 

from pyspark.sql.types import ArrayType, StructField, StructType, StringType, LongType, DoubleType
from pyspark.sql import functions as F
# import file form src dir
import os, sys
from os.path import dirname, join, abspath
sys.path.insert(0, abspath(join(dirname(__file__), '..')))
from src import wlConvert
# import pandas
import pandas as pd


def test_wl_convert(sql_context):		

    source_df = sql_context.createDataFrame([
		("Abbie Murphy","80","92.5","105","105","4",None,"Australia","130"),
		("Abbie Tuong","100","110","120","120","2",None,"Australia","145"),
		("Josie Stanford","150","157.5","165","165","5","New Zealand","Australia","182.5")],
		["Name","Squat1Kg","Squat2Kg","Squat3Kg","Best3SquatKg","Place","Country","MeetCountry","Best3DeadliftKg"]
	)
    actual_df = wlConvert.wl_convert(source_df)

    expected_df = sql_context.createDataFrame([
        ("Abbie Murphy", 80.0, 92.5, 105.0, 105.0, 4, "Australia", 130.0),
		("Abbie Tuong", 100.0, 110.0, 120.0, 120.0, 2, "Australia", 145.0),
		("Josie Stanford", 150.0, 157.5, 165.0, 165.0, 5, "New Zealand", 182.5)],
        ["Name","Squat1Kg","Squat2Kg","Squat3Kg","Best3SquatKg","Place","Country","Best3DeadliftKg"]
    )
    actual_df = get_sorted_data_frame(actual_df.toPandas(), ["Name","Squat1Kg","Squat2Kg","Squat3Kg","Best3SquatKg","Place","Country","Best3DeadliftKg"],)
    expected_df = get_sorted_data_frame(expected_df.toPandas(), ["Name","Squat1Kg","Squat2Kg","Squat3Kg","Best3SquatKg","Place","Country","Best3DeadliftKg"],)

    # Equality assertion
    pd.testing.assert_frame_equal(expected_df, actual_df, check_like=True,)


def get_sorted_data_frame(data_frame, columns_list):
    return data_frame.sort_values(columns_list).reset_index(drop=True)

