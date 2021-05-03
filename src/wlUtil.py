import os, glob, shutil


def writeDFtoCSVfile(source_df, csv_file):
	# set temp dir name
	temp_csv_dir = csv_file + "_tmpdir"
	# save DF as csv
	try:
		source_df.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite') \
			.option("header", "true").save(temp_csv_dir)
	except:
		raise Exception("Error converting DF: ", source_df, " to CSV using option header, mode overwrite dir: " , temp_csv_dir)
	# to modify output to single csv file and remove output dir
	try:
		for source_file in glob.glob(temp_csv_dir+"/*.csv"):
			os.rename(source_file, csv_file)
	except:
		raise Exception("Error renaming file from: ", source_file, " to: " , csv_file)
	# remove temp dir
	try:
		shutil.rmtree(temp_csv_dir)
	except:
		raise Exception("Error removing temp dir: ", temp_csv_dir)
