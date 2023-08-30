# Spark Challenge for ETL and Analytics

Consist of two parts:
1) basic ETL job that transforms a CSV file into parquet format
2) second job that uses the parquet files to compute some basic analytics

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

```
Python (3.8), Apache Spark 3.1+ (Hadoop 3.0+)
Data from https://www.kaggle.com/open-powerlifting/powerlifting-database/download
To run tests pandas, pytest are needed
```

### Installing

Follow links:
* [Install Apache Spark on Windows 10 using WSL](https://kontext.tech/column/spark/311/apache-spark-243-installation-on-windows-10-using-windows-subsystem-for-linux)
* [Install Hadoop Single Cluster](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html)
* [Hadoop 3.2.2 source](https://hadoop.apache.org/release/3.2.2.html)
* [pandas](https://pandas.pydata.org/pandas-docs/stable/getting_started/install.html) or use: pip install pandas, pip install pyspark-pandas
* [pytest](https://docs.pytest.org/en/6.2.x/getting-started.html)

Example of the run:
```
pyspark < src/job_etl.py > log/job_etl.log 2>&1
pyspark < src/job_analytics.py > log/job_analytics.log 2>&1
```

## Running the tests

To run the automated tests for this system, pandas and pytest have to be installed and configured

### Break down into end to end tests

Tests are focusing on evaluate ETL transformation. Analytic calculations checks are focusing on calculation the same output using different formulas/algorithms that should get the same result. To check calculation requires output files/log inspection or add more automatic tests. Example of the test run:

```
pytest < tests/test_wlConvert.py > log/test_wlConvert.log 2>&1
```

## Deployment

Copy folders and directories to target system should work

## Built With

* [Poetry for pyspark example](https://github.com/MrPowers/angelou) - Intention to use Poetry as dependency management and wheel packaging

## Versioning

[SemVer](http://semver.org/) for versioning.

## Authors

* **Radovan Jablonovsky** - *Initial work* - [Spark](https://github.com/rjablonovsky/spark)

See also the list of [contributors](https://github.com/rjablonovsky/spark/contributors) who participated in this project.

## License

This project is licensed under the BSD License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to data analytic community, google, github

