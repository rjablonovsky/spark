# Paceline Challenge

Paceline challenge for ETL and Analytics:
This is Paceline's take home challenge for data platform engineer applicants. The challenge is supposed to be completed in 2-4 hours, but you can take up to 7 days
to return the challenge. The challenge consists of two parts:
A basic ETL job that transforms a CSV file into parquet format
A second job that uses the parquet files to compute some basic analytics

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

What things you need to install the software and how to install them

```
Python (3.6 to 3.8), Apache Spark 3.1+ (Hadoop 3.0+)
Data from https://www.kaggle.com/open-powerlifting/powerlifting-database/download
```

### Installing

Follow links:
* [Install Apache Spark on Windows 10 using WSL](https://kontext.tech/column/spark/311/apache-spark-243-installation-on-windows-10-using-windows-subsystem-for-linux)
* [Install Hadoop Single Cluster](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html)
* [Hadoop 3.2.2 source](https://mirror.cogentco.com/pub/apache/hadoop/common/hadoop-3.2.2/)

Example of the run:
```
pyspark < src/convert_csv_to_parquet.py
```

## Running the tests

Explain how to run the automated tests for this system

### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment

Copy to target system should work

## Built With

* [Angelou](https://pypi.org/project/angelou/) - Intention to use Angelou

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/rjablonovsky/paceline/tags). 

## Authors

* **Radovan Jablonovsky** - *Initial work* - [Paceline](https://github.com/rjablonovsky/paceline)

See also the list of [contributors](https://github.com/rjablonovsky/paceline/contributors) who participated in this project.

## License

This project is licensed under the BSD License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to data analytic community, google, github

