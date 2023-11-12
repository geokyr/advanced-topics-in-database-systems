# advanced-topics-in-database-systems

Semester Project for the [Advanced Topics in Database Systems](https://www.ece.ntua.gr/en/undergraduate/courses/3189) course, during the 9th semester of the School of Electrical and Computer Engineering at the National Technical University of Athens.

## Contributors

* [Georgios Kyriakopoulos](https://github.com/geokyr)
* [Serafeim Tzelepis](https://github.com/sertze)

## Project Description

The project is about processing big data using the [Apache Spark](https://spark.apache.org/) platform. The data is a collection of trip records from the New York City Taxi and Limousine Commission (TLC). The data (per month) is available at [NYC Taxi and Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page.). The data is in CSV format and contains information about the pickup and dropoff locations, the pickup and dropoff times, the number of passengers, the trip distance, the payment type, the fare amount, the tip amount, the tolls amount and others.

We used the Apache Spark platform and HDFS to store the data. We also used both the DataFrame/SQL API and the RDD API of Spark. All the code was written in Python, utilizing the PySpark library.

## Setup

Setup instructions are available on the project report and the guides under the `sources` directory. It mainly involves setting up a Hadoop multi-node cluster, installing Apache Spark and setting up the environment.

We used [~okeanos](https://astakos.okeanos-knossos.grnet.gr/ui/landing) to set up the cluster, which consists of one master and one slave node. The master node is used to run the Spark master and the HDFS namenode, while also being used as a Spark worker and HDFS datanode. The slave node is used to run the Spark worker and the HDFS datanode.

## HDFS

We used the HDFS file system to store the data. We first uploaded the data folder on our HDFS and we later used the HDFS to store our query results. The first part was done through the terminal with commands like:
```
hdfs dfs -mkdir /data
hdfs dfs -put data /data
```

The second part was done using PySpark, as shown in the `main.py` file.

## Running the Project

After setting up the cluster, we can run the project. Make sure that you have run the following commands on the master node to have the HDFS and Spark runnning:
```
start-dfs.sh
start-all.sh
```

This creates 2 workers, one on the master node and one on the slave node. We can suspend the master worker to run queries with only one worker by running the following command on the master node:
```
./<spark_directory>/sbin/stop-worker.sh
```

We can then resume the master worker by running the following command on the master node:
```
start-all.sh
```

There is a `main.py` file that sets up the Spark Session, reads the data from the HDFS, creates the DataFrames and RDDs and then runs a query. The results are then output on a local .txt file together with the time to run the query and the query result is also stored in the HDFS.

To run a query, we can use the following command:
```
spark-submit main.py <query_number> <worker_number> <api_type>
```

Here, `<query_number>` is the number of the query we want to run (valid values are 1, 2, 3, 4, 5), `<worker_number>` is the number of workers (valid values are 1, 2) and `<api_type>` is the API we want to use (only applicable for `<query_number>` equal to 3 and valid values are `df` for DataFrame/SQL API and `rdd` for RDD API).

## Monitoring

The Apache Spark Master UI is available at `http://<master_ip>:8080/`. The Hadoop Overview UI is available at `http://<master_ip>:9870/`.

## Report

The query results are available in the form of tables on the project report. The report also contains the run time of each query for 1 and 2 workers and some analysis of the results.
