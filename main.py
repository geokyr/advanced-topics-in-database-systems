import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from queries.query1 import run_query_1
from queries.query2 import run_query_2
from queries.query3 import run_query_3_df, run_query_3_rdd
from queries.query4 import run_query_4
from queries.query5 import run_query_5
from queries.printer import print_dataframe

spark = SparkSession.builder.appName("project").getOrCreate()
hdfs_path = "hdfs://master:9000/user/user/"

query_number = int(sys.argv[1])
num_workers = int(sys.argv[2])

if query_number not in [1, 2, 3, 4, 5]:
    print("Wrong query number")
    exit(1)

if query_number == 3:
    api_type = sys.argv[3]
    if api_type not in ["df", "rdd"]:
        print("Wrong API type")
        exit(1)

# create the two dataframes from the parquet files and csv file in HDFS
df_taxi_trips = spark.read.parquet(hdfs_path + "data/*.parquet")
df_zone_lookups = spark.read.csv(hdfs_path + "data/taxi+_zone_lookup.csv", header=True)

# create the two RDDs from the dataframes
rdd_taxi_trips = df_taxi_trips.rdd
rdd_zone_lookups = df_zone_lookups.rdd

# filter the data to keep the trips between 2022-01-01 and 2022-06-30
df_taxi_trips = df_taxi_trips.filter((col("tpep_pickup_datetime") >= "2022-01-01") & (col("tpep_pickup_datetime") < "2022-07-01"))

# run the respective query based on the query number
if (query_number == 1):
    output = "outputs/query_1_1.txt" if num_workers == 1 else "outputs/query_1_2.txt"

    with open(output, "w") as file:

        print(f"Starting Query 1 with {num_workers} workers...", file=file)

        start = time.time()
        query_1 = run_query_1(df_taxi_trips, df_zone_lookups)
        end = time.time()

        print(f"Time elapsed for Query 1: {end-start}", file=file)

        print_dataframe([query_1], file)

    hdfs_output = "outputs/query_1_1.csv" if num_workers == 1 else "outputs/query_1_2.csv"
    df = spark.createDataFrame([query_1])
    df.write.csv(hdfs_path + hdfs_output, mode="overwrite", header=True)

if(query_number == 2):
    output = "outputs/query_2_1.txt" if num_workers == 1 else "outputs/query_2_2.txt"

    with open(output, "w") as file:

        print(f"Starting Query 2 with {num_workers} workers...", file=file)

        start = time.time()
        query_2 = run_query_2(df_taxi_trips)
        end = time.time()

        print(f"Time elapsed for Query 2: {end-start}", file=file)
        
        print_dataframe(query_2, file)

    hdfs_output = "outputs/query_2_1.csv" if num_workers == 1 else "outputs/query_2_2.csv"
    df = spark.createDataFrame(query_2)
    df.write.csv(hdfs_path + hdfs_output, mode="overwrite", header=True)

if(query_number == 3):
    output = f"outputs/query_3_1_{api_type}.txt" if num_workers == 1 else f"outputs/query_3_2_{api_type}.txt"

    with open(output, "w") as file:

        print(f"Starting Query 3 with {num_workers} workers, using {'DataFrame/SQL' if api_type=='df' else 'RDD'} API...", file=file)

        function = run_query_3_df if api_type == "df" else run_query_3_rdd
        operand_1, operand_2 = (df_taxi_trips, df_zone_lookups) if api_type == "df" else (rdd_taxi_trips, rdd_zone_lookups)

        start = time.time()
        query_3 = function(operand_1, operand_2)
        end = time.time()

        print(f"Time elapsed for Query 3: {end-start}", file=file)
        
        print_dataframe(query_3, file)

    hdfs_output = f"outputs/query_3_1_{api_type}.csv" if num_workers == 1 else f"outputs/query_3_2_{api_type}.csv"
    df = spark.createDataFrame(query_3)
    df.write.csv(hdfs_path + hdfs_output, mode="overwrite", header=True)

if(query_number == 4):
    output = "outputs/query_4_1.txt" if num_workers == 1 else "outputs/query_4_2.txt"

    with open(output, "w") as file:

        print(f"Starting Query 4 with {num_workers} workers...", file=file)

        start = time.time()
        query_4 = run_query_4(df_taxi_trips)
        end = time.time()

        print(f"Time elapsed for Query 4: {end-start}", file=file)
        
        print_dataframe(query_4, file)

    hdfs_output = "outputs/query_4_1.csv" if num_workers == 1 else "outputs/query_4_2.csv"
    df = spark.createDataFrame(query_4)
    df.write.csv(hdfs_path + hdfs_output, mode="overwrite", header=True)

if(query_number == 5):
    output = "outputs/query_5_1.txt" if num_workers == 1 else "outputs/query_5_2.txt"

    with open(output, "w") as file:

        print(f"Starting Query 5 with {num_workers} workers...", file=file)

        start = time.time()
        query_5 = run_query_5(df_taxi_trips)
        end = time.time()

        print(f"Time elapsed for Query 5: {end-start}", file=file)
        
        print_dataframe(query_5, file)

    hdfs_output = "outputs/query_5_1.csv" if num_workers == 1 else "outputs/query_5_2.csv"
    df = spark.createDataFrame(query_5)
    df.write.csv(hdfs_path + hdfs_output, mode="overwrite", header=True)