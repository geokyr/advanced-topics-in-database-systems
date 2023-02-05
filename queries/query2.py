from pyspark.sql.functions import month, col, max

# query 2 - find for every month the trip with the highest tolls amount
def run_query_2(df_taxi_trips):
    return df_taxi_trips.filter(col("tolls_amount") != 0)\
        .groupBy(month(col("tpep_pickup_datetime")))\
        .agg(max("tolls_amount")\
        .alias("max_tolls_amount"))\
        .join(df_taxi_trips, [month(col("tpep_pickup_datetime")) == col("month(tpep_pickup_datetime)"), col("tolls_amount") == col("max_tolls_amount")])\
        .sort("month(tpep_pickup_datetime)")\
        .drop("month(tpep_pickup_datetime)", "max_tolls_amount")\
        .collect()
