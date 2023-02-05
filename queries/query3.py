import math
from pyspark.sql.functions import col, floor, dayofyear, avg

# ------------------------------
# query 3 - find per 15 days the average trip distance and the average trip cost for the trips with a different drop off zone than the pick up zone

def run_query_3_df(df_taxi_trips, df_zone_lookups):

    return df_taxi_trips.join(df_zone_lookups, df_taxi_trips.DOLocationID == df_zone_lookups.LocationID, "left")\
        .withColumnRenamed("Zone", "DOLocationZone")\
        .drop("LocationID", "Borough", "service_zone")\
        .join(df_zone_lookups, df_taxi_trips.PULocationID == df_zone_lookups.LocationID, "left")\
        .withColumnRenamed("Zone", "PULocationZone")\
        .drop("LocationID", "Borough", "service_zone")\
        .filter(col("DOLocationZone") != col("PULocationZone"))\
        .withColumn("group", floor((dayofyear("tpep_pickup_datetime") - 1)/15))\
        .groupBy("group")\
        .agg(avg("trip_distance").alias("avg_trip_distance"),avg("total_amount").alias("avg_total_amount"))\
        .sort("group")\
        .collect()

def run_query_3_rdd(rdd_taxi_trips, rdd_zone_lookups):
    pass