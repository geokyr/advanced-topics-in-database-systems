from pyspark.sql.functions import col, floor, dayofyear, avg

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
    # Filter taxi trips RDD to only keep necessary columns
    taxi_trips_filtered = rdd_taxi_trips.map(lambda x: (x[7], x[8], x[1], x[4], x[16]))
    
    # Join with zone lookups RDD to get pickup location zone
    joined_1 = taxi_trips_filtered.map(lambda x: (x[0], x)).join(rdd_zone_lookups.map(lambda x: (x[0], x[2])))
    pickup_location_zone = joined_1.map(lambda x: (x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3], x[1][0][4], x[1][1]))
    
    # Join with zone lookups RDD again to get dropoff location zone
    joined_2 = pickup_location_zone.map(lambda x: (x[1], x)).join(rdd_zone_lookups.map(lambda x: (x[0], x[2])))
    all_data = joined_2.map(lambda x: (x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3], x[1][0][4], x[1][0][5], x[1][1]))
    
    # Filter data where pickup location zone is not equal to dropoff location zone
    filtered_data = all_data.filter(lambda x: x[5] != x[6])
    
    # Create the "group" column based on the tpep_pickup_datetime
    grouped_data = filtered_data.map(lambda x: (int((x[2].timetuple().tm_yday - 1) / 15), x[2], x[3], x[4]))
    
    # Group by "group" and calculate average trip_distance and average total_amount using RDD API
    avg_trip_distance_and_total_amount = grouped_data.map(lambda x: (x[0], (x[2], x[3])))\
        .groupByKey()\
        .map(lambda x: (int(x[0]), (sum([y[0] for y in x[1]]) / len(x[1]), sum([y[1] for y in x[1]]) / len(x[1]))))
    
    # Sort by group and collect the data
    return avg_trip_distance_and_total_amount.sortByKey().collect()
