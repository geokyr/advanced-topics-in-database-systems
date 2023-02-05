from pyspark.sql import Window
from pyspark.sql.functions import col, dayofweek, hour, sum, row_number, desc

# query 4 - find for every day of the week the 3 hours with the most passengers, using all the data for all months
def run_query_4(df_taxi_trips):    
    return df_taxi_trips.withColumn("day_of_week", (dayofweek("tpep_pickup_datetime")))\
        .withColumn("hour_of_day", (hour("tpep_pickup_datetime")))\
        .groupBy("day_of_week", "hour_of_day")\
        .agg(sum("passenger_count").alias("sum_passenger_count"))\
        .withColumn("index", row_number().over(Window.partitionBy("day_of_week").orderBy(desc("sum_passenger_count"))))\
        .filter(col("index") <= 3)\
        .drop("index")\
        .sort(col("day_of_week"))\
        .collect()
