from pyspark.sql.functions import col, month, dayofmonth, avg, row_number, desc
from pyspark.sql import Window

# ------------------------------
# query 5 - find top 5 days of each month where the trips had the highest average percentage of tip to fare amount ratio

def run_query_5(df_taxi_trips):    

    return df_taxi_trips.withColumn("month", (month("tpep_pickup_datetime")))\
        .withColumn("day_of_month", (dayofmonth("tpep_pickup_datetime")))\
        .withColumn("tip_percentage", (col("tip_amount") / col("fare_amount")) * 100)\
        .groupBy("month", "day_of_month")\
        .agg(avg("tip_percentage").alias("avg_tip_percentage"))\
        .withColumn("index", row_number().over(Window.partitionBy("month").orderBy(desc("avg_tip_percentage"))))\
        .filter(col("index") <= 5)\
        .drop("index")\
        .sort(col("month"))\
        .collect()
