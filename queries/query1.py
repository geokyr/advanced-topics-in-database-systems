from pyspark.sql.functions import month, col

# ------------------------------
# query 1 - find the trip with the biggest tip in March and drop off zone "Battery Park"

def run_query_1(df_taxi_trips, df_zone_lookups):
    return df_taxi_trips.filter(month(col("tpep_pickup_datetime")) == 3)\
        .join(df_zone_lookups, [df_taxi_trips.DOLocationID == df_zone_lookups.LocationID, df_zone_lookups.Zone == "Battery Park"])\
        .sort((col("tip_amount").desc()))\
        .drop("LocationID", "Borough", "Zone", "service_zone")\
        .first()