import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window, avg, count, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# -----------------------------------------------------------------------------
# 1. Initialize PySpark Session with Apache Iceberg + Postgres Catalog Config
# -----------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("UberRideStreamAQIAnalytics") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "jdbc") \
    .config("spark.sql.catalog.local.uri", "jdbc:postgresql://postgres:5432/iceberg_catalog") \
    .config("spark.sql.catalog.local.jdbc.user", "iceberg") \
    .config("spark.sql.catalog.local.jdbc.password", "iceberg_password") \
    .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg_warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("⚡ PySpark Session initialized with Apache Iceberg Catalog.")

# -----------------------------------------------------------------------------
# 2. Define Event Schemas
# -----------------------------------------------------------------------------
ride_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("ride_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("ride_type", StringType(), True),
    StructField("distance_miles", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

aqi_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("aqi", IntegerType(), True),
    StructField("pm2_5", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# -----------------------------------------------------------------------------
# 3. Create Iceberg Database and Tables if they don't exist
# -----------------------------------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS local.uber_db")

spark.sql("""
CREATE TABLE IF NOT EXISTS local.uber_db.city_ride_stats (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    city STRING,
    total_rides BIGINT,
    avg_fare DOUBLE,
    avg_distance DOUBLE
) USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS local.uber_db.city_aqi_stats (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    city STRING,
    avg_aqi DOUBLE,
    avg_pm2_5 DOUBLE
) USING iceberg
""")

print("🧊 Iceberg database and tables prepared.")

# -----------------------------------------------------------------------------
# 4. Read Redpanda Streams
# -----------------------------------------------------------------------------
raw_rides_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "uber-rides") \
    .option("startingOffsets", "earliest") \
    .load()

raw_aqi_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "aqi-readings") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON strings and convert timestamps
parsed_rides = raw_rides_df.select(
    from_json(col("value").cast("string"), ride_schema).alias("data")
).select("data.*").withColumn("event_time", col("timestamp").cast("timestamp"))

parsed_aqi = raw_aqi_df.select(
    from_json(col("value").cast("string"), aqi_schema).alias("data")
).select("data.*").withColumn("event_time", col("timestamp").cast("timestamp"))

# -----------------------------------------------------------------------------
# 5. Windowed Aggregations (5-minute Tumbling Windows with Watermarking)
# -----------------------------------------------------------------------------
ride_metrics = parsed_rides \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("city")
    ).agg(
        count("ride_id").alias("total_rides"),
        avg("fare_amount").alias("avg_fare"),
        avg("distance_miles").alias("avg_distance")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("city"),
        col("total_rides"),
        col("avg_fare"),
        col("avg_distance")
    )

aqi_metrics = parsed_aqi \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("city")
    ).agg(
        avg("aqi").alias("avg_aqi"),
        avg("pm2_5").alias("avg_pm2_5")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("city"),
        col("avg_aqi"),
        col("avg_pm2_5")
    )

# -----------------------------------------------------------------------------
# 6. Stream Writes to Apache Iceberg Tables
# -----------------------------------------------------------------------------
def write_rides_to_iceberg(batch_df, batch_id):
    batch_df.write \
        .format("iceberg") \
        .mode("append") \
        .save("local.uber_db.city_ride_stats")

def write_aqi_to_iceberg(batch_df, batch_id):
    batch_df.write \
        .format("iceberg") \
        .mode("append") \
        .save("local.uber_db.city_aqi_stats")

print("🌊 Starting Streaming Queries to Iceberg...")

query_rides = ride_metrics.writeStream \
    .foreachBatch(write_rides_to_iceberg) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/iceberg_warehouse/checkpoints/rides") \
    .start()

query_aqi = aqi_metrics.writeStream \
    .foreachBatch(write_aqi_to_iceberg) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/iceberg_warehouse/checkpoints/aqi") \
    .start()

spark.streams.awaitAnyTermination()
