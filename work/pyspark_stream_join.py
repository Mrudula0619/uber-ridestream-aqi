from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# -----------------------------------------------------------------------------
# 1. Initialize PySpark Session
# -----------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("UberRideAQIStreamJoin") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "jdbc") \
    .config("spark.sql.catalog.local.uri", "jdbc:postgresql://postgres:5432/iceberg_catalog") \
    .config("spark.sql.catalog.local.jdbc.user", "iceberg") \
    .config("spark.sql.catalog.local.jdbc.password", "iceberg_password") \
    .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg_warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------------------------------------------------------
# 2. Ensure Database & Iceberg Target Table Exist
# -----------------------------------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS local.db")

spark.sql("""
CREATE TABLE IF NOT EXISTS local.db.uber_ride_aqi_joined (
    ride_id STRING,
    city STRING,
    ride_type STRING,
    fare_amount DOUBLE,
    ride_time TIMESTAMP,
    sensor_id STRING,
    aqi INT,
    pm2_5 DOUBLE,
    aqi_time TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ride_time))
""")

# -----------------------------------------------------------------------------
# 3. Define Schemas
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
# 4. Read Stream Topics
# -----------------------------------------------------------------------------
rides_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "uber-rides") \
    .option("startingOffsets", "earliest") \
    .load()

aqi_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:9092") \
    .option("subscribe", "aqi-readings") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON & apply watermarks for late-data handling
rides_df = rides_raw.select(
    from_json(col("value").cast("string"), ride_schema).alias("ride")
).select("ride.*") \
 .withColumn("ride_time", col("timestamp").cast("timestamp")) \
 .withWatermark("ride_time", "10 minutes")

aqi_df = aqi_raw.select(
    from_json(col("value").cast("string"), aqi_schema).alias("aqi_data")
).select("aqi_data.*") \
 .withColumn("aqi_time", col("timestamp").cast("timestamp")) \
 .withWatermark("aqi_time", "10 minutes")

# -----------------------------------------------------------------------------
# 5. Perform Stream-Stream Join
# -----------------------------------------------------------------------------
rides_aliased = rides_df.alias("ride")
aqi_aliased = aqi_df.alias("aqi_data")

joined_stream = rides_aliased.join(
    aqi_aliased,
    expr("""
        ride.city = aqi_data.city AND
        aqi_time >= ride_time - interval 5 minutes AND
        aqi_time <= ride_time + interval 5 minutes
    """),
    how="inner"
).select(
    col("ride.ride_id"),
    col("ride.city").alias("city"),
    col("ride.ride_type"),
    col("ride.fare_amount"),
    col("ride.ride_time"),
    col("aqi_data.sensor_id"),
    col("aqi_data.aqi"),
    col("aqi_data.pm2_5"),
    col("aqi_data.aqi_time")
)

# -----------------------------------------------------------------------------
# 6. Output Joined Events to Iceberg Table with Checkpointing
# -----------------------------------------------------------------------------
print("⚡ Starting Live Stream-Stream Join Query to Iceberg...")

checkpoint_dir = "/tmp/checkpoints/uber_ride_aqi_joined"

query = joined_stream.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("path", "local.db.uber_ride_aqi_joined") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

query.awaitTermination()