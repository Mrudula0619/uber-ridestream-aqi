from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# 1. Build Spark Session with Iceberg & Kafka configurations
spark = SparkSession.builder \
    .appName("RedpandaToIcebergStreaming") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
            "org.postgresql:postgresql:42.6.0") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog") \
    .config("spark.sql.catalog.local.uri", "jdbc:postgresql://iceberg-catalog-db:5432/iceberg_catalog") \
    .config("spark.sql.catalog.local.jdbc.user", "iceberg") \
    .config("spark.sql.catalog.local.jdbc.password", "iceberg_password") \
    .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg") \
    .config("spark.sql.defaultCatalog", "local") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schema matching your Iceberg ride_events table
json_schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("passenger_id", StringType(), True),
    StructField("aqi_level", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("event_timestamp", TimestampType(), True)
])

# 3. Read stream from Redpanda topic inside the Docker network
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda:19092") \
    .option("subscribe", "ride_events") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Parse JSON payload
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), json_schema).alias("data")) \
    .select("data.*")

# 5. Append stream to Iceberg table every 5 seconds
query = parsed_stream.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation", "/tmp/iceberg_checkpoints/ride_events") \
    .toTable("local.uber_stream.ride_events")

print("Streaming pipeline active from Redpanda to Iceberg...")
query.awaitTermination()
