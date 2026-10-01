from pyspark.sql import SparkSession

# Initialize PySpark with the same Iceberg PostgreSQL JDBC Catalog
spark = SparkSession.builder \
    .appName("QueryIcebergTables") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "jdbc") \
    .config("spark.sql.catalog.local.uri", "jdbc:postgresql://postgres:5432/iceberg_catalog") \
    .config("spark.sql.catalog.local.jdbc.user", "iceberg") \
    .config("spark.sql.catalog.local.jdbc.password", "iceberg_password") \
    .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg_warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n=== 🧊 1. Uber Ride Statistics (Iceberg Table) ===")
rides_df = spark.sql("""
    SELECT window_start, window_end, city, total_rides, round(avg_fare, 2) as avg_fare, round(avg_distance, 2) as avg_distance
    FROM local.uber_db.city_ride_stats
    ORDER BY window_start DESC, total_rides DESC
""")
rides_df.show(10, truncate=False)

print("\n=== 🌫️ 2. Air Quality Index Statistics (Iceberg Table) ===")
aqi_df = spark.sql("""
    SELECT window_start, window_end, city, round(avg_aqi, 1) as avg_aqi, round(avg_pm2_5, 1) as avg_pm2_5
    FROM local.uber_db.city_aqi_stats
    ORDER BY window_start DESC
""")
aqi_df.show(10, truncate=False)

print("\n=== 📸 3. Iceberg Table Snapshots History ===")
spark.sql("SELECT snapshot_id, committed_at, operation FROM local.uber_db.city_ride_stats.snapshots").show(5, truncate=False)

