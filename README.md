# Real-Time Ride & AQI Ingestion Pipeline

An end-to-end real-time streaming data pipeline processing urban ride events and environmental Air Quality Index (AQI) data using Redpanda, PySpark Structured Streaming, and Apache Iceberg.

## System Architecture
## Tech Stack

- **Message Broker:** Redpanda (Kafka API compliant)
- **Stream Processing:** PySpark Structured Streaming (Spark 3.5.0)
- **Storage Format:** Apache Iceberg (v1.5.0)
- **Catalog Database:** PostgreSQL 15 (JDBC Metastore)
- **Infrastructure:** Docker Compose

## Getting Started

### 1. Start Infrastructure Stack
### 2. Run Python Producer
### 3. Deploy PySpark Streaming Job
docker exec -it pyspark-streaming spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.postgresql:postgresql:42.6.0 \
  /home/jovyan/stream_redpanda_to_iceberg.py
### 4. Query Data in spark-sql
Inside `spark-sql`:
