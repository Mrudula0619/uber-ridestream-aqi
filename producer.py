import json
import time
import random
from kafka import KafkaProducer

# Connect to Redpanda on localhost:19092
producer = KafkaProducer(
    bootstrap_servers=['localhost:19092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Starting Redpanda producer... Press Ctrl+C to stop.")

while True:
    event = {
        "ride_id": f"ride_{random.randint(1000, 9999)}",
        "driver_id": f"driver_{random.randint(100, 999)}",
        "passenger_id": f"pass_{random.randint(100, 999)}",
        "aqi_level": random.randint(15, 180),
        "fare_amount": round(random.uniform(10.0, 85.0), 2),
        "event_timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    producer.send("ride_events", event)
    print(f"Sent event: {event}")
    time.sleep(1)
