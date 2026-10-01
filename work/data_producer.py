import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Connect to Redpanda on internal container address 'redpanda:9092'
producer = KafkaProducer(
    bootstrap_servers=['redpanda:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

CITIES = ['New York', 'San Francisco', 'Chicago', 'Austin', 'Seattle']
RIDE_TYPES = ['UberX', 'UberXL', 'Black', 'Comfort']

def generate_uber_ride():
    ride_id = f"ride_{random.randint(100000, 999999)}"
    city = random.choice(CITIES)
    return {
        "event_type": "uber_ride",
        "ride_id": ride_id,
        "city": city,
        "ride_type": random.choice(RIDE_TYPES),
        "distance_miles": round(random.uniform(1.0, 25.0), 2),
        "fare_amount": round(random.uniform(8.0, 120.0), 2),
        "passenger_count": random.randint(1, 4),
        "timestamp": datetime.utcnow().isoformat()
    }, city

def generate_aqi_reading():
    city = random.choice(CITIES)
    return {
        "event_type": "aqi_sensor",
        "sensor_id": f"sensor_{city.lower().replace(' ', '_')}_{random.randint(1, 5)}",
        "city": city,
        "aqi": random.randint(20, 180),
        "pm2_5": round(random.uniform(5.0, 75.0), 1),
        "pm10": round(random.uniform(10.0, 120.0), 1),
        "timestamp": datetime.utcnow().isoformat()
    }, city

print("🚀 Starting Stream Producer inside Docker network... Press Ctrl+C to stop.\n")

try:
    while True:
        # Stream 3 Uber Rides
        for _ in range(3):
            ride_data, city_key = generate_uber_ride()
            producer.send('uber-rides', key=city_key, value=ride_data)
            print(f"🚕 Sent Ride [{ride_data['ride_id']}] for {city_key}: ${ride_data['fare_amount']}")

        # Stream 1 AQI Reading
        aqi_data, city_key = generate_aqi_reading()
        producer.send('aqi-readings', key=city_key, value=aqi_data)
        print(f"🌫️  Sent AQI [{aqi_data['sensor_id']}] for {city_key}: AQI {aqi_data['aqi']}\n")

        producer.flush()
        time.sleep(2)  # Emit batches every 2 seconds

except KeyboardInterrupt:
    print("\n🛑 Producer stopped.")
    producer.close()
