# app/kafka_consumer.py
from kafka import KafkaConsumer
from minio import Minio
import pandas as pd
import json
from io import BytesIO
from datetime import datetime
import signal
import sys

# Setup Kafka Consumer
consumer = KafkaConsumer(
    'transaction-stream',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='fraud-consumer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Setup MinIO Client
client = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_name = "batches"
if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)

# Global batch variables
batch = []
batch_size = 1000

def save_batch_to_minio(batch):
    now = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"batch_{now}.csv"
    df = pd.DataFrame(batch)

    csv_bytes = BytesIO()
    df.to_csv(csv_bytes, index=False, encoding='utf-8')
    csv_bytes.seek(0)

    try:
        client.put_object(
            bucket_name,
            filename,
            csv_bytes,
            length=csv_bytes.getbuffer().nbytes,
            content_type="application/csv"
        )
        print(f"🟢 Saved {filename} ({len(batch)} rows) to MinIO")
    except Exception as e:
        print(f"❌ Gagal upload ke MinIO: {e}")

# Graceful shutdown
def shutdown_handler(sig, frame):
    print("\n🔴 Shutdown signal received.")
    if batch:
        print(f"💾 Menyimpan batch tersisa ({len(batch)} rows)...")
        save_batch_to_minio(batch)
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

print("✅ Kafka Consumer started...")

# Main loop
for message in consumer:
    try:
        data = message.value
        batch.append(data)

        if len(batch) >= batch_size:
            save_batch_to_minio(batch)
            batch.clear()
    except Exception as e:
        print(f"❌ Error dalam loop consumer: {e}")
