import csv
import json
import time
import os
from kafka import KafkaProducer

print("🔄 Starting Kafka Producer...")
print(f"📁 Files in /data directory: {os.listdir('/data')}")

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("📖 Opening CSV file...")
with open('/data/credit_card_transactions.csv', 'r') as file:
    reader = csv.DictReader(file)
    row_count = 0
    for row in reader:
        producer.send('credit-transactions', value=row)
        row_count += 1
        if row_count % 100 == 0:
            print(f"📤 Sent {row_count} records...")
        time.sleep(0.1)  # simulate streaming (adjust if needed)

producer.flush()
print(f"✅ All {row_count} records sent to Kafka topic 'credit-transactions'.")
