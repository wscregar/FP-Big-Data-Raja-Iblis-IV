from kafka import KafkaProducer
import pandas as pd
import json
import time

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

try:
    # Load CSV
    df = pd.read_csv("data/credit_card_transactions.csv")
except Exception as e:
    print(f"❌ Gagal membaca CSV: {e}")
    exit(1)

# Kirim data secara batch
batch_size = 1000
try:
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size].to_dict(orient='records')
        for row in batch:
            producer.send("transaction-stream", value=row)
        print(f"📤 Kirim batch ke-{i // batch_size + 1}, total data: {i + len(batch)}")
        time.sleep(2)
except Exception as e:
    print(f"❌ Error saat mengirim ke Kafka: {e}")
finally:
    producer.flush()
    producer.close()
    print("✅ Kafka Producer selesai.")

