from kafka import KafkaConsumer
import json
import pandas as pd
import boto3
from io import BytesIO
from botocore.exceptions import ClientError

consumer = KafkaConsumer(
    'credit-transactions',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

s3 = boto3.client('s3', endpoint_url='http://minio:9000',
                  aws_access_key_id='minio',
                  aws_secret_access_key='minio123')

# Create bucket if it doesn't exist
bucket_name = 'transactions-data'
try:
    s3.head_bucket(Bucket=bucket_name)
    print(f"✅ Bucket '{bucket_name}' already exists")
except ClientError as e:
    if e.response['Error']['Code'] == '404':
        s3.create_bucket(Bucket=bucket_name)
        print(f"✅ Created bucket '{bucket_name}'")
    else:
        print(f"❌ Error checking bucket: {e}")

batch = []
batch_size = 1000
batch_count = 0

print("🔄 Starting to consume messages from Kafka...")

for message in consumer:
    batch.append(message.value)
    
    if len(batch) >= batch_size:
        df = pd.DataFrame(batch)
        out_buffer = BytesIO()
        df.to_parquet(out_buffer, index=False)
        out_buffer.seek(0)
        key = f'batch_{batch_count}.parquet'
        
        s3.put_object(Bucket=bucket_name, Key=key, Body=out_buffer.getvalue())
        print(f"✅ Saved {key} to MinIO ({len(batch)} records)")
        
        batch = []
        batch_count += 1
