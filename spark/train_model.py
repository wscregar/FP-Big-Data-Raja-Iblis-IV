from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
import os

print("🚀 Starting Spark ML Training...")

# Setup SparkSession with minimal configuration
spark = SparkSession.builder \
    .appName("CreditCardFraudDetection") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

print("✅ Spark session created successfully")

# For now, let's create some dummy data to test the pipeline
print("📊 Creating sample training data...")

# Create sample data that matches the structure expected
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import Row
import random

# Define schema similar to credit card transaction data
schema = StructType([
    StructField("cc_num", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amt", DoubleType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", IntegerType(), True),
    StructField("lat", DoubleType(), True),
    StructField("long", DoubleType(), True),
    StructField("city_pop", IntegerType(), True),
    StructField("job", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("trans_num", StringType(), True),
    StructField("unix_time", IntegerType(), True),
    StructField("merch_lat", DoubleType(), True),
    StructField("merch_long", DoubleType(), True),
    StructField("is_fraud", IntegerType(), True)
])

# Generate sample data
sample_data = []
for i in range(1000):
    sample_data.append(Row(
        cc_num=f"1234567890123{i:03d}",
        merchant=f"Merchant_{i%100}",
        category=random.choice(["grocery_pos", "gas_transport", "entertainment", "shopping_net"]),
        amt=round(random.uniform(1.0, 500.0), 2),
        first=f"First_{i%50}",
        last=f"Last_{i%50}",
        gender=random.choice(["M", "F"]),
        street=f"Street_{i}",
        city=f"City_{i%20}",
        state=f"ST_{i%5}",
        zip=random.randint(10000, 99999),
        lat=round(random.uniform(25.0, 49.0), 6),
        long=round(random.uniform(-125.0, -65.0), 6),
        city_pop=random.randint(1000, 100000),
        job=f"Job_{i%10}",
        dob="1980-01-01",
        trans_num=f"TRANS_{i:06d}",
        unix_time=1640995200 + i,
        merch_lat=round(random.uniform(25.0, 49.0), 6),
        merch_long=round(random.uniform(-125.0, -65.0), 6),
        is_fraud=random.choice([0, 0, 0, 0, 1])  # 20% fraud rate
    ))

df = spark.createDataFrame(sample_data, schema)
print(f"📊 Created sample dataset with {df.count()} records")

# Basic preprocessing
label_col = "is_fraud"
feature_cols = ["amt", "lat", "long", "city_pop", "unix_time", "merch_lat", "merch_long"]

print("🔧 Setting up ML pipeline...")

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
rf = RandomForestClassifier(labelCol=label_col, featuresCol="scaledFeatures", numTrees=50)

pipeline = Pipeline(stages=[assembler, scaler, rf])

print("🎯 Training model...")
model = pipeline.fit(df)

# Create model directory if it doesn't exist
os.makedirs("/app/model", exist_ok=True)

# Save model to file
model.write().overwrite().save("/app/model/fraud_model")

print("✅ Model training completed and saved to /app/model/fraud_model")

# Basic model evaluation
predictions = model.transform(df)
predictions.select("is_fraud", "prediction", "probability").show(10)

spark.stop()
print("🎉 Training completed successfully!")
