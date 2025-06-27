from minio import Minio
from io import BytesIO
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp, hour, dayofweek, count
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# ========================
# ⚙️ Inisialisasi Spark
# ========================
spark = SparkSession.builder \
    .appName("FraudDetectionTraining") \
    .getOrCreate()

# ========================
# 🌟 Koneksi ke MinIO
# ========================
client = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_data = "batches"
bucket_model = "models"

if not client.bucket_exists(bucket_data):
    print(f"❌ Bucket '{bucket_data}' tidak ditemukan.")
    exit()

if not client.bucket_exists(bucket_model):
    client.make_bucket(bucket_model)

# ========================
# 📅 Ambil data dari MinIO
# ========================
print("📅 Mengambil data batch dari MinIO...")

objects = list(client.list_objects(bucket_data))
if not objects:
    print("❌ Tidak ada data di bucket.")
    exit()

dfs = []
for obj in objects:
    response = client.get_object(bucket_data, obj.object_name)
    temp_path = f"/tmp/{obj.object_name}"
    with open(temp_path, "wb") as f:
        f.write(response.read())
    df = spark.read.option("header", "true").csv(temp_path, inferSchema=True)
    dfs.append(df)

df_all = dfs[0]
for df in dfs[1:]:
    df_all = df_all.unionByName(df)

print(f"✅ Total data: {df_all.count()} baris")

# ========================
# 🦜 Feature Engineering
# ========================
df = df_all.select("amt", "category", "merchant", "gender", "is_fraud", "trans_date_trans_time", "cc_num") \
    .dropna(subset=["is_fraud"]) \
    .fillna({"category": "unknown", "merchant": "unknown", "gender": "U"})

# Konversi tipe data
df = df.withColumn("amt", df["amt"].cast("double"))
df = df.withColumn("trans_time", to_timestamp("trans_date_trans_time"))
df = df.withColumn("hour", hour("trans_time"))
df = df.withColumn("day_of_week", dayofweek("trans_time"))
df = df.withColumn("is_night", when((col("hour") < 6) | (col("hour") >= 22), 1).otherwise(0))

# Jumlah transaksi per kartu
w = Window.partitionBy("cc_num")
df = df.withColumn("trx_count_per_card", count("*").over(w))

# ========================
# ⚖️ Encode & Assemble
# ========================
indexers = [
    StringIndexer(inputCol=col, outputCol=col + "_idx", handleInvalid="keep")
    for col in ["category", "merchant", "gender"]
]
encoders = [
    OneHotEncoder(inputCol=col + "_idx", outputCol=col + "_vec")
    for col in ["category", "merchant", "gender"]
]

assembler = VectorAssembler(
    inputCols=[
        "amt", "category_vec", "merchant_vec", "gender_vec",
        "hour", "day_of_week", "is_night", "trx_count_per_card"
    ],
    outputCol="features"
)

rf = RandomForestClassifier(
    labelCol="is_fraud",
    featuresCol="features",
    numTrees=150,
    maxDepth=15,
    seed=42
)

pipeline = Pipeline(stages=indexers + encoders + [assembler, rf])

# ========================
# 🧠 Training
# ========================
train, test = df.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train)

# ========================
# 📊 Evaluasi
# ========================
pred = model.transform(test)
evaluator = MulticlassClassificationEvaluator(labelCol="is_fraud", metricName="f1")
f1_score = evaluator.evaluate(pred)
print(f"📊 F1 Score: {f1_score:.4f}")

# ========================
# 📁 Simpan model ke MinIO
# ========================
local_model_dir = "/tmp/spark_fraud_model"
if os.path.exists(local_model_dir):
    shutil.rmtree(local_model_dir)
model.save(local_model_dir)

# Kompres jadi ZIP
shutil.make_archive("/tmp/fraud_model", 'zip', local_model_dir)

# Upload ke MinIO
with open("/tmp/fraud_model.zip", "rb") as f:
    client.put_object(
        bucket_model,
        "fraud_model_spark.zip",
        data=f,
        length=os.path.getsize("/tmp/fraud_model.zip"),
        content_type="application/zip"
    )

print("✅ Model PySpark disimpan ke MinIO sebagai 'fraud_model_spark.zip'")
