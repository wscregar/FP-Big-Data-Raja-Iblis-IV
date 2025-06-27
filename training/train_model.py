from minio import Minio
import pandas as pd
import os
from io import BytesIO
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import joblib

# Setup MinIO
client = Minio(
    "minio:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

bucket_data = "batches"
bucket_model = "models"

# Pastikan bucket untuk data ada
if not client.bucket_exists(bucket_data):
    print(f"❌ Bucket data '{bucket_data}' tidak ditemukan.")
    exit()

# Buat bucket untuk model jika belum ada
if not client.bucket_exists(bucket_model):
    client.make_bucket(bucket_model)
    print(f" Bucket model '{bucket_model}' dibuat.")
else:
    print(f" Bucket model '{bucket_model}' tersedia.")


# Ambil Data dari MinIO
print("📥 Mengambil data batch dari MinIO...")

objects = client.list_objects(bucket_data)
all_batches = []

for obj in objects:
    try:
        response = client.get_object(bucket_data, obj.object_name)
        df = pd.read_csv(response)
        all_batches.append(df)
        print(f"✅ Diambil: {obj.object_name}")
    except Exception as e:
        print(f"⚠️ Gagal baca {obj.object_name}: {e}")

if not all_batches:
    print("❌ Tidak ada data ditemukan di MinIO.")
    exit()

df_all = pd.concat(all_batches, ignore_index=True)
print(f"📊 Total data: {len(df_all)} baris")


# Preprocessing

if 'is_fraud' not in df_all.columns:
    print("❌ Kolom 'is_fraud' tidak ditemukan.")
    exit()

features = ['amt', 'category', 'merchant', 'gender']
target = 'is_fraud'

df = df_all[features + [target]].copy()

# Bersihkan data
df['amt'] = pd.to_numeric(df['amt'], errors='coerce')
df['amt'] = df['amt'].fillna(df['amt'].median())
df['category'] = df['category'].fillna("unknown")
df['merchant'] = df['merchant'].fillna("unknown")
df['gender'] = df['gender'].fillna("U")
df = df.dropna(subset=[target])

# Encode kategorikal
X = pd.get_dummies(df[features])
y = df[target]

# Validasi kelas target
if y.nunique() < 2:
    print("❌ Target hanya memiliki satu kelas.")
    exit()

# Split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)


# Train Model
model = RandomForestClassifier(
    n_estimators=150,
    max_depth=15,
    class_weight='balanced',
    n_jobs=-1,
    random_state=42
)
model.fit(X_train, y_train)


# Evaluasi

y_pred = model.predict(X_test)
print("\n Evaluasi Model:")
print(classification_report(y_test, y_pred, digits=4))


# Simpan ke MinIO
print(" Menyimpan model ke MinIO...")

model_buffer = BytesIO()
joblib.dump(model, model_buffer)
model_buffer.seek(0)

model_object_name = "fraud_model.pkl"
client.put_object(
    bucket_model,
    model_object_name,
    data=model_buffer,
    length=model_buffer.getbuffer().nbytes,
    content_type="application/octet-stream"
)

print(f"✅ Model disimpan ke MinIO di bucket '{bucket_model}' dengan nama '{model_object_name}'")
