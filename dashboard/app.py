import streamlit as st
from minio import Minio
import pandas as pd
import shutil
import zipfile
import os
import time
from io import BytesIO
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

# ================================
# 🌟 Setup
# ================================
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
DATA_BUCKET = "batches"
MODEL_BUCKET = "models"
MODEL_ZIP = "fraud_model_spark.zip"

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")
st.title("💳 Dashboard Deteksi Penipuan Transaksi (Spark ML)")

# ================================
# 🔧 Setup Spark
# ================================
spark = SparkSession.builder \
    .appName("FraudDetectionDashboard") \
    .getOrCreate()

# ================================
# 📅 Load Data
# ================================
def load_data():
    objects = client.list_objects(DATA_BUCKET)
    dfs = []
    for obj in objects:
        try:
            data = client.get_object(DATA_BUCKET, obj.object_name)
            temp_path = f"/tmp/{obj.object_name}"
            with open(temp_path, "wb") as f:
                f.write(data.read())
            df = pd.read_csv(temp_path)
            dfs.append(df)
        except:
            continue
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# ================================
# 🔹 Load Model PySpark
# ================================
def load_model():
    zip_path = "/tmp/fraud_model.zip"
    model_path = "/tmp/spark_fraud_model"

    if not os.path.exists(zip_path):
        response = client.get_object(MODEL_BUCKET, MODEL_ZIP)
        with open(zip_path, "wb") as f:
            f.write(response.read())

    if os.path.exists(model_path):
        shutil.rmtree(model_path)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(model_path)

    return PipelineModel.load(model_path)

# ================================
# 🔄 Auto-refresh
# ================================
refresh_interval = st.sidebar.slider("⏱️ Auto-refresh interval (detik)", 0, 60, 0)
if refresh_interval > 0:
    st.sidebar.info(f"Auto-refresh tiap {refresh_interval} detik.")
    time.sleep(refresh_interval)
    st.experimental_rerun()

# ================================
# 🚀 Jalankan
# ================================
model = load_model()
df = load_data()

if df.empty:
    st.warning("❌ Tidak ada data ditemukan.")
    st.stop()

# ================================
# 🔍 Filter Search
# ================================
st.sidebar.subheader("🔍 Filter Pencarian")
search_name = st.sidebar.text_input("Nama (first/last)")
search_merchant = st.sidebar.text_input("Merchant")
search_category = st.sidebar.text_input("Kategori")

if search_name:
    df = df[df["first"].str.contains(search_name, case=False, na=False) |
            df["last"].str.contains(search_name, case=False, na=False)]
if search_merchant:
    df = df[df["merchant"].str.contains(search_merchant, case=False, na=False)]
if search_category:
    df = df[df["category"].str.contains(search_category, case=False, na=False)]

# ================================
# 📊 Statistik
# ================================
st.subheader("📊 Statistik Transaksi")
col1, col2, col3 = st.columns(3)
col1.metric("Total Transaksi", len(df))
col2.metric("Fraud", int(df["is_fraud"].sum()) if "is_fraud" in df.columns else "-")
col3.metric("Kategori Unik", df["category"].nunique())

st.dataframe(df.head(50))

# ================================
# 📊 Insight Visualisasi
# ================================
st.subheader("📊 Visualisasi Insight")
col1, col2 = st.columns(2)
col1.bar_chart(df["category"].value_counts())
col2.bar_chart(df["merchant"].value_counts().head(10))

col3, col4 = st.columns(2)
if "hour" in df.columns:
    col3.bar_chart(df["hour"].value_counts().sort_index())
if "trx_count_per_card" in df.columns:
    col4.line_chart(df[["trx_count_per_card"]])

# ================================
# 🧠 Prediksi Form
# ================================
st.subheader("🔮 Prediksi Transaksi Baru")
with st.form("predict_form"):
    amt = st.number_input("Jumlah Transaksi (amt)", min_value=0.0, step=10.0)
    category = st.selectbox("Kategori", sorted(df["category"].dropna().unique()))
    merchant = st.selectbox("Merchant", sorted(df["merchant"].dropna().unique()))
    gender = st.selectbox("Gender", ["M", "F", "U"])
    hour = st.slider("Jam Transaksi", 0, 23, 12)
    day_of_week = st.slider("Hari (1=Senin - 7=Minggu)", 1, 7, 1)
    is_night = 1 if hour < 6 or hour >= 22 else 0
    trx_count_per_card = st.slider("Jumlah Transaksi oleh Kartu", 1, 100, 5)
    submitted = st.form_submit_button("Prediksi")

    if submitted and model:
        input_spark = spark.createDataFrame([{
            "amt": amt,
            "category": category,
            "merchant": merchant,
            "gender": gender,
            "hour": hour,
            "day_of_week": day_of_week,
            "is_night": is_night,
            "trx_count_per_card": trx_count_per_card
        }])
        prediction = model.transform(input_spark).select("prediction").collect()[0][0]
        label = "❗ FRAUD" if prediction == 1 else "✅ Normal"
        st.success(f"Hasil Prediksi: {label}")
