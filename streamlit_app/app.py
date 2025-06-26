import streamlit as st
import pandas as pd
import joblib
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

# UI Title
st.title("📊 Credit Card Fraud Monitoring Dashboard")

# Load model from saved PySpark model
@st.cache_resource
def load_model():
    spark = SparkSession.builder.getOrCreate()
    model = PipelineModel.load("spark/model/fraud_model")
    return model, spark

model, spark = load_model()

# Load data untuk testing (stream simulasi)
uploaded_file = st.file_uploader("Upload batch file (CSV):", type=["csv"])
if uploaded_file:
    df_pd = pd.read_csv(uploaded_file)
    st.write("📋 Preview Data", df_pd.head())

    df_spark = spark.createDataFrame(df_pd)
    pred_df = model.transform(df_spark).select("scaledFeatures", "prediction")

    pred_pd = pred_df.toPandas()
    df_pd["Prediction"] = pred_pd["prediction"]

    # Visualisasi hasil
    fraud_count = df_pd["Prediction"].sum()
    total = len(df_pd)

    st.metric("Jumlah Transaksi", total)
    st.metric("Deteksi Fraud", int(fraud_count))

    st.bar_chart(df_pd["Prediction"].value_counts())
    st.dataframe(df_pd.head(10))
