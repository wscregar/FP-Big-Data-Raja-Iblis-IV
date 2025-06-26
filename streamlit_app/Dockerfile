FROM python:3.10

WORKDIR /app

COPY . .

RUN pip install streamlit pandas pyspark joblib

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.enableCORS=false"]
