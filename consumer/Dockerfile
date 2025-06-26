FROM python:3.10
WORKDIR /app
COPY . .
RUN pip install kafka-python boto3 pandas pyarrow
CMD ["python", "kafka_consumer.py"]
