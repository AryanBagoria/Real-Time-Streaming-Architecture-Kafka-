from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import psycopg2
import json
import time
from kafka import KafkaProducer

def fetch_and_send():
    # Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Postgres connection
    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # Insert 60 users (1 per second)
    for _ in range(60):
        res = requests.get("https://randomuser.me/api/")
        data = res.json()

        # Store raw JSON in Postgres
        cur.execute(
            "INSERT INTO users (raw_data) VALUES (%s);",
            [json.dumps(data)]
        )
        conn.commit()

        # Send same data to Kafka
        producer.send('random_user_topic', data)
        producer.flush()

        time.sleep(1)  # 1 user per second

    cur.close()
    conn.close()
    producer.close()


with DAG(
    "random_user_to_postgres_and_kafka",
    start_date=datetime(2025, 1, 1),
    schedule_interval="* * * * *",
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="fetch_and_send_user",
        python_callable=fetch_and_send
    )
