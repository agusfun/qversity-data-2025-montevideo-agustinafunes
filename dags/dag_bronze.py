# DAG to ingest raw JSON data into the Bronze layer of the database

from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import requests
import os
import json
import pandas as pd
from sqlalchemy import create_engine, text

DATA_URL = "https://qversity-raw-public-data.s3.amazonaws.com/mobile_customers_messy_dataset.json"
RAW_PATH = "/opt/airflow/data/raw/mobile_customers_messy_dataset.json"
POSTGRES_URI = "postgresql+psycopg2://qversity-admin:qversity-admin@postgres:5432/qversity"

default_args = {
    'start_date': datetime(2025, 6, 1),
}

with DAG(
    dag_id='dag_bronze',
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description='Ingests raw JSON data into the Bronze layer',
    tags=["bronze"]
) as dag:

    @task()
    def download_json():
        response = requests.get(DATA_URL)
        os.makedirs(os.path.dirname(RAW_PATH), exist_ok=True)
        with open(RAW_PATH, "wb") as f:
            f.write(response.content)

    @task()
    def load_to_postgres():
        with open(RAW_PATH, "r") as f:
            data = json.load(f)

        df = pd.DataFrame(data)
        for col in ["contracted_services", "payment_history"]:
            df[col] = df[col].apply(json.dumps)

        engine = create_engine(POSTGRES_URI)
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))

        df.to_sql(
            name="raw_customers",
            con=engine,
            schema="bronze",
            if_exists="replace",
            index=False
        )

    download_json() >> load_to_postgres()