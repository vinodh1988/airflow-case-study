from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

import pymongo
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyspark.sql import SparkSession

RAW_DATA_DATASET = Dataset("postgres://airflow/raw_data")
MONGO_DATASET = Dataset("mongodb://mongo/airflow.raw_data")
SHARED_DATASET = Dataset("file:///opt/airflow/shared_data/raw_data_export.json")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="spark_data_pipeline",
    default_args=DEFAULT_ARGS,
    description="Move synthetic data from Postgres to MongoDB and export it to shared storage.",
    schedule_interval="*/10 * * * *",
    start_date=datetime(2026, 4, 11),
    catchup=False,
    dagrun_timeout=timedelta(minutes=15),
    tags=["spark", "mongodb", "dataset"],
) as dag:

    def postgres_to_mongodb(**context: Any) -> int:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        rows = postgres_hook.get_records(
            "SELECT record_id, source_ts, item_value, payload FROM raw_data WHERE processed = FALSE"
        )
        if not rows:
            return 0

        spark = SparkSession.builder.master("local[*]").appName("postgres_to_mongodb").getOrCreate()
        data = [
            {
                "record_id": row[0],
                "source_ts": row[1].isoformat() if row[1] else None,
                "item_value": float(row[2]) if row[2] is not None else None,
                "payload": row[3],
            }
            for row in rows
        ]
        df = spark.createDataFrame(data)
        extracted = [row.asDict(recursive=True) for row in df.collect()]

        client = pymongo.MongoClient("mongodb://mongo:27017/")
        collection = client.airflow.raw_data
        for record in extracted:
            document = {
                "_id": record["record_id"],
                "source_ts": record["source_ts"],
                "item_value": record["item_value"],
                "payload": record["payload"],
            }
            collection.replace_one({"_id": document["_id"]}, document, upsert=True)

        client.close()
        return len(extracted)

    def mongodb_to_shared_volume(**context: Any) -> int:
        client = pymongo.MongoClient("mongodb://mongo:27017/")
        collection = client.airflow.raw_data
        documents = list(collection.find({}))
        client.close()

        if not documents:
            return 0

        spark = SparkSession.builder.master("local[*]").appName("mongodb_to_shared_volume").getOrCreate()
        processed = [
            {
                "record_id": str(doc["_id"]),
                "source_ts": doc.get("source_ts"),
                "item_value": float(doc.get("item_value")) if doc.get("item_value") is not None else None,
                "payload": doc.get("payload"),
            }
            for doc in documents
        ]
        df = spark.createDataFrame(processed)
        output = [row.asDict(recursive=True) for row in df.collect()]

        output_path = "/opt/airflow/shared_data/raw_data_export.json"
        with open(output_path, "w", encoding="utf-8") as handle:
            json.dump(output, handle, default=str, indent=2)

        return len(output)

    postgres_to_mongodb_task = PythonOperator(
        task_id="postgres_to_mongodb",
        python_callable=postgres_to_mongodb,
        inlets=[RAW_DATA_DATASET],
        outlets=[MONGO_DATASET],
    )

    mongodb_to_shared_task = PythonOperator(
        task_id="mongodb_to_shared_volume",
        python_callable=mongodb_to_shared_volume,
        inlets=[MONGO_DATASET],
        outlets=[SHARED_DATASET],
    )

    postgres_to_mongodb_task >> mongodb_to_shared_task
