from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from db_utils import RAW_DATA_TABLE_DDL, get_postgres_hook

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="data_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Fetch raw JSON payloads from the fake API and store them in PostgreSQL.",
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=5),
    tags=["ingestion", "raw"],
) as dag:

    def fetch_data(**context: Any) -> list[dict[str, Any]]:
        response = requests.get("http://fake-api:5000/data", timeout=15)
        response.raise_for_status()
        payload = response.json().get("data")
        if not isinstance(payload, list):
            raise ValueError("Fake API returned invalid payload format")
        return payload

    def validate_payload(ti: Any, **context: Any) -> list[dict[str, Any]]:
        records = ti.xcom_pull(task_ids="fetch_data")
        if not records:
            raise ValueError("No records returned from fake API")

        validated = []
        for index, record in enumerate(records):
            if not isinstance(record, dict):
                raise ValueError(f"Invalid record at index {index}: {record}")
            if "record_id" not in record or "event_ts" not in record or "value" not in record:
                raise ValueError(f"Malformed payload at index {index}: {record}")
            validated.append(
                {
                    "record_id": str(record["record_id"]),
                    "source_ts": record["event_ts"],
                    "item_value": float(record["value"]),
                    "payload": record,
                }
            )
        return validated

    def store_raw_data(ti: Any, **context: Any) -> None:
        records = ti.xcom_pull(task_ids="validate_payload")
        if not records:
            raise ValueError("Validation step returned no records")

        hook = get_postgres_hook()
        hook.run(RAW_DATA_TABLE_DDL)

        insert_sql = """
            INSERT INTO raw_data (record_id, source_ts, item_value, payload, processed)
            VALUES (%s, %s, %s, %s, FALSE)
            ON CONFLICT (record_id) DO UPDATE
            SET source_ts = EXCLUDED.source_ts,
                item_value = EXCLUDED.item_value,
                payload = EXCLUDED.payload,
                processed = FALSE;
        """

        for record in records:
            hook.run(
                insert_sql,
                parameters=(
                    record["record_id"],
                    datetime.fromisoformat(record["source_ts"]),
                    record["item_value"],
                    json.dumps(record["payload"]),
                ),
            )

    fetch_data_task = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data,
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=2),
    )

    validate_payload_task = PythonOperator(
        task_id="validate_payload",
        python_callable=validate_payload,
    )

    store_raw_data_task = PythonOperator(
        task_id="store_raw_data",
        python_callable=store_raw_data,
    )

    fetch_data_task >> validate_payload_task >> store_raw_data_task
