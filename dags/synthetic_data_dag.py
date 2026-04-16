from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

RAW_DATA_DATASET = Dataset("postgres://airflow/raw_data")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

INSERT_SQL = """
    INSERT INTO raw_data (record_id, source_ts, item_value, payload, processed)
    VALUES (%s, %s, %s, %s, FALSE)
    ON CONFLICT (record_id) DO UPDATE
    SET source_ts = EXCLUDED.source_ts,
        item_value = EXCLUDED.item_value,
        payload = EXCLUDED.payload,
        processed = FALSE;
"""

CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS raw_data (
        record_id TEXT PRIMARY KEY,
        source_ts TIMESTAMPTZ NOT NULL,
        item_value NUMERIC NOT NULL,
        payload JSONB NOT NULL,
        processed BOOLEAN NOT NULL DEFAULT FALSE,
        processed_at TIMESTAMPTZ,
        inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
"""

with DAG(
    dag_id="synthetic_data_generation",
    default_args=DEFAULT_ARGS,
    description="Generate synthetic raw events in PostgreSQL every 5 minutes.",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 4, 11),
    catchup=False,
    dagrun_timeout=timedelta(minutes=5),
    tags=["synthetic", "postgres", "dataset"],
) as dag:

    def create_synthetic_data(**context: Any) -> int:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run(CREATE_TABLE_SQL)

        now = datetime.utcnow().replace(microsecond=0).isoformat() + "+00:00"
        records = []
        for _ in range(5):
            record = {
                "record_id": str(uuid.uuid4()),
                "source_ts": now,
                "item_value": round(random.uniform(10.0, 200.0), 2),
                "payload": {
                    "event_ts": now,
                    "record_id": str(uuid.uuid4()),
                    "source": "synthetic",
                    "value": round(random.uniform(10.0, 200.0), 2),
                },
            }
            records.append(record)

        for record in records:
            hook.run(
                INSERT_SQL,
                parameters=(
                    record["record_id"],
                    record["source_ts"],
                    record["item_value"],
                    json.dumps(record["payload"]),
                ),
            )

        return len(records)

    generate_data = PythonOperator(
        task_id="generate_synthetic_raw_data",
        python_callable=create_synthetic_data,
        outlets=[RAW_DATA_DATASET],
    )
