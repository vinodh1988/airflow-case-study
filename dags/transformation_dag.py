from __future__ import annotations

from datetime import timedelta
from typing import Any

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dags.db_utils import get_postgres_hook, PROCESSED_DATA_TABLE_DDL, RAW_DATA_TABLE_DDL

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="data_transformation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Transform raw ingestion records into normalized processed data.",
    schedule_interval="*/10 * * * *",
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=["transformation", "processed"],
) as dag:

    def extract_raw_data(**context: Any) -> list[dict[str, Any]]:
        hook = get_postgres_hook()
        hook.run(RAW_DATA_TABLE_DDL)
        query = "SELECT record_id, source_ts, item_value, payload FROM raw_data WHERE processed = FALSE"
        rows = hook.get_records(query)
        return [
            {
                "record_id": row[0],
                "event_ts": row[1].isoformat() if row[1] else None,
                "value": float(row[2]),
                "payload": row[3],
            }
            for row in rows
        ]

    def transform_and_load(ti: Any, **context: Any) -> None:
        records = ti.xcom_pull(task_ids="extract_raw_data") or []
        if not records:
            return

        hook = get_postgres_hook()
        hook.run(PROCESSED_DATA_TABLE_DDL)

        frame = pd.DataFrame(records)
        frame = frame.drop_duplicates(subset=["record_id"], keep="last")

        insert_sql = """
            INSERT INTO processed_data (record_id, event_ts, value, payload)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (record_id) DO UPDATE
            SET event_ts = EXCLUDED.event_ts,
                value = EXCLUDED.value,
                payload = EXCLUDED.payload,
                loaded_at = NOW();
        """

        updated_ids = []
        for _, row in frame.iterrows():
            hook.run(
                insert_sql,
                parameters=(
                    str(row["record_id"]),
                    row["event_ts"],
                    float(row["value"]),
                    row["payload"],
                ),
            )
            updated_ids.append(row["record_id"])

        if updated_ids:
            update_raw_sql = """
                UPDATE raw_data
                SET processed = TRUE,
                    processed_at = NOW()
                WHERE record_id = ANY(%s)
            """
            hook.run(update_raw_sql, parameters=(updated_ids,))

    extract_raw_data_task = PythonOperator(
        task_id="extract_raw_data",
        python_callable=extract_raw_data,
    )

    transform_and_load_task = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_and_load,
    )

    extract_raw_data_task >> transform_and_load_task
