from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dags.db_utils import get_postgres_hook, METRICS_TABLE_DDL, PROCESSED_DATA_TABLE_DDL

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

REPORT_ROOT = Path("/opt/airflow/reports")

with DAG(
    dag_id="analytics_pipeline",
    default_args=DEFAULT_ARGS,
    description="Aggregate processed data and generate a CSV insight report.",
    schedule_interval="0 * * * *",
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=["analytics", "metrics"],
) as dag:

    def aggregate_metrics(**context: Any) -> str:
        hook = get_postgres_hook()
        hook.run(PROCESSED_DATA_TABLE_DDL)
        hook.run(METRICS_TABLE_DDL)

        stats = hook.get_first(
            "SELECT COUNT(*), AVG(value), MIN(value), MAX(value) FROM processed_data"
        )
        record_count, avg_value, min_value, max_value = stats or (0, None, None, None)
        report_data = {
            "generated_at": [datetime.utcnow().isoformat()],
            "record_count": [int(record_count)],
            "avg_value": [float(avg_value) if avg_value is not None else None],
            "min_value": [float(min_value) if min_value is not None else None],
            "max_value": [float(max_value) if max_value is not None else None],
        }

        REPORT_ROOT.mkdir(parents=True, exist_ok=True)
        report_path = REPORT_ROOT / f"metrics_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"

        pd.DataFrame(report_data).to_csv(report_path, index=False)

        hook.run(
            """
            INSERT INTO metrics (metric_ts, record_count, avg_value, min_value, max_value, report_path)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            parameters=(
                datetime.utcnow(),
                int(record_count),
                avg_value,
                min_value,
                max_value,
                str(report_path),
            ),
        )

        return str(report_path)

    aggregate_metrics_task = PythonOperator(
        task_id="aggregate_metrics",
        python_callable=aggregate_metrics,
    )
