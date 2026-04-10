from __future__ import annotations

from datetime import timedelta
from email.mime.text import MIMEText
from typing import Any

import smtplib
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dags.db_utils import get_postgres_hook, PROCESSED_DATA_TABLE_DDL

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

ALERT_THRESHOLD = 0.20
MAILHOG_HOST = "mailhog"
MAILHOG_PORT = 1025
ALERT_RECIPIENT = "writevinodh@gmail.com"
ALERT_SENDER = "alerting@data-platform.local"

with DAG(
    dag_id="alerting_notification_pipeline",
    default_args=DEFAULT_ARGS,
    description="Detect anomalies in processed data and notify via Mailhog email.",
    schedule_interval="*/15 * * * *",
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=["alerting", "notification"],
) as dag:

    def detect_anomalies(**context: Any) -> list[dict[str, Any]]:
        hook = get_postgres_hook()
        hook.run(PROCESSED_DATA_TABLE_DDL)

        avg_value = hook.get_first("SELECT AVG(value) FROM processed_data")
        if not avg_value or avg_value[0] is None:
            return []

        avg_value = float(avg_value[0])
        rows = hook.get_records(
            "SELECT record_id, value FROM processed_data ORDER BY loaded_at DESC LIMIT 50"
        )

        anomalies = []
        for record_id, value in rows:
            ratio = abs(float(value) - avg_value) / avg_value if avg_value else 0.0
            if ratio > ALERT_THRESHOLD:
                anomalies.append(
                    {
                        "record_id": record_id,
                        "value": float(value),
                        "avg_value": avg_value,
                        "deviation": round(ratio * 100, 2),
                    }
                )
        return anomalies

    def send_notification(ti: Any, **context: Any) -> None:
        anomalies = ti.xcom_pull(task_ids="detect_anomalies") or []
        if not anomalies:
            return

        subject = "Airflow Alert: Data anomaly detected"
        body_lines = ["The analytics pipeline detected anomalous records:", ""]
        for anomaly in anomalies:
            body_lines.append(
                f"- {anomaly['record_id']}: value={anomaly['value']} avg={anomaly['avg_value']} deviation={anomaly['deviation']}%"
            )

        message = MIMEText("\n".join(body_lines))
        message["Subject"] = subject
        message["From"] = ALERT_SENDER
        message["To"] = ALERT_RECIPIENT

        with smtplib.SMTP(host=MAILHOG_HOST, port=MAILHOG_PORT) as smtp:
            smtp.sendmail(ALERT_SENDER, [ALERT_RECIPIENT], message.as_string())

    detect_anomalies_task = PythonOperator(
        task_id="detect_anomalies",
        python_callable=detect_anomalies,
    )

    send_notification_task = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification,
    )

    detect_anomalies_task >> send_notification_task
