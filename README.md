# Airflow POC: Mini Data Platform Orchestration

This repository implements a Dockerized Apache Airflow proof-of-concept based on the provided SRE-oriented specification.

## Architecture
- Apache Airflow with `CeleryExecutor`
- PostgreSQL metadata and data store
- Redis task broker
- Fake API service for ingestion
- Mailhog for email capture
- Adminer for database inspection

## Services
- Airflow Webserver: `http://localhost:8080`
- Fake API: `http://localhost:5000/data`
- Mailhog UI: `http://localhost:8025`
- Adminer: `http://localhost:8081`

## Setup
1. Build and start all containers:
   ```bash
   docker compose up --build
   ```

2. Wait for `airflow-init` to complete.

3. Access Airflow UI and trigger DAGs manually or wait for scheduled execution.

## Airflow credentials
- Username: `admin`
- Password: `admin`

## DAGs
- `data_ingestion_pipeline`: fetches JSON from the Fake API, validates it, and persists raw records.
- `data_transformation_pipeline`: cleans and deduplicates raw data into processed storage.
- `analytics_pipeline`: aggregates metrics and writes a CSV report.
- `alerting_notification_pipeline`: detects anomalies and sends email alerts through Mailhog.

## Notes
- The DAGs automatically create required database tables if they do not exist.
- Reports are written to `./reports` on the host.
- Use Adminer with the `airflow` user and password `airflow` to inspect the PostgreSQL database.
