from airflow.providers.postgres.hooks.postgres import PostgresHook

RAW_DATA_TABLE_DDL = """
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

PROCESSED_DATA_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS processed_data (
    record_id TEXT PRIMARY KEY,
    event_ts TIMESTAMPTZ NOT NULL,
    value NUMERIC NOT NULL,
    payload JSONB NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

METRICS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS metrics (
    metric_id SERIAL PRIMARY KEY,
    metric_ts TIMESTAMPTZ NOT NULL,
    record_count INTEGER NOT NULL,
    avg_value NUMERIC,
    min_value NUMERIC,
    max_value NUMERIC,
    report_path TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


def get_postgres_hook(conn_id: str = "postgres_default") -> PostgresHook:
    return PostgresHook(postgres_conn_id=conn_id)


def ensure_tables(conn_id: str = "postgres_default") -> None:
    hook = get_postgres_hook(conn_id)
    hook.run(RAW_DATA_TABLE_DDL)
    hook.run(PROCESSED_DATA_TABLE_DDL)
    hook.run(METRICS_TABLE_DDL)
