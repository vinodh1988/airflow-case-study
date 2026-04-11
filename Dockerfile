FROM apache/airflow:3.2.0-python3.13

USER airflow
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN python -m pip install --user --no-cache-dir -r /tmp/requirements.txt
