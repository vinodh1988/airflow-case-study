FROM apache/airflow:2.8.3-python3.11

USER airflow
ENV PATH="/home/airflow/.local/bin:/opt/airflow/.local/bin:/opt/airflow/venv/bin:${PATH}"
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN python -m pip install --user --no-cache-dir -r /tmp/requirements.txt
