FROM apache/airflow:2.8.3-python3.11

USER root
COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt
USER airflow
