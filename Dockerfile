FROM apache/airflow:3.2.0-python3.13

USER root
COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt
USER airflow
