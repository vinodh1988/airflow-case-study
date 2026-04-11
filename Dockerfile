FROM apache/airflow:2.11.2-python3.12

USER root
COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt
USER airflow
