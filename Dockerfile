FROM apache/airflow:2.11.4-python3.11

ENV AIRFLOW_HOME=/opt/airflow
ENV PATH=/opt/airflow/.local/bin:/opt/airflow/venv/bin:${PATH}

USER root
COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt
RUN airflow version
USER airflow
