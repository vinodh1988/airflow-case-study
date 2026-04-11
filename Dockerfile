FROM apache/airflow:3.2.0-python3.13

ENV AIRFLOW_HOME=/opt/airflow
ENV PATH=/opt/airflow/.local/bin:/opt/airflow/venv/bin:${PATH}

USER root
COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt
RUN airflow version
USER airflow
