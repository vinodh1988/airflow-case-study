FROM apache/airflow:2.8.3-python3.11

ENV AIRFLOW_HOME=/opt/airflow
ENV HOME=/opt/airflow
ENV PATH=/opt/airflow/.local/bin:/opt/airflow/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

USER airflow
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN python -m pip install --user --no-cache-dir -r /tmp/requirements.txt
