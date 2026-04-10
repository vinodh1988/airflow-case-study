FROM apache/airflow:2.8.3-python3.11

USER airflow
ENV PATH="/home/airflow/.local/bin:/opt/airflow/.local/bin:${PATH}"
COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --user --no-cache-dir -r /requirements.txt
