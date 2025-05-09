FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y gcc python3-dev && \
    echo "airflow:x:501:0::/home/airflow:/bin/bash" >> /etc/passwd && \
    chown -R airflow:root /opt/airflow

USER airflow
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt
COPY --chown=airflow:root script/entrypoint.sh /opt/airflow/script/entrypoint.sh

RUN chmod +x /opt/airflow/script/entrypoint.sh && \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt