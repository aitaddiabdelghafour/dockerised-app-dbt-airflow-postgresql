FROM apache/airflow:2.9.1-python3.9

COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

USER root
RUN apt-get update && \
    apt-get install -y postgresql-client && \
    rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir psycopg2-binary requests csvkit  \
    dbt-core==1.7.9 \
    dbt-postgres==1.7.9
