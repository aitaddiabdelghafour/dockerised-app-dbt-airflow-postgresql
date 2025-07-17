from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import subprocess
import os

# === CONFIG ===
DATA_DIR = "/opt/airflow/data"
POSTGRESQL_CONN_ID = "postgres_default"
SCHEMA_SUFFIX = "_schema.sql"

# Define your delimiter here
DELIMITER=","

def generate_schema(file_path, table_name, schema_path):
    cmd = f"csvsql --dialect postgresql --tables {table_name} {file_path} > {schema_path}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"csvsql failed for {file_path}: {result.stderr}")
    print(f"[{table_name}] Schema generated.")

def create_table(schema_path, table_name):
    hook = PostgresHook(POSTGRESQL_CONN_ID=POSTGRESQL_CONN_ID)
    with open(schema_path, "r") as f:
        sql = f.read()
    hook.run(sql)
    print(f"[{table_name}] Table created.")

def load_data(file_path, table_name):
    df = pd.read_csv(file_path, delimiter=DELIMITER)
    hook = PostgresHook(POSTGRESQL_CONN_ID=POSTGRESQL_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    print(f"[{table_name}] Loaded {len(df)} rows.")

default_args = {
    'start_date': datetime(2025, 1, 1)
}

with DAG(
        dag_id="multi_csv_to_postgresql_dag",
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["csv", "postgresql", "multi-file"]
) as dag:

    for filename in os.listdir(DATA_DIR):
        if not filename.endswith(".csv"):
            continue

        file_path = os.path.join(DATA_DIR, filename)
        table_name = os.path.splitext(filename)[0]
        schema_path = os.path.join(DATA_DIR, table_name + SCHEMA_SUFFIX)

        task_generate_schema = PythonOperator(
            task_id=f"generate_schema_{table_name}",
            python_callable=generate_schema,
            op_args=[file_path, table_name, schema_path]
        )

        task_create_table = PythonOperator(
            task_id=f"create_table_{table_name}",
            python_callable=create_table,
            op_args=[schema_path, table_name]
        )

        task_load_data = PythonOperator(
            task_id=f"load_data_{table_name}",
            python_callable=load_data,
            op_args=[file_path, table_name]
        )

        task_generate_schema >> task_create_table >> task_load_data
