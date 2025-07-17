from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('dbt_docker_run',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    dbt_run = DockerOperator(
        task_id='dbt_run',
        image='project-image:latest',
        api_version='auto',
        auto_remove=True,
        command='dbt run --profiles-dir /dbt',
        docker_url='unix://var/run/docker.sock',
        network_mode='airpost',
        working_dir='/dbt',
        mount_tmp_dir=False
    )

    dbt_run
