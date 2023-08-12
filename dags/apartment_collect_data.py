from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

default_args = {
    'owner': 'cloud.user',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'apartment_data_collector',
    default_args=default_args,
    description='Populates database by scrapping an website',
    schedule_interval=timedelta(hours=1),
    catchup=False,
) as dag:

    t1 = DockerOperator(
        task_id='docker_data_collector',
        image='ryanratedr/kingkong-collectdata:v1.0',
        api_version='auto',
        auto_remove=True,
        command="python scripts/update_database.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(source="/home/ubuntu/KingKong", target="/app/database", type="bind"),
        ]
    )

# DAG Dependencies
t1
