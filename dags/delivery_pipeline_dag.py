from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="delivery_pipeline",
    default_args=default_args,
    description="End-to-end delivery DWH pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["delivery", "dwh"],
) as dag:

    download_data = BashOperator(
        task_id="download_data",
        bash_command="cd /opt/airflow && python scripts/download_data.py",
    )

    load_to_staging = BashOperator(
        task_id="load_to_staging",
        bash_command="cd /opt/airflow && python scripts/load_to_staging.py",
    )

    cleanup_pipeline = BashOperator(
        task_id="cleanup_pipeline",
        bash_command="cd /opt/airflow && python scripts/cleanup_pipeline.py",
    )

    build_dims_natural = BashOperator(
        task_id="build_dims_natural",
        bash_command="cd /opt/airflow && python scripts/build_dims_natural.py",
    )

    build_dims_reference = BashOperator(
        task_id="build_dims_reference",
        bash_command="cd /opt/airflow && python scripts/build_dims_reference.py",
    )

    build_facts = BashOperator(
        task_id="build_facts",
        bash_command="cd /opt/airflow && python scripts/build_facts.py",
    )

    build_orders_mart = BashOperator(
        task_id="build_orders_mart",
        bash_command="cd /opt/airflow && python scripts/build_orders_mart.py",
    )

    build_items_mart = BashOperator(
        task_id="build_items_mart",
        bash_command="cd /opt/airflow && python scripts/build_items_mart.py",
    )

    (
        download_data
        >> load_to_staging
        >> cleanup_pipeline
        >> build_dims_natural
        >> build_dims_reference
        >> build_facts
        >> [build_orders_mart, build_items_mart]
    )
