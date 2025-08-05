from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess

def run_etl_script():
    # Run the app.py script from the mounted volume
    subprocess.run(["python", "/opt/airflow/python/app.py"], check=True)

with DAG(
    dag_id='s3_to_snowflake_etl',
    start_date=datetime(2023, 1, 1),
    schedule=timedelta(days=1),
    catchup=False,
    tags=['etl', 'spark', 'snowflake']
) as dag:

    run_etl = PythonOperator(
        task_id='run_python_etl',
        python_callable=run_etl_script
    )
