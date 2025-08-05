from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Add the project root to Python path so we can import the app
sys.path.append('/opt/airflow/python')

default_args = {
    'owner': 'yusuf',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'csv_to_snowflake_pipeline',
    default_args=default_args,
    description='Process CSV data and upload to Snowflake',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['csv', 'snowflake', 'etl'],
)

def process_csv_data():
    """Process CSV data and upload to Snowflake"""
    import subprocess
    import os
    
    # Change to the python directory
    python_dir = '/opt/airflow/python'
    os.chdir(python_dir)
    
    # Run the Python script directly (no need for virtual environment in container)
    cmd = [
        'python',
        '/opt/airflow/python/app.py'
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print("Script executed successfully!")
        print("STDOUT:", result.stdout)
        return "Success"
    except subprocess.CalledProcessError as e:
        print(f"Script failed with error: {e}")
        print("STDERR:", e.stderr)
        raise e

# Define the task
process_task = PythonOperator(
    task_id='process_csv_to_snowflake',
    python_callable=process_csv_data,
    dag=dag,
)

# Alternative approach using BashOperator if Python path issues occur
def run_python_script():
    """Alternative method using bash command"""
    return """
    cd /opt/airflow/python && \
    python app.py
    """

bash_task = BashOperator(
    task_id='run_csv_processing_bash',
    bash_command=run_python_script(),
    dag=dag,
)

# Set task dependencies (you can choose which approach to use)
# Option 1: Use PythonOperator
# process_task

# Option 2: Use BashOperator  
bash_task 