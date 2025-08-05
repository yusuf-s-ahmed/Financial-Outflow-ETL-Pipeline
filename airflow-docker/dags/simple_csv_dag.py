from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import sys
from io import StringIO
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

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
    'simple_csv_to_snowflake',
    default_args=default_args,
    description='Simple CSV to Snowflake pipeline',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['csv', 'snowflake', 'simple'],
)

def process_csv_data():
    """Process CSV data and upload to Snowflake"""
    print("Starting CSV processing...")
    
    # Load environment variables
    load_dotenv()
    
    # Set AWS credentials
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.getenv('AWS_REGION')
    AWS_S3_BUCKET = os.getenv('AWS_S3_BUCKET')
    FILE_KEY = 'sample_data.csv'
    
    # Create S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    
    # Read CSV from local file instead of S3 for testing
    try:
        # Try to read from S3 first
        csv_obj = s3.get_object(Bucket=AWS_S3_BUCKET, Key=FILE_KEY)
        body = csv_obj['Body'].read().decode('utf-8')
        print("Successfully read CSV from S3")
    except Exception as e:
        print(f"Could not read from S3: {e}")
        print("Reading from local file instead...")
        # Fallback to local file
        with open('/opt/airflow/sample_data.csv', 'r', encoding='utf-8') as file:
            body = file.read()
        print("Successfully read CSV from local file")
    
    # Process the data
    df_1 = pd.read_csv(StringIO(body))
    selected_columns = ["Date of Expense", "Cost"]
    df_1 = df_1[selected_columns]
    df_1.insert(0, "expense_id", range(1, len(df_1) + 1))
    df_1.insert(3, "category_id", range(1, len(df_1) + 1))
    df_1.insert(4, "vendor_id", range(1, len(df_1) + 1))
    df_1.rename(columns={"Date of Expense": "date_of_expense", "Cost": "cost"}, inplace=True)
    df_1['date_of_expense'] = pd.to_datetime(df_1['date_of_expense'], format='%d/%m/%Y').dt.date
    df_1['cost'] = df_1['cost'].apply(float)
    
    print(f"Processed {len(df_1)} records")
    print("First 5 rows:", df_1.head())
    
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database='PRE_OPERATING_EXPENSES_SAMPLE',
        schema=os.getenv('SNOWFLAKE_SCHEMA').upper()
    )
    
    # Create schema if it doesn't exist
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS EXPENSE_DATA")
    
    # Create table
    cur.execute("""
    CREATE OR REPLACE TABLE PRE_OPERATING_EXPENSES_SAMPLE.EXPENSE_DATA.EXPENSES (
        expense_id INTEGER,
        date_of_expense DATE,
        cost FLOAT,
        category_id INTEGER,
        vendor_id INTEGER
    )
    """)
    
    # Upload data
    write_pandas(
        conn,
        df_1,
        table_name='EXPENSES',
        database='PRE_OPERATING_EXPENSES_SAMPLE',
        schema='EXPENSE_DATA',
        auto_create_table=False
    )
    
    print("Data uploaded successfully to Snowflake!")
    conn.close()
    
    return "Success"

# Define the task
process_task = PythonOperator(
    task_id='process_csv_to_snowflake',
    python_callable=process_csv_data,
    dag=dag,
)

# Set task dependencies
process_task 