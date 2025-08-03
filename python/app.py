# ---------------------------------    Packages & Configuration      ---------------------------------- # 

import os
os.environ["PYSPARK_PYTHON"] = "C:/Users/yusuf/Desktop/terraform-setup/venv/Scripts/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/Users/yusuf/Desktop/terraform-setup/venv/Scripts/python.exe"
import pandas as pd
import datetime
from io import StringIO
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv
import pyspark
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector



print("PySpark version:", pyspark.__version__)

load_dotenv()

# Set AWS credentials
AWS_ACCESS_KEY_ID= os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY= os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION= os.getenv('AWS_REGION')
AWS_S3_BUCKET= os.getenv('AWS_S3_BUCKET')
FILE_KEY = 'Largence CF to 31-07-2025.csv'

# Create S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Download CSV from S3 as string
csv_obj = s3.get_object(Bucket=AWS_S3_BUCKET, Key=FILE_KEY)
body = csv_obj['Body'].read().decode('utf-8')










# ---------------------------------    Creating DataFrame1      ---------------------------------- # 

# Load CSV into pandas DataFrame
df_1 = pd.read_csv(StringIO(body))

selected_columns = ["Date of Expense", "Cost"]
df_1 = df_1[selected_columns]

df_1.insert(0, "expense_id", range(1, len(df_1) + 1))
df_1.insert(3, "category_id", range(1, len(df_1) + 1))
df_1.insert(4, "vendor_id", range(1, len(df_1) + 1))

df_1.rename(columns={"Date of Expense": "date_of_expense",
                   "Cost": "cost"},
                   inplace=True)

df_1['date_of_expense'] = pd.to_datetime(df_1['date_of_expense'], format='%d/%m/%Y').dt.date
df_1['cost'] = df_1['cost'].apply(float)

# Debug: Print column names and first few rows
print("Columns:", df_1.columns.tolist())
print("Data types:\n", df_1.dtypes)
print("First 5 rows:\n", df_1.head())


# ---------------------------------    Creating DataFrame2      ---------------------------------- # 



# Load CSV into pandas DataFrame
df_2 = pd.read_csv(StringIO(body))

selected_columns = ["Category", "Pre Operating Expense"]
df_2 = df_2[selected_columns]

df_2.insert(0, "category_id", range(1, len(df_1) + 1))

df_2.rename(columns={"Category": "category_type",
                   "Pre Operating Expense": "expense_name"},
                   inplace=True)

# Debug: Print column names and first few rows
print("Columns:", df_2.columns.tolist())
print("Data types:\n", df_2.dtypes)
print("First 5 rows:\n", df_2.head())



# ---------------------------------    Creating DataFrame3      ---------------------------------- # 


# Load CSV into pandas DataFrame
df_3 = pd.read_csv(StringIO(body))

selected_columns = ["Vendor", "Contact Info"]
df_3 = df_3[selected_columns]

df_3.insert(0, "vendor_id", range(1, len(df_3) + 1))

df_3.rename(columns={"Vendor": "vendor_name",
                   "Contact Info": "vendor_contact_information"},
                   inplace=True)

# Debug: Print column names and first few rows
print("Columns:", df_3.columns.tolist())
print("Data types:\n", df_3.dtypes)
print("First 5 rows:\n", df_3.head())











# ----------------------------    Apache Spark Session Configuration    ----------------------------- # 

# Create SparkSession
spark = SparkSession.builder\
    .master("local")\
    .appName("S3ToSpark")\
    .config('spark.ui.port', '4050')\
    .getOrCreate()

# -----------------------------------------    Schema    ------------------------------------------ # 

schema_df_1 = StructType([
    StructField("EXPENSE_ID", IntegerType()),
    StructField("DATE_OF_EXPENSE", DateType()),
    StructField("COST", FloatType()),
    StructField("CATEGORY_ID", IntegerType()),
    StructField("VENDOR_ID", IntegerType()),
])


schema_df_2 = StructType([
    StructField("CATEGORY_ID", IntegerType()),
    StructField("CATEGORY_TYPE", StringType()),
    StructField("EXPENSE_NAME", StringType()),
])

schema_df_3 = StructType([
    StructField("VENDOR_ID", IntegerType()),
    StructField("VENDOR_NAME", StringType()),
    StructField("VENDOR_CONTACT_INFORMATION", StringType()),
])


# ---------------------------------    Display All Tables    ---------------------------------- #



print("Showing all rows and columns in df_1:")
spark_df_1 = spark.createDataFrame(df_1, schema=schema_df_1)
spark_df_1.show()

print("Showing all rows and columns in df_2:")
spark_df_2 = spark.createDataFrame(df_2, schema=schema_df_2)
spark_df_2.show()

print("Showing all rows and columns in df_3:")
spark_df_3 = spark.createDataFrame(df_3, schema=schema_df_3)
spark_df_3.show()


conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database='PRE_OPERATING_EXPENSES',
    schema=os.getenv('SNOWFLAKE_SCHEMA').upper()
)

cur = conn.cursor()


cur.execute("""
CREATE OR REPLACE TABLE pre_operating_expenses.expense_data.expenses (
    expense_id INTEGER,
    date_of_expense DATE,
    cost FLOAT,
    category_id INTEGER,
    vendor_id INTEGER
)
""")

cur.execute("""
CREATE OR REPLACE TABLE pre_operating_expenses.expense_data.categories (
    category_id INTEGER,
    category_type STRING,
    expense_name STRING
)
""")

cur.execute("""
CREATE OR REPLACE TABLE pre_operating_expenses.expense_data.vendors (
    vendor_id INTEGER,
    vendor_name STRING,
    vendor_contact_information STRING
)
""")


conn.commit()
cur.close()

# Now write data to the properly structured tables
write_pandas(
    conn,
    spark_df_1.toPandas(),
    table_name='EXPENSES',
    database='PRE_OPERATING_EXPENSES',
    schema=os.getenv('SNOWFLAKE_SCHEMA').upper(),
    auto_create_table=False
)

write_pandas(
    conn,
    spark_df_2.toPandas(),
    table_name='CATEGORIES',
    database='PRE_OPERATING_EXPENSES',
    schema=os.getenv('SNOWFLAKE_SCHEMA').upper(),
    auto_create_table=False
)

write_pandas(
    conn,
    spark_df_3.toPandas(),
    table_name='VENDORS',
    database='PRE_OPERATING_EXPENSES',
    schema=os.getenv('SNOWFLAKE_SCHEMA').upper(),
    auto_create_table=False
)

print("Data uploaded successfully!")

conn.close()


