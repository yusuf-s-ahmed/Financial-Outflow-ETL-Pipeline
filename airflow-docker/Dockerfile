FROM apache/airflow:3.0.3

# Install PySpark and other required packages
RUN pip install --no-cache-dir \
    pyspark==3.2.1 \
    pandas \
    boto3 \
    snowflake-connector-python \
    python-dotenv

# Set environment variables for PySpark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3 