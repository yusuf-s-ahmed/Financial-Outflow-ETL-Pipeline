FROM apache/airflow:3.0.3-python3.10

# Switch to root for installing system packages
USER root

# Install Java 11 (Temurin)
RUN apt-get update && \
    apt-get install -y wget gnupg software-properties-common && \
    mkdir -p /etc/apt/keyrings && \
    wget -O- https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor > /etc/apt/keyrings/adoptium.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" | tee /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && \
    apt-get install -y temurin-11-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA env vars
ENV JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV JAVA_OPTS="-Xmx512m -Xms256m"

# Set PySpark environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Optional: Airflow config
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db

# Switch back to airflow user before installing python packages
USER airflow
WORKDIR /opt/airflow

# Install Python dependencies as airflow user
RUN pip install --no-cache-dir \
    pyspark==3.2.1 \
    pandas \
    boto3 \
    snowflake-connector-python \
    python-dotenv
