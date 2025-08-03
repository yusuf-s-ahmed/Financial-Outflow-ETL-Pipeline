# Pre-Operating Expenses Data Pipeline Infrastructure

This project implements a data pipeline infrastructure using Terraform, AWS, and Snowflake to process and analyse pre-operating expenses financial data. The solution provides automated infrastructure provisioning, data processing with PySpark, and cloud-based data warehousing, Snowflake to extract financial analytics using SQL.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Technology Stack](#technology-stack)
- [Implementation Details](#implementation-details)
- [Key Insights](#key-insights)
- [Challenges and Solutions](#challenges-and-solutions)
- [Impact](#impact)

## Overview

This project establishes a complete data infrastructure for processing pre-operating expenses data using Infrastructure as Code (IaC) principles. The solution automates the provisioning of AWS S3 storage, processes CSV data using PySpark, and loads structured data into Snowflake for advanced analytics and reporting.

The pipeline handles financial data from CSV files, transforms it into normalised database tables (expenses, categories, vendors), and provides SQL-based analytics capabilities. This enables organisations to track, analyse, and optimise their pre-operating expenses with automated data processing and cloud-based storage.

## Features

- **Infrastructure as Code**: Automated AWS S3 bucket provisioning using Terraform
- **Data Processing**: PySpark-based ETL pipeline for CSV data transformation
- **Cloud Storage**: Secure S3 bucket for raw data storage with automated file uploads
- **Data Warehousing**: Snowflake integration for structured data storage and analytics
- **Multi-Provider Support**: AWS and Databricks provider configurations
- **Environment Management**: Virtual environment setup with dependency management
- **SQL Analytics**: Pre-built queries for expense analysis and reporting

## Technology Stack

| Component            | Tools and Libraries                       |
|---------------------|--------------------------------------------|
| Infrastructure       | Terraform, AWS S3, Databricks             |
| Data Processing      | PySpark, Pandas, Python             |
| Cloud Services       | AWS S3, Snowflake                         |
| Data Storage         | CSV files, Snowflake Data Warehouse       |
| Development          | Python virtual environment, requirements.txt |
| Version Control      | Git                                        |

## Implementation Details

- **Terraform Configuration**: 
  - AWS S3 bucket creation for data storage
  - Automated CSV file upload to S3 with MD5 validation
  - Multi-provider setup for AWS and Databricks integration
  
- **Data Processing Pipeline**:
  - PySpark session initialization with custom Python environment
  - CSV data extraction from S3 using boto3
  - Data transformation into three normalised tables: expenses, categories, vendors
  - Automated data type conversion and column mapping
  
- **Database Schema**:
  - Snowflake warehouse and database creation
  - Normalised schema design for expense tracking
  - Pre-built SQL queries for expense analysis and reporting
  
- **Environment Management**:
  - Virtual environment with isolated dependencies
  - Environment variable configuration for AWS credentials
  - Cross-platform compatibility (Windows/Linux)

## Key Insights

| Component                    | Implementation Details                                    |
|------------------------------|-----------------------------------------------------------|
| S3 Data Storage              | Automated bucket provisioning with file upload validation |
| PySpark Processing           | 3.2.1 version with custom Python environment configuration |
| Data Normalization           | Three-table schema: expenses, categories, vendors         |
| Snowflake Integration        | XSMALL warehouse with auto-suspend/resume capabilities    |
| Infrastructure Automation    | Complete IaC setup with Terraform state management        |

## Challenges and Solutions

| Challenge                         | Solution                                                       |
|----------------------------------|----------------------------------------------------------------|
| Cross-platform Python paths       | Used absolute paths and environment variable configuration    |
| PySpark environment setup         | Configured PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON variables |
| AWS credential management         | Implemented dotenv for secure credential handling             |
| Data type consistency             | Applied explicit type conversions for dates and numeric fields |
| Infrastructure state management   | Used Terraform state files for deployment tracking            |

## Impact

This project demonstrates modern data engineering practices by combining Infrastructure as Code with cloud-native data processing. It provides a scalable foundation for financial data analytics, enabling organisations to:

- Automate infrastructure provisioning and reduce manual setup time
- Process large volumes of financial data efficiently with PySpark
- Maintain data integrity through normalized database design
- Enable real-time analytics with cloud-based data warehousing
- Scale processing capabilities based on business needs

The solution reduces operational overhead while providing a robust platform for financial data analysis and reporting.