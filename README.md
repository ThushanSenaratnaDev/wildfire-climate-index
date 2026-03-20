# Global Wildfire & Climate Data Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7+-green.svg)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue.svg)
![AWS S3](https://img.shields.io/badge/AWS-S3-orange.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Data%20Warehouse-blue.svg)

## Project Overview
An automated, containerized **ELT (Extract, Load, Transform)** pipeline designed to analyze the correlation between global temperature anomalies and wildfire intensity over the past 24+ years. 

This project orchestrates the extraction of raw data from the NASA FIRMS API, stores it in a highly available AWS S3 Data Lake, and models the data into a PostgreSQL Data Warehouse. The entire workflow is scheduled and monitored using Apache Airflow.

## Architecture & Data Flow
1. **Extract:** Python scripts fetch historical climate data and 2000-2023 global wildfire data from NASA APIs.
2. **Data Lake (Bronze Layer):** Raw CSV/JSON files are temporarily staged locally before an idempotent Python job synchronizes them to an **AWS S3 Bucket**.
3. **Data Warehouse (Gold Layer):** Data is loaded into a **PostgreSQL** database.
4. **Transform:** SQL transformations aggregate fire frequency, maximum intensity (MW), and average temperatures by year.
5. **Visualize:** Generates dual-axis correlation charts (Temperature Anomaly vs. Total Fires) using Matplotlib and Seaborn.
6. **Orchestration:** **Apache Airflow** manages dependencies, retries, and scheduling inside a **Docker** environment.

## Technology Stack
* **Language:** Python 3
* **Orchestration:** Apache Airflow
* **Containerization:** Docker & Docker Compose
* **Cloud Storage:** Amazon Web Services (AWS S3, Boto3)
* **Database:** PostgreSQL
* **Data Processing:** Pandas, SQL
* **Visualization:** Matplotlib, Seaborn

## Repository Structure
```text
wildfire-climate-index/
├── dags/
│   └── wildfire_pipeline.py       # Airflow DAG definition
├── scripts/
│   ├── fetch_temperature.py       # API extraction for climate data
│   ├── fetch_fire_raw.py          # API extraction for NASA fire data
│   ├── upload_to_s3.py            # Idempotent AWS S3 sync script
│   ├── load_to_postgres.py        # Database ingestion script
│   ├── transform_data.py          # SQL aggregations (Gold Layer)
│   └── visualize_trends.py        # Trend analysis chart generator
├── data/                          # Local volume mapping for Bronze/Gold staging
├── docker-compose.yaml            # Container orchestration config
├── requirements.txt               # Python dependencies
└── .env                           # Environment variables & API Keys (Git-ignored)

## Setup
NASA_API_KEY=your_nasa_api_key
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=your-unique-s3-bucket-name

docker-compose build
docker-compose up -d
