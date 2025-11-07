# Retail-Sales-Pipeline-using-PySpark-and-Airflow
A data engineering pipeline that simulates a retail sales workflow — from raw data ingestion to transformation and loading into a Postgres warehouse.
This project demonstrates an end-to-end ETL orchestration using Apache Airflow and PySpark inside a fully containerized environment with Docker.

## Project Overview
The goal of this project is to design a scalable data pipeline that:
1. Extracts daily retail sales data (CSV format).
2. Transforms it using PySpark for data cleaning and aggregation.
3. Loads the processed data into a Postgres data warehouse.
4. Orchestrates all steps using Airflow with retries, notifications, and parallel execution.
