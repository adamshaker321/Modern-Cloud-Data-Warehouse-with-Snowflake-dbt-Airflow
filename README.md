# Modern-Cloud-Data-Warehouse-with-Snowflake-dbt-Airflow

<img width="1660" height="948" alt="Project Architecture" src="https://github.com/user-attachments/assets/b651227d-8e37-4705-8c12-bdb695b440b9" />


# Modern Cloud Data Warehouse with Snowflake, dbt & Apache Airflow

An end-to-end cloud data warehouse project that automates data ingestion, transformation, orchestration, and reporting using AWS S3, Snowflake, dbt, Apache Airflow, and Power BI.

## Overview

This project implements a modern ELT pipeline that ingests raw CSV files from AWS S3, transforms the data through Bronze, Silver, and Gold layers in Snowflake using dbt, orchestrates the workflow with Apache Airflow, and delivers interactive dashboards in Power BI.

## Tech Stack

- AWS S3
- Snowflake
- dbt
- Apache Airflow
- SQL
- Power BI

## Architecture

```
AWS S3
   │
   ▼
Bronze Layer
   │
   ▼
Silver Layer
   │
   ▼
Gold Layer (Galaxy Schema)
   │
   ▼
Power BI
```

## Data Model

### Dimension Tables
- Dim_Customers
- Dim_Products
- Dim_Stores
- Dim_Staffs
- Dim_Date

### Fact Tables
- Fact_Sales
- Fact_Inventory

## Pipeline Workflow

- Detect new files in AWS S3
- Load data into Snowflake Bronze
- Validate loaded data
- Execute dbt Silver models
- Execute dbt Gold models
- Run dbt tests
- Send email notification
- Refresh Power BI dashboard

## Features

- Automated ELT Pipeline
- Bronze, Silver, and Gold Architecture
- Galaxy Schema
- Data Quality Validation
- Workflow Orchestration with Airflow
- Interactive Power BI Dashboard
- Direct Snowflake Connection

## Project Structure

```
.
├── dags/
├── dbt/
├── include/
├── dashboards/
├── images/
└── README.md
```

## Screenshots

- Architecture
- Airflow DAG
- dbt Lineage
- Power BI Dashboard
