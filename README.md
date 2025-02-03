# Coincap End to End Data Pipeline

## Introduction
The goal of this project is to develop an automated data pipeline that collects cryptocurrency data from the CoinCap public API, performs extraction, transformation, and loading (ETL) processes, and prepares the data for analysis. This pipeline will allow the creation of a dashboard that provides end-users with actionable insights and valuable information.

## Dataset
The dataset is sourced from the CoinCap API, which offers comprehensive data on cryptocurrency market activity.

## Tech Stack Used 
My goal is to leverage open-source tools and technologies to develop this project. The key components and technologies I plan to use include:

Programming Languages:
+ Python
+ SQL

Containerization:
+ Docker
+ Docker Compose

Data Warehouse:
+ PostgreSQL

Data Transformation:
+ dbt (data build tool)

Data Visualization:
+ Metabase

Data Pipeline Orchestration:
+ Apache Airflow

## Data Pipeline
![data-pipeline](/media/coincap_data_pipeline.gif)

### Thought Process

#### 1. Data Extraction
Every 5 minutes, data is retrieved from the CoinCap API and converted from JSON to Parquet format.

#### 2. Data Loading 
The converted data is loaded into the raw schema of PostgreSQL.

#### 3. Data Transformation
Using dbt, the source and data lineage are established to perform data transformation. The transformed data is then loaded into separate data models, such as source, staging, and data mart (dm) layers within PostgreSQL.

#### 4. Data Visualization
Metabase connects to the PostgreSQL data mart (dm) layer to access the dataset, which is now ready for analysis and visualization.

#### * Data Pipeline Orchestration *
Apache Airflow orchestrates the entire workflow, from data extraction to transformation, ensuring the data remains up-to-date.
+ The ETL process runs every 5 minutes
+ The dbt transformation process runs every 10 minutes

## Data Visualization
![crypto-analysis-dashboard](/media/metabase-crypto-analysis-dashboard.png)

