import os, requests, pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


## ENV VAR
POSTGRES_USER=os.environ['POSTGRES_USER']
POSTGRES_PASSWORD=os.environ['POSTGRES_PASSWORD']
DB_NAME=os.environ['DB_NAME']

## Variables
DATA_URL = 'https://api.coincap.io/v2/assets?limit=100'
DB_URL = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgres:5432/{DB_NAME}'  
TODAY = datetime.today().date()

schema_name = 'coincap_raw'
table_name = 'raw_asset'
source_file_path = f'./data/asset_data_{TODAY}.parquet'


## Get data from API source and format to parquet file
def get_format_data():
    """
    1. To get data from API source. 
    2. To format it to parquet file.
    3. To save in destinated source file path as temporary file.
    """
    result = requests.get(DATA_URL).json()
    data = result['data']
    df = pd.json_normalize(data)
    df.to_parquet(source_file_path, index=False) 


## connect to PostgreSQL in docker
def get_db_engine():
    return create_engine(DB_URL)

engine = get_db_engine()
db_connection = engine.connect()


## create table schema
def create_schema(schema: str):
    """
    To create scehma in PostgreSQL.
    Parameter input as schema name.
    """
    create_schema = text(f"""
                    CREATE SCHEMA IF NOT EXISTS {schema};
                    """)
                    
    db_connection.execute(create_schema)
    print(f'Schema:{schema} created')


## create table within the schema
def create_table(schema: str, table: str):
    """
    To create table within the schema.
    Parameters input as schema name and table name.
    """
    create_table = text (f"""
                        CREATE TABLE IF NOT EXISTS {schema}.{table} (
                        id TEXT,
                        rank TEXT,
                        symbol TEXT,	 
                        name TEXT,
                        supply FLOAT,	
                        maxSupply FLOAT,	
                        marketCapUsd FLOAT,	
                        volumeUsd24Hr FLOAT,	
                        priceUsd FLOAT,		
                        changePercent24Hr FLOAT,	
                        vwap24Hr FLOAT,	
                        explorer TEXT);
                        """)
    db_connection.execute(create_table)
    print(f'{schema}.{table} created')


## Load Parquet into raw schema
def load_in_DB(source_file_path, schema, table):
    with db_connection as conn:
        df = pd.read_parquet(source_file_path)
        df.to_sql(table, conn, schema=schema, if_exists='replace', index=False)

        print(f'Successfully loaded data in {schema}.{table}')


## remove data after loading successfully
def remove_source_file():
    """
    To remove source files after the completion of data pipeline.
    """
    try:
        os.remove(source_file_path)
    except FileNotFoundError:
        print('No file was deleted.')




## airflow DAGs default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 17),
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}


## DAGs declaration
with DAG(
    dag_id='data_ingestion_dag',
    schedule='*/5 * * * *',  # runs every 5 minutes 
    default_args=default_args
    ) as dag:


## 1. get the data
    get_dataset_task = PythonOperator(
        task_id='get_dataset',
        python_callable=get_format_data,
        op_kwargs={
            'data_url': DATA_URL,
            'source_file_path': source_file_path
        }
    )


## 2. create schema 
    checking_schema_task = PythonOperator(
        task_id='checking_schema',
        python_callable=create_schema,
        op_kwargs={
            'schema': schema_name
        }
    )


## 3. create table
    checking_table_task = PythonOperator(
        task_id='checking_table',
        python_callable=create_table,
        op_kwargs={
            'schema': schema_name,
            'table': table_name
        }
    )


## 3. load data
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_in_DB,
        op_kwargs={
            'source_file_path': source_file_path,
            'schema': schema_name,
            'table': table_name
        }
    )


## 4. remove data
    remove_file_task = PythonOperator(
        task_id='remove_file',
        python_callable=remove_source_file,
    )


get_dataset_task >> checking_schema_task >> checking_table_task >> load_data_task >> remove_file_task

