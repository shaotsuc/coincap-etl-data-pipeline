import os, requests, pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime




## Env variables
POSTGRES_USER=os.environ['POSTGRES_USER']
POSTGRES_PASSWORD=os.environ['POSTGRES_PASSWORD']
DB_NAME=os.environ['DB_NAME']


## Variables
DATA_URL = 'https://api.coincap.io/v2/assets?limit=100'
TODAY = datetime.today().date()


## Get data from API source and format to parquet file
def get_format_data(data_url):
    result = requests.get(data_url).json()
    data = result['data']
    raw_data = pd.json_normalize(data)
    raw_data.to_parquet(f'./src/{TODAY}_raw_data.parquet', index=False) 


## connect to PostgreSQL in docker
def get_db_engine():
    return create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:5432/{DB_NAME}')

db_engine = get_db_engine().connect()


# create table schema
def create_schema(schema: str):
    create_schema = text(f"""
                    CREATE SCHEMA IF NOT EXISTS {schema};
                    """)
    db_engine.execute(create_schema)
    print(f'Schema:{schema} created')


# create table within the schema
def create_table(schema: str, table_name: str):
    create_table = text (f"""
                        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
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
    db_engine.execute(create_table)
    print(f'{schema}.{table_name} created')


## Get ready to load into Local PostgreSQL
