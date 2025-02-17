import os, requests, pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta


## Env variables
POSTGRES_USER=os.environ['POSTGRES_USER']
POSTGRES_PASSWORD=os.environ['POSTGRES_PASSWORD']
DB_NAME=os.environ['DB_NAME']


## Variables
DATA_URL = 'https://api.coincap.io/v2/assets?limit=100'
DB_URL = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:5432/{DB_NAME}'
TODAY = datetime.today().date()

schema_name = 'coincap_raw'
table_name = 'raw_asset'
source_file_path = f'./src/{TODAY}_raw_data.parquet'


## Get data from API source and format to parquet file
def get_format_data(data_url):
    """
    1. To get data from API source. 
    2. To format it to parquet file.
    3. To save in destinated source file path as temporary file.
    """
    result = requests.get(data_url).json()
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
def load_in_DB():
    with db_connection as conn:
        df = pd.read_parquet(source_file_path)
        df.to_sql(table_name, conn, schema=schema_name, if_exists='replace', index=False)

        print(f'Successfully loaded data in {schema_name}.{table_name}')



## airflow DAGs default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 17),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}