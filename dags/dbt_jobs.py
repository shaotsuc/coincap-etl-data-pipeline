from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator



#######################
#### One time DAG #####
#######################
onetime_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 9)
}

with DAG(
    dag_id='dbt_setup_dag',
    schedule_interval='@once',
    default_args=onetime_args,
    tags=['dbt','setup']
) as dag:
        
    first_setup_task = BashOperator(
        task_id='first_setup',
        bash_command='pip install dbt-core dbt-postgres'
        )



#######################
### dbt cronjob DAG ###
#######################
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 9),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dbt_cronjob_dag',
    schedule='*/10 * * * *', # runs every 10 minutes 
    default_args=default_args,
    catchup=False,
    tags=['dbt','jobs']
) as dag:
    

    dbt_cronjob_task = BashOperator(
        task_id='dbt_cronjob',
        bash_command='cd /opt/airflow/dbt && ls -a && dbt build --profiles-dir .'
    )



