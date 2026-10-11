from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
with DAG('medallion',start_date=datetime(2025,1,1),schedule='*/5 * * * *',catchup=False,max_active_runs=1,default_args={'retries':1,'retry_delay':timedelta(seconds=15)}) as dag:
    build=BashOperator(task_id='dbt_build',bash_command='cd /opt/lab/dbt && /opt/dbt-venv/bin/dbt build --profiles-dir .')
