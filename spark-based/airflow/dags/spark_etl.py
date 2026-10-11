import json,time,urllib.request
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def execute():
    with urllib.request.urlopen(urllib.request.Request('http://worker:8090/run',data=b'{}',headers={'Content-Type':'application/json'},method='POST'),timeout=10) as r:job=json.load(r)['job_id']
    deadline=time.monotonic()+1800
    while time.monotonic()<deadline:
        with urllib.request.urlopen('http://worker:8090/jobs/'+job,timeout=10) as r:result=json.load(r)
        if result['status'] in ('success','failed'):
            print(result.get('output',''))
            if result['status']=='failed':raise RuntimeError('Spark worker job failed')
            return
        time.sleep(5)
    raise TimeoutError('Spark ETL job deadline exceeded; inspect worker before retrying')

with DAG('spark_etl',start_date=datetime(2025,1,1),schedule='0 * * * *',catchup=False,max_active_runs=1,default_args={'retries':1,'retry_delay':timedelta(seconds=30)}) as dag:
    PythonOperator(task_id='extract_transform_load',python_callable=execute,execution_timeout=timedelta(minutes=35))
