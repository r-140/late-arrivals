import os
from pathlib import Path
import clickhouse_connect,psycopg
ROOT=Path(os.getenv('LAB_DATA','/data'))
def pg():return psycopg.connect(os.getenv('PG_DSN','host=postgres dbname=lab user=lab password=lab'))
def ch():return clickhouse_connect.get_client(host=os.getenv('CH_HOST','clickhouse'),username='lab',password='lab',database='lab')
