import clickhouse_connect
import psycopg

def ch():
    return clickhouse_connect.get_client(host='clickhouse', username='lab', password='lab', database='lab')

def pg():
    return psycopg.connect('host=postgres dbname=lab user=lab password=lab')
