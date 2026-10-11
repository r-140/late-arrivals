import fcntl,json,time
from psycopg.types.json import Jsonb
from common import ROOT,pg
from extract import extract
from transform import session,transform
from load import load

def run(fail=''):
    ROOT.mkdir(parents=True,exist_ok=True)
    with (ROOT/'pipeline.lock').open('w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        # autocommit prevents accidental long source transactions between stages.
        with pg() as conn:
            conn.autocommit=True
            t=time.monotonic();batch=extract(conn)
            if batch is None:print('No unclaimed source changes');return None
            conn.execute('UPDATE etl_batches SET extract_seconds=%s,error=NULL WHERE batch_no=%s',(time.monotonic()-t,batch))
            spark=None
            try:
                if fail=='after-extract':raise RuntimeError('Injected failure after durable JSON extraction')
                spark=session();spark.sparkContext.setLogLevel('WARN')
                status,manifest=conn.execute('SELECT status,delta_manifest FROM etl_batches WHERE batch_no=%s',(batch,)).fetchone()
                if status!='transformed':
                    t=time.monotonic();manifest=transform(spark,batch)
                    conn.execute("UPDATE etl_batches SET status='transformed',delta_manifest=%s,transform_seconds=%s WHERE batch_no=%s",(Jsonb(manifest),time.monotonic()-t,batch))
                if fail=='after-transform':raise RuntimeError('Injected failure after Delta manifest persisted')
                t=time.monotonic();load(spark,conn,batch,manifest,fail)
                conn.execute('UPDATE etl_batches SET load_seconds=%s WHERE batch_no=%s',(time.monotonic()-t,batch))
                print('Loaded batch',batch,manifest);return batch
            except Exception as e:
                conn.execute('UPDATE etl_batches SET error=%s WHERE batch_no=%s',(str(e),batch));raise
            finally:
                if spark:spark.stop()
