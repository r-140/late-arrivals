"""Insert before committing Kafka offsets. Replay is deliberately safe downstream."""
import json, time, threading
from pathlib import Path
from confluent_kafka import Consumer
from prometheus_client import start_http_server, Counter, Gauge, Histogram
from common import ch
from envelope import normalize
received=Counter('lab_cdc_events','Delivered events')
held=Gauge('lab_held_events','Events held in memory for controlled demo')
lag=Gauge('lab_kafka_lag','Partition high watermark minus committed position')
age=Histogram('lab_delivery_seconds','Source change to Bronze delay',buckets=(1,5,10,30,60,120,300,3600))
last=Gauge('lab_last_delivery_timestamp','Last successful Bronze insert')
dbt_ok=Gauge('lab_dbt_last_run_success','1 if every result in latest dbt invocation succeeded')
dbt_duration=Gauge('lab_dbt_last_run_seconds','Latest dbt invocation elapsed seconds')
dbt_time=Gauge('lab_dbt_last_run_timestamp','Latest dbt invocation completion')
def artifacts():
    from datetime import datetime
    while True:
        try:
            result=json.loads(Path('/app/dbt/target/run_results.json').read_text())
            dbt_ok.set(all(x['status'] in ('success','pass','warn') for x in result['results']))
            dbt_duration.set(result['elapsed_time'])
            dbt_time.set(datetime.fromisoformat(result['metadata']['generated_at'].replace('Z','+00:00')).timestamp())
        except (FileNotFoundError,ValueError,KeyError): pass
        time.sleep(5)
threading.Thread(target=artifacts,daemon=True).start()
start_http_server(9101)
c=Consumer({'bootstrap.servers':'kafka:9092','group.id':'bronze-v1','auto.offset.reset':'earliest','enable.auto.commit':False})
c.subscribe(['lab.public.entities'])
buffer=[]
client=None

def mode():
    try: return Path('/app/control/mode').read_text().strip()
    except FileNotFoundError: return ''

def insert(msg):
    global client
    if client is None: client=ch()
    row=normalize(msg.value(),msg.topic(),msg.partition(),msg.offset())
    if row is None: return
    client.insert('bronze_events',[row],column_names=['event_id','kind','id','revision','op','data','source_ms','source_lsn','topic','partition_id','offset_id'])
    received.inc(); last.set(time.time())
    if row[6]: age.observe(max(0,time.time()-row[6]/1000))

while True:
    try:
        m=c.poll(1)
        if m and m.error(): raise RuntimeError(m.error())
        if m and m.value():
            # Once a hold starts, hold every later record to retain commit safety.
            if mode() in ('hold','reverse') or buffer:
                buffer.append(m)
            else:
                insert(m); c.commit(message=m,asynchronous=False)
        held.set(len(buffer))
        if buffer and mode() in ('release','release-reverse'):
            ordered=list(reversed(buffer)) if mode()=='release-reverse' else buffer
            for event in ordered: insert(event)
            # Highest original offset, never the last reverse-delivered offset.
            c.commit(message=buffer[-1],asynchronous=False)
            buffer.clear(); held.set(0); Path('/app/control/mode').write_text('')
        for tp in c.assignment():
            _,high=c.get_watermark_offsets(tp,timeout=2)
            committed=c.committed([tp],timeout=2)[0].offset
            lag.set(max(0,high-max(0,committed)))
    except Exception as exc:
        print(f'Retrying: {exc}',flush=True)
        client=None; time.sleep(2)
        # On failure uncommitted records are replayed; restart to reset fetch position.
        raise
