"""Run each isolated scenario against the actual CDC stack; no synthetic bypass except replay."""
import argparse,json,time,subprocess
from datetime import datetime,timedelta,timezone
from pathlib import Path
from common import pg,ch
NAMES=['late-fact','cdc-delay','missing-dimension','out-of-order','late-scd2','dbt-delay','replay','late-delete']
ROOT=Path(__file__).resolve().parents[1]

def put(kind,id,revision,data):
    with pg() as conn:
        conn.execute("INSERT INTO entities VALUES (%s,%s,%s,%s::jsonb) ON CONFLICT(kind,id) DO UPDATE SET revision=excluded.revision,data=excluded.data",(kind,id,revision,json.dumps(data)))

def wait(id,revision=1,op=None):
    deadline=time.monotonic()+90
    while time.monotonic()<deadline:
        q="SELECT count() FROM bronze_events WHERE id={id:String} AND revision>={v:UInt64}"
        if op:q+=" AND op={op:String}"
        if ch().query(q,parameters={'id':id,'v':revision,'op':op or ''}).first_row[0]:return
        time.sleep(.5)
    raise TimeoutError(f'CDC timeout: {id}, revision {revision}; inspect connect/worker logs')

def build():
    subprocess.run(['dbt','build','--project-dir',str(ROOT/'dbt'),'--profiles-dir',str(ROOT/'dbt')],check=True)

def hold(value): (ROOT/'control/mode').write_text(value)

def ids(name):return name+'-customer',name+'-order',name+'-history'

def seed(name):
    c,o,h=ids(name)
    if ch().query('SELECT count() FROM bronze_events WHERE id={id:String}',parameters={'id':o}).first_row[0]:
        raise RuntimeError('Scenario already exists. Use ./bin/lab reset before rerunning this case.')
    old=(datetime.now(timezone.utc)-timedelta(days=7)).isoformat()
    if name!='missing-dimension':
        put('customer',c,1,{'name':'Alice'})
        put('customer_history',h,1,{'customer_id':c,'segment':'standard','effective_from':'2020-01-01T00:00:00Z'})
        wait(c);wait(h)
    if name!='late-fact':
        put('order',o,1,{'customer_id':c,'amount':100,'event_time':old,'status':'paid'})
        wait(o)
    build()

def inject(name):
    c,o,h=ids(name); old=(datetime.now(timezone.utc)-timedelta(days=7)).isoformat()
    def order(v,amount=100,status='paid'):
        put('order',o,v,{'customer_id':c,'amount':amount,'event_time':old,'status':status})
    if name=='late-fact':order(1);wait(o)
    elif name in ('cdc-delay','out-of-order'):
        hold('hold');time.sleep(2)
        order(2,120)
        if name=='out-of-order':order(3,150)
        # Wait until the consumer has actually buffered the expected messages.
        import requests
        deadline=time.monotonic()+90;expected=2 if name=='out-of-order' else 1
        while time.monotonic()<deadline:
            text=requests.get('http://worker:9101/metrics',timeout=3).text
            line=next((x for x in text.splitlines() if x.startswith('lab_held_events ')), 'lab_held_events 0')
            if float(line.split()[1])>=expected:break
            time.sleep(.5)
        else:raise TimeoutError('Consumer did not buffer changes')
        print('Held events visible in Grafana; Gold still shows 100. Run release, then build.')
    elif name=='missing-dimension':
        put('customer',c,1,{'name':'Alice'})
        put('customer_history',h,1,{'customer_id':c,'segment':'standard','effective_from':'2020-01-01T00:00:00Z'});wait(c);wait(h)
    elif name=='late-scd2':
        put('customer_history',h+'-backdated',1,{'customer_id':c,'segment':'premium','effective_from':'2021-01-01T00:00:00Z'});wait(h+'-backdated')
    elif name=='dbt-delay':order(2,120);wait(o,2)
    elif name=='replay':
        ch().command('INSERT INTO bronze_events SELECT * FROM bronze_events WHERE id={id:String}',parameters={'id':o})
    elif name=='late-delete':
        order(2,status='cancelled');wait(o,2);build()
        with pg() as conn:conn.execute('DELETE FROM entities WHERE kind=%s AND id=%s',('order',o))
        wait(o,2,'d')

def release(name):
    _,o,_=ids(name);hold('release-reverse' if name=='out-of-order' else 'release')
    wait(o,3 if name=='out-of-order' else 2)
    deadline=time.monotonic()+30
    while (ROOT/'control/mode').read_text().strip() and time.monotonic()<deadline:time.sleep(.2)

def inspect(name):
    c,o,h=ids(name)
    for q in ["SELECT kind,id,revision,op,offset_id,ingested_at FROM bronze_events WHERE id={id:String} ORDER BY ingested_at", "SELECT order_id,amount,customer_known,segment,status,deleted FROM gold_orders WHERE order_id={id:String}"]:
        print(q);print(ch().query(q,parameters={'id':o}).result_rows)

def verify(name):
    _,o,_=ids(name)
    rows=ch().query('SELECT amount,customer_known,segment,status,deleted FROM gold_orders WHERE order_id={id:String}',parameters={'id':o}).result_rows
    amount=150 if name=='out-of-order' else 120 if name in ('cdc-delay','dbt-delay') else 100
    segment='premium' if name=='late-scd2' else 'standard'
    expected=(float(amount),1,segment,'cancelled' if name=='late-delete' else 'paid',1 if name=='late-delete' else 0)
    assert len(rows)==1 and tuple(rows[0])==expected,(name,rows,expected)
    if name=='late-fact':
        assert ch().query('SELECT count() FROM naive_recent_orders WHERE order_id={id:String}',parameters={'id':o}).first_row[0]==0
    print('PASS',name,rows)
    return rows

if __name__=='__main__':
    a=argparse.ArgumentParser();a.add_argument('action',choices=['seed','inject','release','inspect','verify','demo']);a.add_argument('name',choices=NAMES);x=a.parse_args()
    if x.action=='demo':
        seed(x.name);inspect(x.name);inject(x.name)
        if x.name in ('cdc-delay','out-of-order'):release(x.name)
        inspect(x.name);build();first=verify(x.name);build();assert first==verify(x.name)
    else:globals()[x.action](x.name)
