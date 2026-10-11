import json
from datetime import datetime,timedelta,timezone
from common import pg,ch
from pipeline import run
NAMES=['late-fact','missing-dimension','out-of-order','late-scd2','etl-delay','replay','late-delete','after-extract','after-transform','during-load','after-publish']

def ids(name):return name+'-customer',name+'-order',name+'-history'

def put(kind,id,revision,data):
    with pg() as c:c.execute('INSERT INTO entities VALUES(%s,%s,%s,%s::jsonb) ON CONFLICT(kind,id) DO UPDATE SET revision=excluded.revision,data=excluded.data',(kind,id,revision,json.dumps(data)))

def seed(name):
    c,o,h=ids(name)
    with pg() as conn:
        if conn.execute('SELECT 1 FROM change_queue WHERE id=%s LIMIT 1',(o,)).fetchone():raise RuntimeError('Scenario already seeded. Use inspect/verify, or reset the lab to replay it.')
    if name!='missing-dimension':
        put('customer',c,1,{'name':'Alice'})
        put('customer_history',h,1,{'customer_id':c,'segment':'standard','effective_from':'2020-01-01T00:00:00Z'})
    if name!='late-fact':order(name,1,100)
    run()

def order(name,revision,amount,status='paid'):
    c,o,_=ids(name)
    put('order',o,revision,{'customer_id':c,'amount':amount,'status':status,'event_time':(datetime.now(timezone.utc)-timedelta(days=7)).isoformat()})

def inject(name):
    c,o,h=ids(name)
    if name=='late-fact':order(name,1,100)
    elif name=='missing-dimension':
        put('customer',c,1,{'name':'Alice'})
        put('customer_history',h,1,{'customer_id':c,'segment':'standard','effective_from':'2020-01-01T00:00:00Z'})
    elif name=='out-of-order':
        # Producer receives a stale revision after a newer revision.
        order(name,3,150);order(name,2,120)
    elif name=='late-scd2':put('customer_history',h+'-backdated',1,{'customer_id':c,'segment':'premium','effective_from':'2021-01-01T00:00:00Z'})
    elif name=='replay':order(name,1,100)
    elif name=='late-delete':
        order(name,2,100,'cancelled');run()
        with pg() as conn:conn.execute('DELETE FROM entities WHERE kind=%s AND id=%s',('order',o))
    else:order(name,2,120)

def inspect(name):
    _,o,_=ids(name)
    with pg() as c:
        print('Source changes',c.execute('SELECT event_id,revision,op,batch_no FROM change_queue WHERE id=%s ORDER BY event_id',(o,)).fetchall())
        print('Batches',c.execute('SELECT batch_no,status,json_sha256,delta_manifest,error FROM etl_batches ORDER BY batch_no').fetchall())
    print('Published fact',ch().query('SELECT order_id,amount,customer_known,segment,status,deleted FROM fact_order WHERE order_id={id:String}',parameters={'id':o}).result_rows)

def verify(name):
    _,o,_=ids(name)
    row=ch().query('SELECT amount,customer_known,segment,status,deleted FROM fact_order WHERE order_id={id:String}',parameters={'id':o}).result_rows
    amount=150 if name=='out-of-order' else 120 if name in ('etl-delay','after-extract','after-transform','during-load','after-publish') else 100
    expected=(amount,1,'premium' if name=='late-scd2' else 'standard','cancelled' if name=='late-delete' else 'paid',1 if name=='late-delete' else 0)
    assert len(row)==1 and tuple(row[0])==expected,(name,row,expected)
    client=ch();fact_total=client.query("SELECT sumIf(amount,deleted=0 AND status='paid') FROM fact_order").first_row[0]
    assert fact_total==client.query('SELECT sum(revenue) FROM daily_revenue').first_row[0]
    assert client.query('SELECT count()-uniqExact(order_id) FROM fact_order').first_row[0]==0
    print('PASS',name,row);return row

def demo(name):
    seed(name);inspect(name);inject(name)
    if name in ('after-extract','after-transform','during-load','after-publish'):
        _,o,_=ids(name);before=ch().query('SELECT amount FROM fact_order WHERE order_id={id:String}',parameters={'id':o}).first_row[0]
        try:run(fail=name)
        except RuntimeError as e:
            if not str(e).startswith('Injected failure'):raise
            print('Expected failure:',e)
        else:raise AssertionError('Failure injection did not occur')
        visible=ch().query('SELECT amount FROM fact_order WHERE order_id={id:String}',parameters={'id':o}).first_row[0]
        assert visible==(120 if name=='after-publish' else before),'Partial warehouse publication'
    run();first=verify(name)
    # Replay durable event membership through Delta MERGE and a fresh publication.
    with pg() as c:c.execute('UPDATE change_queue SET batch_no=NULL WHERE id=%s',(ids(name)[1],))
    run();assert first==verify(name)
