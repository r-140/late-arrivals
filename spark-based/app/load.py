"""Staging generations plus one publication pointer; failed attempts stay invisible."""
import uuid
from datetime import timezone
from common import ROOT,ch

def load(spark,conn,batch,manifest,fail=''):
    client=ch()
    existing=client.query('SELECT count() FROM publications WHERE batch_no={b:UInt64}',parameters={'b':batch}).first_row[0]
    if existing:
        # Recovery from crash AFTER publish but BEFORE Postgres stage acknowledgement.
        with conn.transaction():conn.execute("UPDATE etl_batches SET status='loaded',published_at=now(),error=NULL WHERE batch_no=%s",(batch,))
        return
    generation=uuid.uuid4().hex
    models=[('dim_customer','customer_generations',['customer_sk','customer_id','name','segment','valid_from','valid_to','inferred']),('fact_order','order_generations',['order_id','customer_id','customer_sk','amount','event_time','status','deleted','customer_known','segment']),('daily_revenue','revenue_generations',['day','segment','revenue','orders'])]
    for name,table,cols in models:
        df=spark.read.format('delta').option('versionAsOf',manifest[name]).load(str(ROOT/'delta'/name))
        rows=[]
        for row in df.select(*cols).toLocalIterator():
            values=[v.replace(tzinfo=timezone.utc) if hasattr(v,'tzinfo') and v.tzinfo is None else v for v in row]
            rows.append([generation,*values])
            if len(rows)>=1000:client.insert(table,rows,column_names=['generation',*cols]);rows=[]
        if rows:client.insert(table,rows,column_names=['generation',*cols])
        if fail=='during-load' and name=='dim_customer':raise RuntimeError('Injected failure after staged dimensions; published star remains unchanged')
    # Validate staged star before visibility switches.
    facts=client.query("SELECT count(),uniqExact(order_id),sumIf(amount,deleted=0 AND status='paid') FROM order_generations WHERE generation={g:String}",parameters={'g':generation}).first_row
    revenue=client.query('SELECT sum(revenue) FROM revenue_generations WHERE generation={g:String}',parameters={'g':generation}).first_row[0]
    assert facts[0]==facts[1] and (facts[2] or 0)==(revenue or 0),(facts,revenue)
    unresolved=client.query("SELECT count() FROM order_generations f LEFT JOIN customer_generations d ON f.generation=d.generation AND f.customer_sk=d.customer_sk WHERE f.generation={g:String} AND d.customer_sk=''",parameters={'g':generation}).first_row[0]
    assert unresolved==0,'Broken dimension foreign key'
    client.insert('publications',[[batch,generation]],column_names=['batch_no','generation'])
    if fail=='after-publish':raise RuntimeError('Injected failure after publication before acknowledgement')
    with conn.transaction():conn.execute("UPDATE etl_batches SET status='loaded',published_at=now(),error=NULL WHERE batch_no=%s",(batch,))
