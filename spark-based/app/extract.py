"""Durable membership checkpoint: never skip records using max(sequence_id)."""
import hashlib,json,os
from settings import ROOT

def extract(conn):
    with conn.transaction():
        pending=conn.execute("SELECT batch_no FROM etl_batches WHERE status!='loaded' ORDER BY batch_no LIMIT 1 FOR UPDATE").fetchone()
        if pending:batch=pending[0]
        else:
            events=conn.execute('SELECT event_id FROM change_queue WHERE batch_no IS NULL ORDER BY event_id FOR UPDATE SKIP LOCKED').fetchall()
            if not events:return None
            batch=conn.execute('INSERT INTO etl_batches DEFAULT VALUES RETURNING batch_no').fetchone()[0]
            conn.execute('UPDATE change_queue SET batch_no=%s WHERE event_id=ANY(%s)',(batch,[x[0] for x in events]))
    # A later commit with a lower event_id remains unclaimed and enters a future batch.
    path=ROOT/'json'/f'batch-{batch:06d}.jsonl';path.parent.mkdir(parents=True,exist_ok=True)
    rows=conn.execute('SELECT event_id,kind,id,revision,op,data,source_time FROM change_queue WHERE batch_no=%s ORDER BY event_id',(batch,)).fetchall()
    payload=''.join(json.dumps(dict(zip(['event_id','kind','id','revision','op','data','source_time'],[r[0],r[1],r[2],r[3],r[4],r[5],r[6].isoformat()])),sort_keys=True)+'\n' for r in rows).encode()
    digest=hashlib.sha256(payload).hexdigest()
    if path.exists():
        if hashlib.sha256(path.read_bytes()).hexdigest()!=digest:raise RuntimeError('Immutable JSON batch checksum mismatch')
    else:
        temp=path.with_suffix('.tmp')
        with temp.open('wb') as out:out.write(payload);out.flush();os.fsync(out.fileno())
        temp.replace(path)
    with conn.transaction():conn.execute("UPDATE etl_batches SET json_sha256=%s,status=CASE WHEN status='claimed' THEN 'extracted' ELSE status END WHERE batch_no=%s",(digest,batch))
    return batch
