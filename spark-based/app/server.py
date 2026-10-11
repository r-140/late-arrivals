"""Airflow delegates to worker; its Python environment stays independent of Spark."""
import json,subprocess,threading,uuid,traceback
from http.server import ThreadingHTTPServer,BaseHTTPRequestHandler
from common import pg,ch
jobs={};guard=threading.Lock()

def metrics():
    out=[]
    with pg() as c:
        queries={'pending_changes':'SELECT count(*) FROM change_queue WHERE batch_no IS NULL','incomplete_batches':"SELECT count(*) FROM etl_batches WHERE status!='loaded'",'failed_batches':'SELECT count(*) FROM etl_batches WHERE error IS NOT NULL','last_extract_seconds':'SELECT coalesce((SELECT extract_seconds FROM etl_batches ORDER BY batch_no DESC LIMIT 1),0)','last_transform_seconds':'SELECT coalesce((SELECT transform_seconds FROM etl_batches ORDER BY batch_no DESC LIMIT 1),0)','last_load_seconds':'SELECT coalesce((SELECT load_seconds FROM etl_batches ORDER BY batch_no DESC LIMIT 1),0)','seconds_since_publish':"SELECT coalesce(extract(epoch from now()-max(published_at)),0) FROM etl_batches WHERE status='loaded'"}
        for name,q in queries.items():out.append(f'spark_lab_{name} {c.execute(q).fetchone()[0] or 0}')
    client=ch()
    for name,q in {'unresolved_customers':'SELECT count() FROM fact_order WHERE customer_known=0 AND deleted=0','unknown_segments':"SELECT count() FROM fact_order WHERE segment='unknown' AND deleted=0",'revenue':'SELECT sum(revenue) FROM daily_revenue','deleted_orders':'SELECT count() FROM fact_order WHERE deleted=1','order_count':'SELECT count() FROM fact_order'}.items():out.append(f'spark_lab_{name} {client.query(q).first_row[0] or 0}')
    return '\n'.join(out)+'\n'

def execute(id):
    with guard:
        try:
            r=subprocess.run(['python','app/cli.py','run'],capture_output=True,text=True)
            jobs[id]={'status':'success' if r.returncode==0 else 'failed','output':(r.stdout+r.stderr)[-12000:]}
        except Exception:jobs[id]={'status':'failed','output':traceback.format_exc()}

class Handler(BaseHTTPRequestHandler):
    def reply(self,code,body,kind='application/json'):
        self.send_response(code);self.send_header('Content-Type',kind);self.end_headers();self.wfile.write(body.encode())
    def do_GET(self):
        try:
            if self.path=='/health':
                with pg() as c:c.execute('SELECT 1')
                ch().query('SELECT 1');self.reply(200,'{"ready":true}')
            elif self.path=='/metrics':self.reply(200,metrics(),'text/plain; version=0.0.4')
            elif self.path.startswith('/jobs/'):
                value=jobs.get(self.path.split('/')[-1]);self.reply(200 if value else 404,json.dumps(value or {'error':'Unknown job'}))
            else:self.reply(404,'{}')
        except Exception as e:self.reply(503,json.dumps({'error':str(e)}))
    def do_POST(self):
        if self.path!='/run':self.reply(404,'{}');return
        id=uuid.uuid4().hex;jobs[id]={'status':'queued'}
        threading.Thread(target=execute,args=(id,),daemon=True).start()
        self.reply(202,json.dumps({'job_id':id}))
ThreadingHTTPServer(('0.0.0.0',8090),Handler).serve_forever()
