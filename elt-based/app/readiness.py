"""Host-side Connect registration; wait for a running task, not only REST availability."""
import json,os,time,urllib.request,urllib.error
from pathlib import Path

def running(status):
    return status.get('connector',{}).get('state')=='RUNNING' and bool(status.get('tasks')) and all(t.get('state')=='RUNNING' for t in status['tasks'])

def register():
    base='http://localhost:'+os.environ.get('LAB_CONNECT_PORT','18083')
    config=json.loads((Path(__file__).resolve().parents[1]/'infra/connector.json').read_text())['config']
    deadline=time.monotonic()+180
    registered=False;last='Connect not reachable'
    while time.monotonic()<deadline:
        try:
            if not registered:
                req=urllib.request.Request(base+'/connectors/lab-source/config',data=json.dumps(config).encode(),headers={'Content-Type':'application/json'},method='PUT')
                with urllib.request.urlopen(req,timeout=5) as response:response.read()
                registered=True
            with urllib.request.urlopen(base+'/connectors/lab-source/status',timeout=5) as response:status=json.load(response)
            if running(status):
                print('Debezium connector and source task RUNNING');return
            last=json.dumps(status)
            failures=[t for t in [status.get('connector',{}),*status.get('tasks',[])] if t.get('state')=='FAILED']
            if failures:raise RuntimeError('Debezium failed: '+json.dumps(failures))
        except (urllib.error.URLError,TimeoutError,OSError) as exc:last=str(exc)
        time.sleep(2)
    raise TimeoutError('Connect readiness timeout: '+last)

if __name__=='__main__':register()
