import sys,tempfile,unittest,json
from contextlib import nullcontext
from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'app'))
import extract
class Result:
    def __init__(self,rows):self.rows=rows
    def fetchone(self):return self.rows[0] if self.rows else None
    def fetchall(self):return self.rows
class Store:
    def __init__(self):self.events={};self.batches={}
    def transaction(self):return nullcontext()
    def event(self,id):self.events[id]={'batch':None,'row':(id,'order','o',1,'u',{'amount':100},datetime(2026,1,1,tzinfo=timezone.utc))}
    def execute(self,q,args=()):
        if q.startswith('SELECT batch_no'):return Result([(b,) for b,s in self.batches.items() if s!='loaded'][:1])
        if q.startswith('SELECT event_id FROM'):return Result([(e,) for e,v in sorted(self.events.items()) if v['batch'] is None])
        if q.startswith('INSERT INTO etl_batches'):
            b=len(self.batches)+1;self.batches[b]='claimed';return Result([(b,)])
        if q.startswith('UPDATE change_queue'):
            for e in args[1]:self.events[e]['batch']=args[0]
            return Result([])
        if q.startswith('SELECT event_id,kind'):return Result([v['row'] for e,v in sorted(self.events.items()) if v['batch']==args[0]])
        if q.startswith('UPDATE etl_batches'):
            b=args[1]
            if self.batches[b]=='claimed':self.batches[b]='extracted'
            return Result([])
        raise AssertionError(q)
class ExtractTests(unittest.TestCase):
    def test_retry_reuses_immutable_json_and_detects_corruption(self):
        with tempfile.TemporaryDirectory() as d,patch.object(extract,'ROOT',Path(d)):
            store=Store();store.event(11);b=extract.extract(store);path=Path(d)/'json/batch-000001.jsonl';first=path.read_bytes()
            self.assertEqual(extract.extract(store),b);self.assertEqual(path.read_bytes(),first)
            path.write_text('{}')
            with self.assertRaisesRegex(RuntimeError,'checksum'):extract.extract(store)
    def test_later_committing_lower_id_is_claimed_next_time(self):
        with tempfile.TemporaryDirectory() as d,patch.object(extract,'ROOT',Path(d)):
            store=Store();store.event(11);first=extract.extract(store);store.batches[first]='loaded'
            store.event(10);second=extract.extract(store)
            self.assertNotEqual(first,second);self.assertEqual(store.events[10]['batch'],second)
            self.assertEqual(json.loads((Path(d)/'json/batch-000002.jsonl').read_text())['event_id'],10)
