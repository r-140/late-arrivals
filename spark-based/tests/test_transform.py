"""Runs against real local Spark + Delta inside the worker image."""
import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'app'))
import transform
class TransformTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.spark=transform.session();cls.spark.sparkContext.setLogLevel('ERROR')
    @classmethod
    def tearDownClass(cls):cls.spark.stop()
    def test_old_fact_replay_and_backdated_history(self):
        with tempfile.TemporaryDirectory() as d,patch.object(transform,'ROOT',Path(d)):
            root=Path(d);(root/'json').mkdir()
            def event(n,kind,id,revision,data,op='u'):return {'event_id':n,'kind':kind,'id':id,'revision':revision,'op':op,'data':data,'source_time':'2026-01-01T00:00:00Z'}
            records=[event(1,'order','o',1,{'customer_id':'c','amount':100,'event_time':'2025-01-01T00:00:00Z','status':'paid'})]
            def batch(n,rows):
                (root/'json'/f'batch-{n:06d}.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in rows));return transform.transform(self.spark,n)
            def fact():return self.spark.read.format('delta').load(str(root/'delta/fact_order')).first()
            batch(1,records);self.assertEqual(fact().customer_known,0);self.assertEqual(fact().customer_sk,'unknown')
            batch(2,[event(2,'customer','c',1,{'name':'Alice'}),event(3,'customer_history','h',1,{'customer_id':'c','segment':'standard','effective_from':'2020-01-01T00:00:00Z'})])
            self.assertEqual(fact().segment,'standard');self.assertEqual(fact().customer_known,1)
            manifest=batch(3,[event(4,'customer_history','h2',1,{'customer_id':'c','segment':'premium','effective_from':'2021-01-01T00:00:00Z'})])
            self.assertEqual(fact().segment,'premium')
            batch(4,records);self.assertEqual(fact().segment,'premium')
            self.assertEqual(self.spark.read.format('delta').load(str(root/'delta/events')).count(),4)
            old=self.spark.read.format('delta').option('versionAsOf',manifest['fact_order']).load(str(root/'delta/fact_order')).first();self.assertEqual(old.segment,'premium')
