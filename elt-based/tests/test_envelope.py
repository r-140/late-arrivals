import json,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'app'))
from envelope import normalize
class EnvelopeTests(unittest.TestCase):
    def event(self,op='u',encoded=True):
        row={'kind':'order','id':'o1','revision':2,'data':json.dumps({'amount':120}) if encoded else {'amount':120}}
        return {'op':op,'after':None if op=='d' else row,'before':row if op=='d' else None,'source':{'ts_ms':1000,'lsn':90}}
    def test_schemas_disabled_and_identity(self):
        r=normalize(json.dumps(self.event()),'topic',0,12)
        self.assertEqual(r[:5],['topic:0:12','order','o1',2,'u'])
        self.assertEqual(r[6:],[1000,90,'topic',0,12])
    def test_schema_enabled_delete(self):
        r=normalize(json.dumps({'schema':{},'payload':self.event('d')}),'topic',0,13)
        self.assertEqual(r[4],'d');self.assertEqual(json.loads(r[5]),{'amount':120})
    def test_json_object_payload(self):
        r=normalize(json.dumps(self.event(encoded=False)),'topic',0,12)
        self.assertEqual(json.loads(r[5]),{'amount':120})
    def test_tombstone_and_truncate(self):
        self.assertIsNone(normalize('null','topic',0,0))
        self.assertIsNone(normalize('{"op":"t"}','topic',0,0))
if __name__=='__main__':unittest.main()
