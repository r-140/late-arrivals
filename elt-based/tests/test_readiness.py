import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'app'))
from readiness import running
class ReadinessTests(unittest.TestCase):
    def test_empty_tasks_is_not_ready(self):
        self.assertFalse(running({'connector':{'state':'RUNNING'},'tasks':[]}))
    def test_failed_task_is_not_ready(self):
        self.assertFalse(running({'connector':{'state':'RUNNING'},'tasks':[{'state':'FAILED'}]}))
    def test_connector_and_task_running(self):
        self.assertTrue(running({'connector':{'state':'RUNNING'},'tasks':[{'state':'RUNNING'}]}))
