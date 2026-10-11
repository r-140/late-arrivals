import argparse,json
from pathlib import Path
from common import ROOT,pg
from pipeline import run
from scenarios import NAMES,seed,inject,inspect,verify,demo
p=argparse.ArgumentParser();p.add_argument('action',choices=['run','seed','inject','inspect','verify','demo','batches','files','json','delta','test']);p.add_argument('name',nargs='?');p.add_argument('--fail',default='',choices=['','after-extract','after-transform','during-load','after-publish']);x=p.parse_args()
if x.action=='run':run(x.fail)
elif x.action=='batches':
    with pg() as c:
        for row in c.execute('SELECT batch_no,status,created_at,error FROM etl_batches ORDER BY batch_no'):print(row)
elif x.action=='files':
    for path in sorted(ROOT.rglob('*')):
        if path.is_file():print(path)
elif x.action=='json':print((ROOT/'json'/f'batch-{int(x.name):06d}.jsonl').read_text())
elif x.action=='delta':
    from transform import session
    from delta.tables import DeltaTable
    s=session()
    try:
        name=x.name or 'fact_order';path=str(ROOT/'delta'/name)
        s.read.format('delta').load(path).show(truncate=False)
        DeltaTable.forPath(s,path).history().select('version','timestamp','operation','operationMetrics').show(truncate=False)
    finally:s.stop()
elif x.action=='test':
    import unittest
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.discover('tests'))
    raise SystemExit(0 if result.wasSuccessful() else 1)
else:
    if x.name not in NAMES:p.error('Choose a scenario: '+', '.join(NAMES))
    globals()[x.action](x.name)
