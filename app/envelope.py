import json

def normalize(value,topic,partition,offset):
    e=json.loads(value)
    if e is None: return None
    e=e.get('payload',e)
    if e is None or e.get('op') not in ('c','u','d','r'):
        return None
    row=e.get('after') or e['before']; src=e['source']
    data=row['data']; data=data if isinstance(data,str) else json.dumps(data)
    return [f'{topic}:{partition}:{offset}',row['kind'],row['id'],row['revision'],e['op'],data,src.get('ts_ms') or 0,src.get('lsn') or 0,topic,partition,offset]
