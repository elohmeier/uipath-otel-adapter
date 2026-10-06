"""Validate the synthetic two-tenant demo's metric identity contract."""
import argparse,json,urllib.request,urllib.parse
from promql import with_source_metadata
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--metrics-url', default='http://localhost:19090')
args=parser.parse_args()

def query(q):
 u=args.metrics_url.rstrip('/')+'/api/v1/query?'+urllib.parse.urlencode({'query':q})
 d=json.load(urllib.request.urlopen(u))
 assert d['status']=='success' and not d.get('warnings'),d
 return d['data']['result']
raw=query('uipath_jobs')
assert raw and all(x['metric'].get('job') and x['metric'].get('instance') for x in raw)
assert all('uipath_installation' not in x['metric'] and 'uipath_tenant_id' not in x['metric'] for x in raw)
info=query('target_info{uipath_installation="demo"}')
assert len({x['metric']['instance'] for x in info})==2, 'need both tenants'
assert {x['metric']['uipath_tenant_id'] for x in info}=={'demo','tenant-b'}
# Consumers attach installation/tenant through a target_info join.
joined=query(with_source_metadata('count by (uipath_tenant_id) (uipath_jobs{uipath_installation="demo"})'))
assert {x['metric']['uipath_tenant_id'] for x in joined}=={'demo','tenant-b'}, 'metadata join incomplete'
print(json.dumps({"source_instances":2,"joined_tenants":2,"promotion_required":False}))
