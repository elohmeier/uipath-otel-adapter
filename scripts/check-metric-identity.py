"""Validate the synthetic two-tenant demo and every generated metric query."""
import argparse,sys,json,urllib.request,urllib.parse
from pathlib import Path
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--metrics-url', default='http://localhost:19090')
args=parser.parse_args()
root=Path(__file__).resolve().parents[1]

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
dashboard=json.load(open(root/'dev/grafana/dashboards/uipath.json'))
count=0
empty=[]
for panel in dashboard['panels']:
 for target in panel.get('targets',[]):
  if 'expr' not in target:continue
  q=target['expr']
  for token,value in [('$__rate_interval','1m'),('$installation','demo'),('$folder','.*'),('${process:regex}','.*'),('${queue:regex}','.*')]:q=q.replace(token,value)
  rows=query(q);count+=1
  if not rows:empty.append(panel['title'])
  if panel['title']=='Collection health':assert float(rows[0]['value'][1])==1
for var in dashboard['templating']['list']:
 if var['type']!='query':continue
 q=var['query']['query']
 for token,value in [('$installation','demo'),('$folder','.*')]:q=q.replace(token,value)
 if q.startswith('query_result('):assert query(q[len('query_result('):-1]),var['name']
 else:assert var['name']=='installation' and 'target_info{' in q

assert not empty, "synthetic dashboard has empty panels: " + str(empty)
print(json.dumps({"panels_queried":count,"source_instances":2,"promotion_required":False}))
