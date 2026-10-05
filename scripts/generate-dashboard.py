#!/usr/bin/env python3
"""Generate the neutral, provisioned UiPath dashboard. No runtime data is used."""
import json
from promql import with_source_metadata, SOURCE_JOBS
from pathlib import Path

root = Path(__file__).resolve().parents[1]
metrics = {"type": "prometheus", "uid": "uipath-metrics"}
selector = 'uipath_installation=~"$installation",uipath_folder_id=~"$folder"'
panels = []

def panel(title, expr, x, y, w=8, h=7, kind="timeseries", unit="short", description="", legend="{{uipath_folder_id}}"):
    p = {"id": len(panels)+1, "title": title, "type": kind,
         "description": description, "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "datasource": metrics,
         "maxDataPoints": 1440, "interval": "15s", "targets": [{"refId": "A", "expr": expr, "legendFormat": legend, "range": kind == "timeseries", "instant": kind != "timeseries"}],
         "fieldConfig": {"defaults": {"unit": unit, "color": {"mode": "palette-classic"}}, "overrides": []},
         "options": {"tooltip": {"mode": "multi"}, "legend": {"displayMode": "list", "placement": "bottom"}}}
    if kind == "stat":
        p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"], "values": False}, "textMode": "auto", "colorMode": "value", "graphMode": "area"}
    panels.append(p)

panels.append({"id":1,"title":"UiPath — collection and execution overview","type":"text","gridPos":{"x":0,"y":0,"w":24,"h":3},"options":{"mode":"markdown","content":"All three signals arrive via **OTLP**. Check collection health before interpreting metrics. Job spans show elapsed execution time; bootstrap history is excluded from live completion counters."}})
panel("Collection health", f'min(uipath_collector_scrape_success_ratio{{{selector}}})',0,3,6,5,"stat",description="1 = every selected dataset succeeded; 0 = a collection failure. No data is a coverage gap.")
panel("Pending jobs", f'sum(uipath_jobs{{{selector},uipath_job_state="Pending"}})',6,3,6,5,"stat")
panel("Running jobs", f'sum(uipath_jobs{{{selector},uipath_job_state="Running"}})',12,3,6,5,"stat")
panel("Oldest pending job", f'max(uipath_job_oldest_pending_age_seconds{{{selector}}})',18,3,6,5,"stat","s")
panel("Active jobs by state", f'sum by (uipath_job_state) (uipath_jobs{{{selector}}})',0,8,12,7,legend="{{uipath_job_state}}")
panel("New completions / minute", f'sum by (uipath_job_state) (rate(uipath_jobs_completed_total{{{selector}}}[$__rate_interval])) * 60',12,8,12,7,legend="{{uipath_job_state}}")
panel("Last observed execution duration", f'uipath_job_last_duration_seconds{{{selector}}}',0,15,12,7,unit="s",legend="{{uipath_folder_id}} / {{uipath_process_name}}")
panel("Time since last successful job", f'time() - uipath_process_last_success_time_seconds{{{selector}}}',12,15,12,7,unit="s",legend="{{uipath_folder_id}} / {{uipath_process_name}}")
panel("Execution duration p95 — new completions", f'histogram_quantile(0.95, sum by (le, uipath_process_name) (rate(uipath_job_duration_seconds_bucket{{{selector}}}[$__rate_interval])))',0,22,12,7,unit="s",legend="{{uipath_process_name}}",description="Requires new completed jobs after monitoring started; bootstrap history is excluded.")
panel("Dataset freshness", f'time() - uipath_collector_last_success_time_seconds{{{selector}}}',12,22,12,7,unit="s",legend="{{uipath_folder_id}} / {{uipath_dataset}}")
panel("Queued OTLP batches", 'uipath_collector_outbox_pending{uipath_installation=~"$installation"}',0,29,8,6,legend="pending")
panel("Quarantined OTLP batches", 'uipath_collector_outbox_quarantined{uipath_installation=~"$installation"}',8,29,8,6,legend="quarantined",description="Permanent or partial rejection. Batches are retained locally for investigation, never automatically replayed.")
panel("Queue backlog — optional dataset", f'sum by (uipath_queue_id,uipath_queue_state) (uipath_queue_items{{{selector}}})',16,29,8,6,legend="{{uipath_queue_id}} / {{uipath_queue_state}}",description="Enable COLLECT_QUEUES and the appropriate read permissions. No series does not prove an empty queue.")
panels.append({"id":len(panels)+1,"title":"Robot logs and job lifecycle events","type":"logs","gridPos":{"x":0,"y":35,"w":24,"h":10},"datasource":{"type":"elasticsearch","uid":"uipath-logs"},"targets":[{"refId":"A","query":'uipath.installation:/${installation:regex}/ AND uipath.folder.id:/${folder:regex}/',"metrics":[{"id":"1","type":"logs"}],"bucketAggs":[],"timeField":"@timestamp"}],"options":{"showTime":True,"showLabels":False,"wrapLogMessage":True,"sortOrder":"Descending","enableLogDetails":True,"prettifyLogMessage":False},"description":"Events for the selected source/folder and time range. Expand a record to inspect identity and follow trace.id to Tempo."})
panels.append({"id":len(panels)+1,"title":"Reconstructed execution traces","type":"table","gridPos":{"x":0,"y":45,"w":24,"h":9},"datasource":{"type":"tempo","uid":"uipath-traces"},"targets":[{"refId":"A","queryType":"traceql","query":'{ span.uipath.trace.origin = "orchestrator_api" && resource.uipath.installation =~ "${installation:regex}" && span.uipath.folder.id =~ "${folder:regex}" }',"limit":20,"tableType":"traces"}],"options":{"showHeader":True},"description":"Job-summary spans for the selected source/folder and time range. Click a trace to inspect job state, duration and correlation."})
# Inventory names are joined at query time, keeping mutable names off counters.
identity = "instance,uipath_installation,uipath_tenant_id,uipath_folder_id"
info = f'max by ({identity},uipath_folder_name) (uipath_folder_info{{{selector}}})'
process_selector = selector + ',uipath_process_name=~"${process:regex}"'
def named(expr):
    return f'({expr}) * on ({identity}) group_left(uipath_folder_name) ({info})'

panels[0]["options"]["content"] = "Current job counts and coverage follow **Folder**. Execution metrics, logs and traces also follow **Process**. Live counters exclude bootstrap history."
# Health requires current inventory, successful discovery, and all enabled datasets.
fresh = f'(time() - uipath_collector_last_success_time_seconds{{{selector}}} < bool 180)'
scrape = f'uipath_collector_scrape_success_ratio{{{selector}}}'
enabled = f'uipath_collector_dataset_enabled_ratio{{{selector}}} == 1'
health_expr = f'min(({scrape} * {fresh}) and on ({identity},uipath_dataset) ({enabled}))'
discovery = 'min(uipath_collector_discovery_success_ratio{uipath_installation=~"$installation"})'
coverage = f'(count({scrape} and on ({identity},uipath_dataset) ({enabled})) == bool count({enabled}))'
panels[1]["targets"][0]["expr"] = f'({health_expr}) * ({discovery}) * ({coverage})'
health = panels[1]["fieldConfig"]["defaults"]
health.update({"min":0,"max":1,"noValue":"Unknown", "color":{"mode":"thresholds"},"thresholds":{"mode":"absolute","steps":[{"color":"red","value":None},{"color":"green","value":1}]},"mappings":[{"type":"value","options":{"0":{"text":"Degraded","color":"red"},"1":{"text":"Healthy","color":"green"}}}]})
panels[1]["description"] = "Discovery, enabled-dataset coverage and successful collection within 3 minutes. Missing signals are Unknown. Scope is selected folders, not the process filter."
# Keep operational context near the top, diagnostics at the bottom.
for p in panels[5:]: p["gridPos"]["y"] += 5
panel("Folders discovered", f'count({info})',0,8,8,5,"stat",description="Authorized selected folders, including folders without jobs. This is not proof that every server folder is authorized.")
panel("Datasets successful / enabled", f'count(({scrape} == 1) and on ({identity},uipath_dataset) ({enabled}) and on ({identity},uipath_dataset) ({fresh} == 1)) or (0 * count({enabled}))',8,8,8,5,"stat",legend="Successful")
panels[-1]["targets"].append({"refId":"B","expr":f'count({enabled})',"legendFormat":"Enabled","instant":True,"range":False})
panel("Oldest successful collection", f'max(time() - uipath_collector_last_success_time_seconds{{{selector}}})',16,8,8,5,"stat","s")
# Process-specific queries; preserve identity when aggregating duration buckets.
panels[6]["title"] = "New completions / hour"
panels[6]["targets"][0]["expr"] = f'sum by (uipath_job_state) (rate(uipath_jobs_completed_total{{{process_selector}}}[$__rate_interval])) * 3600'
panels[7].update({"title":"Latest execution duration by process", "type":"table", "options":{"showHeader":True,"sortBy":[{"displayName":"Duration","desc":True}]}})
# Table-wide units also format numeric-looking identity labels.
panels[7]["fieldConfig"]["defaults"]["unit"] = "none"
panels[7]["fieldConfig"]["overrides"].append({"matcher":{"id":"byName","options":"Duration"},"properties":[{"id":"unit","value":"s"}]})
panels[7]["targets"][0].update({"expr":named(f'uipath_job_last_duration_seconds{{{process_selector}}}'),"instant":True,"range":False,"format":"table"})
panels[7]["transformations"] = [{"id":"organize","options":{"excludeByName":{"Time":True,"__name__":True,"job":True,"instance":True,"receive_replica":True,"deployment_environment_name":True},"renameByName":{"uipath_folder_name":"Folder","uipath_folder_id":"Folder ID","uipath_process_name":"Process","uipath_installation":"Installation","uipath_tenant_id":"Tenant","Value":"Duration"}}}]
panels[8]["targets"][0].update({"expr":named(f'time() - uipath_process_last_success_time_seconds{{{process_selector}}}'),"legendFormat":"{{uipath_folder_name}} / {{uipath_process_name}}"})
panels[9]["targets"][0].update({"expr":named(f'histogram_quantile(0.95, sum by (le,{identity},uipath_process_name) (rate(uipath_job_duration_seconds_bucket{{{process_selector}}}[$__rate_interval])))'),"legendFormat":"{{uipath_folder_name}} / {{uipath_process_name}}"})
panels[10]["targets"][0].update({"expr":named(f'time() - uipath_collector_last_success_time_seconds{{{selector}}}'),"legendFormat":"{{uipath_folder_name}} / {{uipath_dataset}}"})
log_filter = 'uipath.installation:/${installation:regex}/ AND uipath.folder.id:/${folder:regex}/ AND uipath.process.name:/${process:regex}/'
panels[14]["targets"][0]["query"] = log_filter + ' AND log.level:/${severity:regex}/ AND NOT message:"Robot log (message collection disabled)"'
panels[14]["title"] = "Robot messages and lifecycle events"
panels[14]["description"] += " Earlier message-disabled placeholders are hidden."
panels[15]["targets"][0]["query"] = '{ span.uipath.trace.origin = "orchestrator_api" && resource.uipath.installation =~ "${installation:regex}" && span.uipath.folder.id =~ "${folder:regex}" && span.uipath.process.name =~ "${process:regex}" }'
# Execution history comes from lifecycle events, including bootstrap history.
panels.append({"id":20,"title":"Completed executions","type":"table","gridPos":{"x":0,"y":59,"w":24,"h":10},"datasource":{"type":"elasticsearch","uid":"uipath-logs"},"targets":[{"refId":"A","query":log_filter+' AND uipath.event.kind:"job.completed"',"metrics":[{"id":"1","type":"raw_data","settings":{"size":"100"}}],"bucketAggs":[],"timeField":"@timestamp"}],"options":{"showHeader":True},"transformations":[{"id":"filterFieldsByName","options":{"include":{"names":["@timestamp","uipath.folder.name","uipath.folder.id","uipath.process.name","uipath.job.state","uipath.job.duration","trace.id"]}}},{"id":"organize","options":{"renameByName":{"@timestamp":"Completed at","uipath.folder.name":"Folder","uipath.folder.id":"Folder ID","uipath.process.name":"Process","uipath.job.state":"Outcome","uipath.job.duration":"Duration","trace.id":"Trace"}}}],"fieldConfig":{"defaults":{},"overrides":[{"matcher":{"id":"byName","options":"Duration"},"properties":[{"id":"unit","value":"s"}]},{"matcher":{"id":"byName","options":"Trace"},"properties":[{"id":"links","value":[{"title":"Open trace","url":"/explore?left=" + __import__('urllib.parse',fromlist=['quote']).quote(json.dumps({"datasource":"uipath-traces","queries":[{"refId":"A","queryType":"traceql","query":"${__value.raw}"}]})),"targetBlank":True}]}]}]},"description":"Latest 100 completed-job events in the selected period. Older events may lack folder names or duration; stable folder IDs remain available."})
# Leave Grafana's field variable unescaped within the encoded Explore state.
for override in panels[-1]["fieldConfig"]["overrides"]:
    for prop in override["properties"]:
        if prop["id"] == "links":
            for link in prop["value"]:
                link["url"] = link["url"].replace("%24%7B__value.raw%7D", "${__value.raw}")

# Consistent execution columns put actionable context before trace identifiers.
panels[-1]["transformations"][1]["options"]["indexByName"] = {name:i for i,name in enumerate(["@timestamp","uipath.folder.name","uipath.folder.id","uipath.process.name","uipath.job.state","uipath.job.duration","trace.id"])}
panels[-1]["fieldConfig"]["overrides"].extend([
    {"matcher":{"id":"byName","options":"Outcome"},"properties":[{"id":"mappings","value":[{"type":"value","options":{"Successful":{"color":"green"},"Faulted":{"color":"red"},"Stopped":{"color":"orange"}}}]},{"id":"custom.cellOptions","value":{"type":"color-text"}}]},
    {"matcher":{"id":"byName","options":"Process"},"properties":[{"id":"custom.width","value":350}]}
])
panels[7]["transformations"].insert(0,{"id":"filterFieldsByName","options":{"include":{"names":["uipath_installation","uipath_tenant_id","uipath_folder_name","uipath_folder_id","uipath_process_name","Value"]}}})
panels[7]["transformations"][1]["options"]["indexByName"]={name:i for i,name in enumerate(["uipath_folder_name","uipath_process_name","Value","uipath_folder_id","uipath_installation","uipath_tenant_id"])}
# Put execution history ahead of diagnostic charts.
for p in panels:
    if p["id"] == 20: p["gridPos"]["y"] = 20
    elif p["gridPos"]["y"] >= 20: p["gridPos"]["y"] += 10
variables=[]
for name,label,query in [("installation","Installation",f'label_values(target_info{{job=~"{SOURCE_JOBS}"}}, uipath_installation)' ),("folder","Folder",'query_result(max by (uipath_folder_id,uipath_folder_name) (uipath_folder_info{uipath_installation=~"$installation"}))'),("process","Process",'query_result(max by (uipath_process_name) (uipath_job_last_duration_seconds{uipath_installation=~"$installation",uipath_folder_id=~"$folder"}))')]:
    v={"name":name,"label":label,"type":"query","datasource":metrics,"query":{"query":query,"refId":"variable"},"refresh":2,"multi":True,"includeAll":True,"allValue":".*","current":{"text":"All","value":"$__all"}}
    if name == "folder": v["regex"] = '/uipath_folder_id="(?<value>[^\"]+)".*uipath_folder_name="(?<text>[^\"]+)"/'
    if name == "process": v["regex"] = '/uipath_process_name="([^\"]+)"/'
    variables.append(v)
variables.append({"name":"severity","label":"Log level","type":"custom","query":"TRACE,DEBUG,INFO,WARN,ERROR,FATAL","multi":True,"includeAll":True,"allValue":".*","current":{"text":"All","value":"$__all"}})
# Queue views are folder-scoped at collection; deduplicate linked views by tenant
# and queue identity before presenting totals. Failed/stale views are excluded.
queue_identity = "instance,uipath_installation,uipath_tenant_id,uipath_queue_id"
queue_selector = selector + ',uipath_queue_id=~"${queue:regex}"'
queue_ok = f'(uipath_collector_dataset_enabled_ratio{{{selector},uipath_dataset="queues"}} == 1) and on ({identity}) (uipath_collector_scrape_success_ratio{{{selector},uipath_dataset="queues"}} == 1) and on ({identity}) (time() - uipath_collector_last_success_time_seconds{{{selector},uipath_dataset="queues"}} < 180) and on ({identity}) (uipath_collector_folder_available_ratio{{{selector}}} == 1) and on (instance,uipath_installation,uipath_tenant_id) (uipath_collector_discovery_success_ratio{{uipath_installation=~"$installation"}} == 1)'
def queue_view(expr):
    return f'({expr}) and on ({identity}) ({queue_ok})'
queue_info = f'max by ({queue_identity},uipath_queue_name) ({queue_view(f"uipath_queue_info{{{queue_selector}}}")})'
def queue_named(metric, extra=""):
    by = queue_identity + (","+extra if extra else "")
    return f'max by ({by}) ({queue_view(f"{metric}{{{queue_selector}}}")}) * on ({queue_identity}) group_left(uipath_queue_name) ({queue_info})'
# Known folders lost from discovery remain visible and degrade health.
panels[1]["targets"][0]["expr"] += f' * min(uipath_collector_folder_available_ratio{{{selector}}})'
panels[16]["title"] = "Folders visible / known"
panels[16]["targets"][0]["expr"] = f'sum(uipath_collector_folder_available_ratio{{{selector}}})'
panels[16]["targets"][0]["legendFormat"] = "Visible"
panels[16]["targets"].append({"refId":"B","expr":f'count(uipath_collector_folder_available_ratio{{{selector}}})',"legendFormat":"Known","instant":True,"range":False})
# Replace old backlog panel, which summed linked folder views.
panels[13]["title"] = "Queue backlog by state"
panels[13]["targets"][0].update({"expr":queue_named("uipath_queue_items","uipath_queue_state"),"legendFormat":"{{uipath_queue_name}} / {{uipath_queue_state}}"})
panels[13]["description"] = "Current New/InProgress items. Linked views use max per queue, never a sum across folders. Only successful, fresh queue views are included."
for p in panels:
    if p["gridPos"]["y"] >= 30: p["gridPos"]["y"] += 26
queue_status = f'uipath_collector_dataset_status{{{selector},uipath_dataset="queues"}}'
queue_status_query = f'max(({queue_status} != 1) or (({queue_status} == 1) and on ({identity}) ({queue_ok})) or (4 * (({queue_status} == 1) unless on ({identity}) ({queue_ok}))))'
panel("Queue collection",queue_status_query,0,30,6,5,"stat")
panels[-1]["fieldConfig"]["defaults"]["noValue"]="Unknown"
panels[-1]["fieldConfig"]["defaults"]["mappings"]=[{"type":"value","options":{"0":{"text":"Disabled","color":"gray"},"1":{"text":"Healthy","color":"green"},"2":{"text":"Failed","color":"red"},"3":{"text":"Forbidden","color":"red"},"4":{"text":"Stale / unavailable","color":"orange"}}}]
panels[-1]["description"]="Last queue collection result. Check collection age and folder coverage above; missing values are Unknown. Queue panels ignore the process filter."
panel("Queues observed",f'count({queue_info})',6,30,6,5,"stat")
panel("Eligible work",f'sum({queue_named("uipath_queue_eligible")})',12,30,6,5,"stat")
panel("Overdue active items",f'sum({queue_named("uipath_queue_overdue")})',18,30,6,5,"stat")
panel("Oldest eligible item",queue_named("uipath_queue_oldest_pending_age_seconds"),0,35,12,7,unit="s",legend="{{uipath_queue_name}}",description="Age since creation or defer date, whichever is later. Future-deferred items are excluded.")
panel("Deferred work",queue_named("uipath_queue_deferred"),12,35,12,7,legend="{{uipath_queue_name}}")
panel("Transaction outcomes — rolling window",queue_named("uipath_queue_transactions","uipath_queue_state"),0,42,12,7,legend="{{uipath_queue_name}} / {{uipath_queue_state}}",description="Snapshot counts by EndProcessing within QUEUE_HISTORY_LOOKBACK (default 24h). Gauges, not counters; do not rate or sum over time. Retried attempts are separate records, not unique business transactions.")
panel("Processing exceptions — rolling window",queue_named("uipath_queue_exceptions","uipath_queue_exception_type"),12,42,12,7,legend="{{uipath_queue_name}} / {{uipath_queue_exception_type}}")
panel("Mean processing duration — rolling window",queue_named("uipath_queue_processing_mean_duration_seconds"),0,49,8,7,unit="s",legend="{{uipath_queue_name}}")
panel("Processing samples — rolling window",queue_named("uipath_queue_processing_samples"),8,49,8,7,legend="{{uipath_queue_name}}")
panel("Completed retry attempts — rolling window",queue_named("uipath_queue_retry_attempts"),16,49,8,7,legend="{{uipath_queue_name}}",description="Completed item records with RetryNumber > 0; not a sum of RetryNumber and not unique business transactions.")
variables.append({"name":"queue","label":"Queue","type":"query","datasource":metrics,"query":{"query":'query_result(max by (uipath_queue_id,uipath_queue_name) (uipath_queue_info{uipath_installation=~"$installation",uipath_folder_id=~"$folder"}))',"refId":"queue"},"regex":'/uipath_queue_id="(?<value>[^\"]+)".*uipath_queue_name="(?<text>[^\"]+)"/',"refresh":2,"multi":True,"includeAll":True,"allValue":".*","current":{"text":"All","value":"$__all"}})
# Enrich instant vectors (and counter rates) before domain-specific joins.
for p in panels:
    for target in p.get("targets", []):
        if "expr" in target:
            target["expr"] = with_source_metadata(target["expr"])
for variable in variables:
    if variable["type"] == "query" and variable["name"] != "installation":
        variable["query"]["query"] = with_source_metadata(variable["query"]["query"])
dashboard={"uid":"uipath-overview","title":"UiPath monitoring","tags":["uipath","opentelemetry"],"schemaVersion":39,"version":2,"editable":False,"timezone":"browser","time":{"from":"now-24h","to":"now"},"refresh":"30s","templating":{"list":variables},"panels":panels}
from scheduling_dashboard import scheduling_dashboard
dashboard["links"] = [{"title":"Host occupancy and jobs","url":"/d/uipath-scheduling"}]
(root/"dev/grafana/dashboards/uipath.json").write_text(json.dumps(dashboard,indent=2)+"\n")
(root/"dev/grafana/dashboards/uipath-scheduling.json").write_text(json.dumps(scheduling_dashboard(),indent=2)+"\n")
