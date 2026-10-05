"""Source-scoped operations board; no per-job Prometheus dimensions."""
import copy
import json
from urllib.parse import quote

METRICS = {"type": "prometheus", "uid": "uipath-metrics"}
LOGS = {"type": "elasticsearch", "uid": "uipath-logs"}


def scheduling_dashboard():
    panels = []
    def table(title, x, y, w, h, datasource, targets, transforms=(), description=""):
        p = {"id": len(panels)+1, "title": title, "type": "table", "gridPos": {"x": x, "y": y, "w": w, "h": h},
             "datasource": datasource, "targets": targets, "transformations": list(transforms),
             "options": {"showHeader": True, "cellHeight": "sm"}, "description": description,
             "fieldConfig": {"defaults": {"unit": "none", "noValue": "—", "custom": {"minWidth": 50}}, "overrides": []}}
        panels.append(p)
        return p
    def prom(expr, ref="A"):
        return {"refId": ref, "expr": expr, "instant": True, "range": False, "format": "table"}
    def columns(mapping):
        return [{"id": "filterFieldsByName", "options": {"include": {"names": list(mapping)}}},
                {"id": "organize", "options": {"renameByName": mapping, "indexByName": {k:i for i,k in enumerate(mapping)}}}]
    def match(field, pattern):
        return {"fieldName": field, "config": {"id": "regex", "options": {"value": pattern}}}
    def keep(*filters):
        return {"id": "filterByValue", "options": {"type": "include", "match": "all", "filters": list(filters)}}
    source = 'instance="$source"'
    sel = source + ',uipath_runtime_type=~"${runtime:regex}"'
    key = 'instance,uipath_session_id,uipath_runtime_type'
    success = f'uipath_collector_runtimes_last_success_time_seconds{{{source}}}'
    fresh = f'uipath_collector_freshness_seconds{{{source}}}'
    ok = f'(uipath_collector_runtimes_status{{{source}}} == 1) and on(instance) (time() - {success} < on(instance) {fresh})'
    observed = f'(uipath_runtime_observed_time_seconds{{{sel}}} == on(instance) group_left {success})'
    def current(metric):
        return f'({metric}{{{sel}}} and on({key}) {observed}) and on(instance) ({ok})'
    panels.append({"id": 1, "title": "Host occupancy and executions", "type": "text", "gridPos": {"x":0,"y":0,"w":24,"h":3},
                   "options": {"mode":"markdown","content":"**Host capacity covers the whole source**, including work outside the selected folder. Job tables show the latest unexpired, complete snapshot; an empty table is only known-empty when **Snapshot status = complete** and its job count is zero. No snapshot means unknown. Scheduling and per-job queue totals are not collected."}})
    status_metric=f'uipath_collector_runtimes_status{{{source}}}'
    status_expr=f'({status_metric} != 1) or ({status_metric} and on(instance) ({ok})) or (4 * (({status_metric} == 1) unless on(instance) ({ok})))'
    status = table("Runtime collection",0,3,8,4,METRICS,[prom(status_expr)],columns({"Value":"Collection"}))
    status["fieldConfig"]["defaults"]["mappings"]=[{"type":"value","options":{"0":{"text":"Disabled"},"1":{"text":"Collected"},"2":{"text":"Failed","color":"red"},"3":{"text":"Forbidden","color":"red"},"4":{"text":"Stale","color":"orange"}}}]
    query = 'service.node.name:"$source" AND uipath.event.kind:"jobs.snapshot" AND uipath.snapshot.valid_until:[now TO *]'
    snapshot = {"refId":"A","query":query,"metrics":[{"id":"1","type":"raw_data","settings":{"size":"1"}}],"bucketAggs":[],"timeField":"@timestamp"}
    snapshot_status = table("Latest job snapshot",8,3,16,4,LOGS,[copy.deepcopy(snapshot)],columns({"@timestamp":"Observed at","uipath.snapshot.status":"Snapshot status","uipath.snapshot.jobs":"Active jobs","uipath.snapshot.valid_until":"Expires at"}),"Newest observation for this source, including incomplete and empty snapshots. This panel must be present and complete before interpreting empty job tables.")
    snapshot_status["fieldConfig"]["overrides"]=[{"matcher":{"id":"byName","options":"Snapshot status"},"properties":[{"id":"mappings","value":[{"type":"value","options":{"complete":{"text":"Complete","color":"green"},"incomplete":{"text":"Incomplete","color":"red"},"capped":{"text":"Capped","color":"orange"}}}]},{"id":"custom.cellOptions","value":{"type":"color-text"}}]}]
    # Merge instant tables by their common identity and observation timestamp.
    hosts=table("Hosts — capacity across all folders",0,7,10,15,METRICS,
        [prom(current(m),r) for m,r in [("uipath_runtime_status","A"),("uipath_runtime_used","B"),("uipath_runtime_capacity","C")]],
        [{"id":"filterFieldsByName","options":{"exclude":{"names":["__name__"]}}},{"id":"merge","options":{}}]+columns({"uipath_host_name":"Host","Value #A":"Status","Value #B":"Used","Value #C":"Slots","uipath_runtime_type":"Runtime"}),
        "One row per session/runtime type; never summed across folders. Unknown includes stale heartbeat. No current rows means no fresh inventory, not free hosts.")
    mappings={"0":{"text":"Unknown / stale","color":"gray"},"1":{"text":"Free","color":"green"},"2":{"text":"Partly occupied","color":"yellow"},"3":{"text":"Occupied","color":"orange"},"4":{"text":"Maintenance","color":"purple"},"5":{"text":"Disconnected","color":"red"},"6":{"text":"Unresponsive","color":"red"},"7":{"text":"No capacity","color":"gray"}}
    hosts["fieldConfig"]["overrides"]=[{"matcher":{"id":"byName","options":"Status"},"properties":[{"id":"mappings","value":[{"type":"value","options":mappings}]},{"id":"custom.cellOptions","value":{"type":"color-text"}}]}]
    for name, width in [("Host",145),("Status",110),("Used",50),("Slots",50),("Runtime",110)]:
        hosts["fieldConfig"]["overrides"].append({"matcher":{"id":"byName","options":name},"properties":[{"id":"custom.width","value":width}]})
    for title,states,y in [("Running / stopping jobs","Running|Stopping|Terminating|Resumed",7),("Pending / suspended jobs","Pending|Suspended",15)]:
        transforms=[keep(match("uipath.snapshot.status","^complete$")),
            {"id":"extractFields","options":{"source":"message","format":"json","replace":True}},
            {"id":"reduce","options":{"mode":"seriesToRows","reducers":["lastNotNull"]}},
            {"id":"extractFields","options":{"source":"Last *","format":"json","replace":True}},
            keep(match("state","^("+states+")$"),match("folder_id","^(${folder:regex})$"))]
        mapping={"process":"Process","host":"Host","robot_username":"Account","state":"State",("elapsed_seconds" if y == 7 else "waiting_seconds"):("Elapsed" if y == 7 else "Waiting"),"folder":"Folder","priority":"Priority"}
        p=table(title,10,y,14,8,LOGS,[copy.deepcopy(snapshot)],transforms+columns(mapping),"An empty host means the API has not reported an assignment. Elapsed time is sampled and includes suspension; it does not measure consumed runtime. Folder filtering is applied after selecting the source snapshot.")
        p["fieldConfig"]["overrides"]=[{"matcher":{"id":"byName","options":name},"properties":[{"id":"unit","value":"s"}]} for name in ["Elapsed","Waiting"]]
        p["fieldConfig"]["overrides"].append({"matcher":{"id":"byName","options":"Host"},"properties":[{"id":"mappings","value":[{"type":"value","options":{"":{"text":"Unassigned"}}}]}]})
        for name,width in [("Process",155),("Host",135),("Account",120),("State",95),("Elapsed",80),("Waiting",80),("Folder",140),("Priority",75)]:
            p["fieldConfig"]["overrides"].append({"matcher":{"id":"byName","options":name},"properties":[{"id":"custom.width","value":width}]})
    table("Host heartbeat age",0,22,10,7,METRICS,[prom('time() - ('+current("uipath_runtime_last_heartbeat_seconds")+')')],columns({"uipath_host_name":"Host","uipath_runtime_type":"Runtime","Value":"Heartbeat age"}))["fieldConfig"]["overrides"]=[{"matcher":{"id":"byName","options":"Heartbeat age"},"properties":[{"id":"unit","value":"s"}]}]
    table("Runtime observation age",10,23,14,6,METRICS,[prom(f'time() - {success}')],columns({"Value":"Seconds since successful collection"}))["fieldConfig"]["defaults"]["unit"]="s"
    done=table("Recently completed jobs",0,29,24,10,LOGS,[{"refId":"A","query":'service.node.name:"$source" AND uipath.event.kind:"job.completed" AND uipath.folder.id:/${folder:regex}/',"metrics":[{"id":"1","type":"raw_data","settings":{"size":"100"}}],"bucketAggs":[],"timeField":"@timestamp"}],columns({"@timestamp":"Completed at","uipath.process.name":"Process","uipath.folder.name":"Folder","uipath.host.name":"Host","uipath.robot.username":"Account","uipath.job.state":"Outcome","uipath.job.duration":"Duration","trace.id":"Trace"}),"Latest 100 completions in the selected history window. Host/account fields are available only on newly collected enriched records.")
    done["fieldConfig"]["overrides"]=[{"matcher":{"id":"byName","options":"Duration"},"properties":[{"id":"unit","value":"s"}]}]
    done["fieldConfig"]["overrides"].append({"matcher":{"id":"byName","options":"Trace"},"properties":[{"id":"links","value":[{"title":"Open trace","url":"/explore?left="+quote(json.dumps({"datasource":"uipath-traces","queries":[{"refId":"A","queryType":"traceql","query":"${__value.raw}"}]})),"targetBlank":True}]}]})
    panels[0]["gridPos"]["h"]=4
    for p in panels:
        if p["id"] != 1:
            p["gridPos"]["y"] += 1
        if p["id"] not in [1,9]:
            p["timeFrom"]="15m"
            p["hideTimeOverride"]=True
    variables=[
        {"name":"source","label":"Source","type":"query","datasource":METRICS,"query":{"query":'query_result(label_join(max by(instance,uipath_installation,uipath_tenant_id,deployment_environment_name)(target_info{job="uipath/uipath-orchestrator"}),"source_label"," / ","uipath_installation","uipath_tenant_id","deployment_environment_name"))',"refId":"source"},"regex":'/instance="(?<value>[^"]+)".*source_label="(?<text>[^"]+)"/',"multi":False,"includeAll":False,"refresh":2},
        {"name":"folder","label":"Job folder","type":"query","datasource":METRICS,"query":{"query":'query_result(uipath_folder_info{instance="$source"})',"refId":"folder"},"regex":'/uipath_folder_id="(?<value>[^"]+)".*uipath_folder_name="(?<text>[^"]+)"/',"multi":True,"includeAll":True,"allValue":".*","refresh":2,"current":{"text":"All","value":"$__all"}},
        {"name":"runtime","label":"Runtime type","type":"query","datasource":METRICS,"query":{"query":'label_values(uipath_runtime_status{instance="$source"}, uipath_runtime_type)',"refId":"runtime"},"multi":False,"includeAll":True,"allValue":".*","refresh":2,"current":{"text":"Unattended","value":"Unattended"}},
    ]
    return {"uid":"uipath-scheduling","title":"UiPath host occupancy","tags":["uipath","opentelemetry"],"schemaVersion":39,"version":1,"editable":False,"timezone":"browser","time":{"from":"now-24h","to":"now"},"refresh":"30s","templating":{"list":variables},"panels":panels,"links":[{"title":"Execution monitoring","url":"/d/uipath-overview"}]}
