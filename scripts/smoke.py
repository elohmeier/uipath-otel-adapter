#!/usr/bin/env python3
"""Read-only backend smoke test. Prints counts, never source payloads or names."""
import argparse
import json
import time
import urllib.parse
import urllib.request


def request(base, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(base+path, data=data, headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)

def check_coverage(installation, require_queues=False, fetch=request, now=None):
    """Require every known folder and configured dataset, not only surviving series."""
    now = time.time() if now is None else now
    selector = '{uipath_installation=' + json.dumps(installation) + '}'
    def query(metric):
        response = fetch("http://localhost:19090", "/api/v1/query?" + urllib.parse.urlencode({"query": metric + selector}))
        assert response.get("status") == "success" and not response.get("warnings"), "metric query incomplete"
        return response["data"]["result"]
    def tenant(row):
        labels = row["metric"]
        return labels.get("uipath_installation"), labels.get("uipath_tenant_id")
    def folder(row):
        return tenant(row) + (row["metric"].get("uipath_folder_id"),)
    def dataset(row):
        return folder(row) + (row["metric"].get("uipath_dataset"),)
    def values(rows, key):
        result = {}
        for row in rows:
            identity = key(row)
            assert all(identity) and identity not in result, "ambiguous or missing identity"
            result[identity] = float(row["value"][1])
        return result
    discovery = values(query("uipath_collector_discovery_success_ratio"), tenant)
    assert discovery and all(v == 1 for v in discovery.values()), "discovery failed or missing"
    folders = values(query("uipath_collector_folder_available_ratio"), folder)
    assert folders and all(v == 1 for v in folders.values()), "folder disappeared or unavailable"
    assert {k[:2] for k in folders} == set(discovery), "tenant folder coverage incomplete"
    enabled = values(query("uipath_collector_dataset_enabled_ratio"), dataset)
    expected = {k + (d,) for k in folders for d in ("jobs", "logs", "queues")}
    assert set(enabled) == expected and all(v in (0, 1) for v in enabled.values()), "dataset configuration missing"
    assert all(enabled[k + ("jobs",)] == 1 for k in folders), "jobs collection disabled"
    if require_queues:
        assert all(enabled[k + ("queues",)] == 1 for k in folders), "queues collection disabled"
    health = values(query("uipath_collector_scrape_success_ratio"), dataset)
    freshness = values(query("uipath_collector_last_success_time_seconds"), dataset)
    for identity, active in enabled.items():
        if not active:
            continue
        assert health.get(identity) == 1, "enabled dataset missing or failed"
        ts = freshness.get(identity, 0)
        assert ts > 0 and -60 <= now - ts < 180, "enabled dataset missing or stale"


def check(a):
    q = 'count(uipath_jobs{uipath_installation='+json.dumps(a.installation)+'})'
    m = request("http://localhost:19090", "/api/v1/query?"+urllib.parse.urlencode({"query":q}))
    assert m["data"]["result"], "no source metrics"
    l = request("http://localhost:19200", "/logs-uipath-*/_search", {"size":1,"track_total_hits":True,
        "query":{"bool":{"filter":[{"term":{"uipath.installation":a.installation}},{"exists":{"field":"trace.id"}}]}}})
    hits=l["hits"]["hits"]
    assert hits, "no correlated logs for installation"
    source=hits[0]["_source"]
    trace=source.get("trace",{}).get("id") or source.get("trace.id")
    assert trace, "log has no trace identifier"
    t=request("http://localhost:13200", "/api/traces/"+trace)
    assert t.get("batches") or t.get("resourceSpans"), "referenced trace not stored"
    d=request("http://localhost:13000", "/api/dashboards/uid/uipath-overview")
    assert len(d["dashboard"]["panels"])>=10, "dashboard missing"
    check_coverage(a.installation, require_queues=a.queues)
    if a.queues:
        for query in [
            'min(uipath_collector_scrape_success_ratio{uipath_installation='+json.dumps(a.installation)+',uipath_dataset="queues"})',
            'count(uipath_queue_info{uipath_installation='+json.dumps(a.installation)+'})',
        ]:
            result=request("http://localhost:19090", "/api/v1/query?"+urllib.parse.urlencode({"query":query}))
            assert result["data"]["result"] and float(result["data"]["result"][0]["value"][1]) >= 1, "queue collection/inventory missing"
    return {"metric_series":int(float(m["data"]["result"][0]["value"][1])),"correlated_logs":l["hits"]["total"]["value"],"trace_lookup":"ok","dashboard_panels":len(d["dashboard"]["panels"]),"collection_health":"ok"}

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--installation", required=True)
    p.add_argument("--timeout", type=int, default=90)
    p.add_argument("--queues", action="store_true", help="Require fresh queue collection and inventory")
    a = p.parse_args()
    deadline = time.monotonic() + a.timeout
    while True:
        try:
            print(json.dumps(check(a)))
            return
        except Exception:
            if time.monotonic() >= deadline:
                raise SystemExit("Smoke test failed: inspect source coverage, OTLP delivery, backend health and dashboard provisioning locally.")
            time.sleep(2)

if __name__ == "__main__":
    main()
