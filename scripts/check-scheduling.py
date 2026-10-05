#!/usr/bin/env python3
"""Read-only assertions for the synthetic Compose scheduling demo."""
import json
from datetime import datetime, timezone
from urllib.parse import urlencode
from smoke import request


def main():
    def query(expr):
        response = request("http://localhost:19090", "/api/v1/query?" + urlencode({"query": expr}))
        assert response["status"] == "success" and not response.get("warnings"), "incomplete metrics query"
        return response["data"]["result"]

    identity = query('target_info{job="uipath/uipath-orchestrator",uipath_installation="demo",uipath_tenant_id="demo"}')
    assert len(identity) == 1, "expected one synthetic demo source"
    instance = identity[0]["metric"]["instance"]
    selector = '{instance=' + json.dumps(instance) + '}'
    rows = query('uipath_runtime_status' + selector)
    assert len(rows) == 4 and sorted(float(r["value"][1]) for r in rows) == [1, 3, 4, 5], "runtime inventory/state mismatch"
    assert all("uipath_folder_id" not in r["metric"] for r in rows), "tenant capacity duplicated per folder"
    assert len(query('uipath_runtime_capacity' + selector)) == 4, "capacity missing"
    result = request("http://localhost:19200", "/logs-uipath-*/_search", {
        "size": 1, "sort": [{"@timestamp": "desc"}], "query": {"bool": {"filter": [
            {"term": {"service.node.name": instance}}, {"term": {"uipath.event.kind": "jobs.snapshot"}}
        ]}}})
    assert result["hits"]["hits"], "snapshot missing"
    doc = result["hits"]["hits"][0]["_source"]
    snapshot = doc["uipath"]["snapshot"]
    assert snapshot["status"] == "complete" and snapshot["jobs"] == 2, "snapshot incomplete"
    assert datetime.fromisoformat(snapshot["valid_until"].replace("Z", "+00:00")) > datetime.now(timezone.utc), "snapshot expired"
    jobs = list(json.loads(doc["message"]).values())
    running = next(j for j in jobs if j["state"] == "Running")
    pending = next(j for j in jobs if j["state"] == "Pending")
    assert running["host"] == "robot-a.example.com" and running["robot_username"] == "example-robot", "running assignment missing"
    assert pending["host"] == "" and pending["waiting_seconds"] > 0, "pending assignment fabricated"
    assert all(j["folder"] and j["job_key"] for j in jobs), "job identity missing"
    dashboard = request("http://localhost:13000", "/api/dashboards/uid/uipath-scheduling")["dashboard"]
    assert len(dashboard["panels"]) == 9, "scheduling dashboard missing"
    print(json.dumps({"runtime_rows": len(rows), "active_jobs": len(jobs), "assignments": "ok", "snapshot_freshness": "ok"}))


if __name__ == "__main__":
    main()
