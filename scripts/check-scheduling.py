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
    # Zero-slot runtime types and the disconnected host's types are not exported.
    assert sorted((r["metric"]["uipath_host_name"], r["metric"]["uipath_runtime_type"], float(r["value"][1])) for r in rows) == [
        ("robot-a.example.com", "Unattended", 3), ("robot-b.example.com", "NonProduction", 1),
        ("robot-b.example.com", "Unattended", 1), ("robot-d.example.com", "Unattended", 4)], "runtime inventory/state mismatch"
    assert all("uipath_folder_id" not in r["metric"] for r in rows), "tenant capacity duplicated per folder"
    assert len(query('uipath_runtime_capacity' + selector)) == 4, "capacity missing"
    sessions = query('uipath_session_status' + selector)
    assert sorted((r["metric"]["uipath_host_name"], float(r["value"][1])) for r in sessions) == [
        ("robot-a.example.com", 3), ("robot-b.example.com", 1), ("robot-c.example.com", 5), ("robot-d.example.com", 4)], "session state mismatch"
    assert all("uipath_runtime_type" not in r["metric"] for r in sessions), "session state duplicated per runtime type"
    assert float(query('uipath_runtime_observed' + selector)[0]["value"][1]) == 7, "inventory row coverage mismatch"
    assert float(query('uipath_session_observed' + selector)[0]["value"][1]) == 4, "session coverage mismatch"
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
    print(json.dumps({"runtime_rows": len(rows), "sessions": len(sessions), "active_jobs": len(jobs), "assignments": "ok", "snapshot_freshness": "ok"}))


if __name__ == "__main__":
    main()
