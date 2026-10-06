package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/elohmeier/uipath-otel-adapter/internal/config"
	"github.com/elohmeier/uipath-otel-adapter/internal/state"
	"github.com/elohmeier/uipath-otel-adapter/internal/uipath"
	logexport "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
)

func integer(n int64) *int64 { return &n }

func TestInvalidJobsInvalidateSnapshot(t *testing.T) {
	for _, kind := range []string{"duplicate", "future", "unknown-state"} {
		t.Run(kind, func(t *testing.T) {
			a, f, _ := testAdapter(t)
			a.Config.JobSnapshots = true
			j := uipath.Job{Key: "job-a", State: "Pending", CreationTime: time.Now().Add(-time.Minute)}
			f.jobs = []uipath.Job{j}
			switch kind {
			case "duplicate":
				f.jobs = append(f.jobs, j)
			case "future":
				f.jobs[0].CreationTime = time.Now().Add(time.Hour)
			case "unknown-state":
				f.jobs[0].State = "new-state"
			}
			if a.Poll(context.Background()) == nil {
				t.Fatal("invalid read accepted")
			}
			r, _ := latestSnapshot(t, a.Store)
			rec := r.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
			if attribute(rec.Attributes, "uipath.snapshot.status") != "incomplete" || rec.Body.GetStringValue() != "{}" {
				t.Fatal("partial snapshot displayed")
			}
		})
	}
}
func exampleRuntime(now time.Time) uipath.MachineRuntime {
	return uipath.MachineRuntime{SessionID: 11, MachineID: 22, HostMachineName: "robot-a.example.com", RuntimeType: "Unattended", Status: "Available", MaintenanceMode: "Default", Runtimes: integer(2), UsedRuntimes: integer(0), ReportingTime: now}
}

type runtimeFake struct {
	*fakeSource
	rows  []uipath.MachineRuntime
	err   error
	calls int
}

func (f *runtimeFake) MachineRuntimes(context.Context) ([]uipath.MachineRuntime, error) {
	f.calls++
	return f.rows, f.err
}

func TestRuntimeStatesAndCoverage(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name   string
		mutate func(*uipath.MachineRuntime)
		want   float64
	}{
		{"free", func(r *uipath.MachineRuntime) {}, 1},
		{"partial", func(r *uipath.MachineRuntime) { r.UsedRuntimes = integer(1) }, 2},
		{"full", func(r *uipath.MachineRuntime) { r.UsedRuntimes = integer(2) }, 3},
		{"maintenance", func(r *uipath.MachineRuntime) { r.MaintenanceMode = "Enabled" }, 4},
		{"disconnected", func(r *uipath.MachineRuntime) { r.Status = "Disconnected" }, 5},
		{"unresponsive", func(r *uipath.MachineRuntime) { r.IsUnresponsive = true }, 6},
		{"no_capacity", func(r *uipath.MachineRuntime) { r.Runtimes = integer(0) }, 7},
		{"stale", func(r *uipath.MachineRuntime) { r.ReportingTime = now.Add(-time.Hour) }, 0},
		{"inconsistent", func(r *uipath.MachineRuntime) { r.Status = "Busy" }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := exampleRuntime(now)
			tc.mutate(&r)
			if e := validateRuntimes([]uipath.MachineRuntime{r}, now); e != nil {
				t.Fatal(e)
			}
			if got := runtimeStatus(r, now, time.Minute); got != tc.want {
				t.Fatalf("got %v", got)
			}
		})
	}
	r := exampleRuntime(now)
	if validateRuntimes([]uipath.MachineRuntime{r, r}, now) == nil {
		t.Fatal("duplicate accepted")
	}
	r.Runtimes = nil
	if validateRuntimes([]uipath.MachineRuntime{r}, now) == nil {
		t.Fatal("missing capacity became zero")
	}
	r = exampleRuntime(now)
	r.Status = "new-status"
	if validateRuntimes([]uipath.MachineRuntime{r}, now) == nil {
		t.Fatal("unknown state became free")
	}
	a, f, _ := testAdapter(t)
	a.Config.Runtimes = true
	f.folders = []uipath.Folder{{ID: 1}, {ID: 2}}
	rf := &runtimeFake{fakeSource: f, rows: []uipath.MachineRuntime{exampleRuntime(now)}}
	a.Source = rf
	if e := a.Poll(context.Background()); e != nil {
		t.Fatal(e)
	}
	if rf.calls != 1 {
		t.Fatal("tenant inventory collected per folder")
	}
	rf.err = &uipath.HTTPError{Status: 403}
	domain, health, e := a.runtimes(context.Background(), now.Add(time.Second))
	if e == nil || len(domain) != 0 || health[0].GetGauge().DataPoints[0].GetAsDouble() != 3 {
		t.Fatal("failed runtime read reused healthy capacity")
	}
}

func TestRuntimeInventoryEmitsSessionsOnceAndOnlySlottedTypes(t *testing.T) {
	now := time.Now()
	a, f, _ := testAdapter(t)
	a.Config.Runtimes = true
	a.Config.Freshness = time.Minute
	row := func(session int64, host, kind, status string, capacity, used int64, heartbeat time.Time) uipath.MachineRuntime {
		r := exampleRuntime(now)
		r.SessionID, r.MachineID, r.HostMachineName, r.RuntimeType, r.Status = session, session*10, host, kind, status
		r.Runtimes, r.UsedRuntimes, r.ReportingTime = integer(capacity), integer(used), heartbeat
		return r
	}
	a.Source = &runtimeFake{fakeSource: f, rows: []uipath.MachineRuntime{
		row(1, "robot-a.example.com", "Development", "Busy", 0, 0, now),
		row(1, "robot-a.example.com", "NonProduction", "Busy", 1, 0, now.Add(-10*time.Second)),
		row(1, "robot-a.example.com", "Unattended", "Busy", 2, 1, now),
		row(2, "robot-b.example.com", "Development", "Disconnected", 0, 0, now.Add(-time.Hour)),
		row(2, "robot-b.example.com", "Unattended", "Disconnected", 0, 0, now.Add(-time.Hour)),
	}}
	domain, _, e := a.runtimes(context.Background(), now)
	if e != nil {
		t.Fatal(e)
	}
	points := map[string]map[string]float64{}
	for _, m := range domain {
		for _, p := range m.GetGauge().DataPoints {
			key := attribute(p.Attributes, "uipath.session.id") + "/" + attribute(p.Attributes, "uipath.runtime.type")
			if points[m.Name] == nil {
				points[m.Name] = map[string]float64{}
			}
			points[m.Name][key] = p.GetAsDouble()
		}
	}
	if want := map[string]float64{"1/": 2, "2/": 5}; !maps.Equal(points["uipath.session.status"], want) {
		t.Fatalf("session status %v", points["uipath.session.status"])
	}
	if got := points["uipath.session.last_heartbeat"]["1/"]; got != float64(now.Add(-10*time.Second).UnixMilli())/1000 {
		t.Fatal("session heartbeat is not the oldest row")
	}
	if want := map[string]float64{"1/NonProduction": 1, "1/Unattended": 2}; !maps.Equal(points["uipath.runtime.capacity"], want) {
		t.Fatalf("zero-slot runtime types exported: %v", points["uipath.runtime.capacity"])
	}
	// The session is Busy, but its idle NonProduction slot is still free.
	if want := map[string]float64{"1/NonProduction": 1, "1/Unattended": 2}; !maps.Equal(points["uipath.runtime.status"], want) {
		t.Fatalf("runtime status %v", points["uipath.runtime.status"])
	}
	if points["uipath.runtime.observed"]["/"] != 5 || points["uipath.session.observed"]["/"] != 2 {
		t.Fatal("inventory coverage must count every row and session")
	}

	conflict := row(1, "robot-z.example.com", "Headless", "Busy", 0, 0, now)
	if validateRuntimes(append(a.Source.(*runtimeFake).rows, conflict), now) == nil {
		t.Fatal("conflicting session identity accepted")
	}
}

func TestRuntimeTypeStatusInheritsDegradedSession(t *testing.T) {
	r := exampleRuntime(time.Now())
	for session, want := range map[float64]float64{0: 0, 1: 1, 2: 1, 3: 1, 4: 4, 5: 5, 6: 6, 7: 1} {
		if got := runtimeTypeStatus(r, session); got != want {
			t.Fatalf("session %v: got %v", session, got)
		}
	}
	r.UsedRuntimes = integer(2)
	if runtimeTypeStatus(r, 2) != 3 {
		t.Fatal("occupied type not reported")
	}
}

func TestSessionStateMerge(t *testing.T) {
	now := time.Now()
	base := exampleRuntime(now)
	for _, tc := range []struct {
		name   string
		mutate func(*uipath.MachineRuntime)
		want   float64
	}{
		{"maintenance", func(r *uipath.MachineRuntime) { r.MaintenanceMode = "Enabled" }, 4},
		{"disconnected", func(r *uipath.MachineRuntime) { r.Status = "Disconnected" }, 5},
		{"unresponsive", func(r *uipath.MachineRuntime) { r.IsUnresponsive = true }, 6},
		{"unknown", func(r *uipath.MachineRuntime) { r.Status = "Unknown" }, 0},
		{"stale", func(r *uipath.MachineRuntime) { r.ReportingTime = now.Add(-time.Hour) }, 0},
		{"occupied", func(r *uipath.MachineRuntime) { r.Status, r.UsedRuntimes = "Busy", integer(2) }, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			other := base
			other.RuntimeType = "NonProduction"
			tc.mutate(&other)
			merged := sessions([]uipath.MachineRuntime{base, other})
			if len(merged) != 1 || *merged[0].Runtimes != 4 {
				t.Fatal("session rows not merged")
			}
			if got := runtimeStatus(merged[0], now, time.Minute); got != tc.want {
				t.Fatalf("got %v", got)
			}
		})
	}
	if *base.Runtimes != 2 {
		t.Fatal("merge mutated source rows")
	}
}

func attribute(attrs []*common.KeyValue, key string) string {
	for _, a := range attrs {
		if a.Key == key {
			return a.Value.GetStringValue()
		}
	}
	return ""
}
func latestSnapshot(t *testing.T, s *state.Store) (*logexport.ExportLogsServiceRequest, []byte) {
	t.Helper()
	pending, e := s.Pending(time.Now().Add(time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	for i := len(pending) - 1; i >= 0; i-- {
		if pending[i].Signal == "logs" {
			r := new(logexport.ExportLogsServiceRequest)
			if e = proto.Unmarshal(pending[i].Payload, r); e != nil {
				t.Fatal(e)
			}
			for _, rec := range r.ResourceLogs[0].ScopeLogs[0].LogRecords {
				if attribute(rec.Attributes, "uipath.event.kind") == "jobs.snapshot" {
					return r, pending[i].Payload
				}
			}
		}
	}
	t.Fatal("no snapshot")
	return nil, nil
}
func TestActiveSnapshotEmptyFailurePrivacyAndReplay(t *testing.T) {
	a, f, _ := testAdapter(t)
	a.Config.JobSnapshots = true
	now := time.Now().Add(-time.Minute)
	f.jobs = []uipath.Job{{Key: "job-a", State: "Running", ReleaseName: "Example process", HostMachineName: "robot-a.example.com", Robot: &uipath.JobRobot{Name: "Example robot", Username: "example-account"}, CreationTime: now, StartTime: &now}}
	if e := a.Poll(context.Background()); e != nil {
		t.Fatal(e)
	}
	r, _ := latestSnapshot(t, a.Store)
	rec := r.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	var jobs map[string]activeJob
	if e := json.Unmarshal([]byte(rec.Body.GetStringValue()), &jobs); e != nil {
		t.Fatal(e)
	}
	if len(jobs) != 1 || jobs["1/job-a"].Host != "robot-a.example.com" || jobs["1/job-a"].Username != "" {
		t.Fatal("assignment/privacy contract")
	}
	f.jobs = nil
	if e := a.Poll(context.Background()); e != nil {
		t.Fatal(e)
	}
	r, _ = latestSnapshot(t, a.Store)
	rec = r.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	if rec.Body.GetStringValue() != "{}" || attribute(rec.Attributes, "uipath.snapshot.status") != "complete" {
		t.Fatal("empty snapshot not explicit")
	}
	f.discoveryErr = errors.New("unavailable")
	if a.Poll(context.Background()) == nil {
		t.Fatal("missing discovery accepted")
	}
	r, _ = latestSnapshot(t, a.Store)
	rec = r.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	if attribute(rec.Attributes, "uipath.snapshot.status") != "incomplete" {
		t.Fatal("discovery failure presented as no work")
	}

	path := filepath.Join(t.TempDir(), "replay.db")
	s, e := state.Open(path, "source", 10)
	if e != nil {
		t.Fatal(e)
	}
	a.Store = s
	a.Config = config.Config{Installation: "example", Tenant: "tenant-a", Environment: "test", Freshness: time.Minute, LogRecordUID: true}
	if e = a.snapshot(map[string]activeJob{}, true, time.Now()); e != nil {
		t.Fatal(e)
	}
	r, before := latestSnapshot(t, s)
	uid := attribute(r.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Attributes, "log.record.uid")
	if len(uid) != 64 {
		t.Fatal("snapshot UID missing")
	}
	// Receiver accepted; crash before ACK, then reopen and replay the saved payload.
	s.Close()
	s, e = state.Open(path, "source", 10)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	_, after := latestSnapshot(t, s)
	if !bytes.Equal(before, after) {
		t.Fatal("replay changed observation identity")
	}
}

func TestSnapshotCapAndMissingFolder(t *testing.T) {
	a, f, _ := testAdapter(t)
	a.Config.JobSnapshots = true
	f.folders = []uipath.Folder{{ID: 1}, {ID: 2}}
	if e := a.Poll(context.Background()); e != nil {
		t.Fatal(e)
	}
	f.folders = []uipath.Folder{{ID: 1}}
	if a.Poll(context.Background()) == nil {
		t.Fatal("missing folder accepted")
	}
	r, _ := latestSnapshot(t, a.Store)
	if attribute(r.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Attributes, "uipath.snapshot.status") != "incomplete" {
		t.Fatal("missing coverage")
	}
	if a.snapshot(map[string]activeJob{"a": {Process: strings.Repeat("x", maxSnapshotBytes)}}, true, time.Now()) == nil {
		t.Fatal("oversized snapshot accepted")
	}
	r, _ = latestSnapshot(t, a.Store)
	if rec := r.ResourceLogs[0].ScopeLogs[0].LogRecords[0]; attribute(rec.Attributes, "uipath.snapshot.status") != "capped" || rec.Body.GetStringValue() != "{}" {
		t.Fatal("partial snapshot escaped")
	}
}
