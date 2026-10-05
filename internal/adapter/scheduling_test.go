package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
