package adapter

import (
	"context"
	"errors"
	"github.com/elohmeier/uipath-otel-adapter/internal/config"
	"github.com/elohmeier/uipath-otel-adapter/internal/state"
	"github.com/elohmeier/uipath-otel-adapter/internal/telemetry"
	"github.com/elohmeier/uipath-otel-adapter/internal/uipath"
	metricexport "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
	"path/filepath"
	"testing"
	"time"
)

type fakeSource struct {
	definitions            []uipath.QueueDefinition
	items                  []uipath.QueueItem
	queueErr, discoveryErr error
	folders                []uipath.Folder
	jobs                   []uipath.Job
	logs                   []uipath.RobotLog
	logErr                 error
}

func (f *fakeSource) Folders(context.Context) ([]uipath.Folder, error) {
	if f.discoveryErr != nil {
		return nil, f.discoveryErr
	}
	if f.folders != nil {
		return f.folders, nil
	}
	return []uipath.Folder{{ID: 1}}, nil
}
func (f *fakeSource) Jobs(context.Context, int64, time.Time) ([]uipath.Job, error) {
	return f.jobs, nil
}
func (f *fakeSource) Logs(context.Context, int64, time.Time) ([]uipath.RobotLog, error) {
	return f.logs, f.logErr
}
func (f *fakeSource) QueueItems(context.Context, int64, time.Time) ([]uipath.QueueItem, error) {
	return f.items, f.queueErr
}

type fakeSender struct {
	counts map[string]int
	fail   string
}

func (f *fakeSender) Send(_ context.Context, s string, _ []byte) error {
	f.counts[s]++
	if s == f.fail {
		return errors.New("temporary")
	}
	return nil
}
func testAdapter(t *testing.T) (*Adapter, *fakeSource, *fakeSender) {
	t.Helper()
	s, e := state.Open(filepath.Join(t.TempDir(), "state.db"), "test", 20)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	f := &fakeSource{}
	sender := &fakeSender{counts: map[string]int{}}
	return &Adapter{Config: config.Config{Lookback: time.Hour, LogLookback: time.Hour, Overlap: time.Minute, Freshness: time.Minute, Logs: true}, Store: s, Source: f, Sender: sender}, f, sender
}
func TestReplayDoesNotDuplicateSignalsAndBootstrapDoesNotCount(t *testing.T) {
	a, f, _ := testAdapter(t)
	end := time.Now().Add(-time.Minute)
	start := end.Add(-time.Minute)
	f.jobs = []uipath.Job{{Key: "job-a", CreationTime: start, StartTime: &start, EndTime: &end, State: "Successful", ReleaseName: "demo"}}
	f.logs = []uipath.RobotLog{{ID: 10, JobKey: "job-a", TimeStamp: end, Level: "Info"}}
	for range 2 {
		if e := a.Poll(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	p, _ := a.Store.Pending(time.Now())
	if len(p) != 3 {
		t.Fatalf("expected lifecycle, trace, robot batches; got %d", len(p))
	}
	var d FolderState
	a.Store.Load("folder/1", &d)
	if d.Processes["demo"].Completed["Successful"] != 0 {
		t.Fatal("bootstrap inflated live counters")
	}
	// A terminal job discovered after monitoring started is counted once.
	end = time.Now()
	start = end.Add(-time.Minute)
	f.jobs = []uipath.Job{{Key: "job-b", CreationTime: start, StartTime: &start, EndTime: &end, State: "Successful", ReleaseName: "demo"}}
	for range 2 {
		if e := a.Poll(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	a.Store.Load("folder/1", &d)
	if d.Processes["demo"].Completed["Successful"] != 1 || d.Processes["demo"].Duration.Count != 1 {
		t.Fatal("completion not deduplicated")
	}
}
func TestIncompleteLogsDoNotAdvanceCheckpoint(t *testing.T) {
	a, f, _ := testAdapter(t)
	f.logErr = uipath.ErrCapped
	if e := a.Poll(context.Background()); e == nil {
		t.Fatal("gap hidden")
	}
	var d FolderState
	a.Store.Load("folder/1", &d)
	if !d.LogCursor.IsZero() || !d.LogsSuccess.IsZero() {
		t.Fatal("failed dataset checkpoint advanced")
	}
}
func TestMissingStableLogIdentityIsGap(t *testing.T) {
	a, f, _ := testAdapter(t)
	f.logs = []uipath.RobotLog{{ID: 0, TimeStamp: time.Now().Add(-time.Second)}}
	if e := a.Poll(context.Background()); e == nil {
		t.Fatal("identity gap hidden")
	}
}
func TestOneSignalFailureDoesNotBlockOthers(t *testing.T) {
	a, _, sender := testAdapter(t)
	a.Store.Save("x", 1, []state.Envelope{{Signal: "logs", Payload: []byte("a")}, {Signal: "traces", Payload: []byte("b")}})
	sender.fail = "logs"
	if e := a.Flush(context.Background()); e == nil {
		t.Fatal("failure hidden")
	}
	if sender.counts["traces"] != 1 {
		t.Fatal("trace blocked by logs")
	}
	n, _ := a.Store.Counts()
	if n != 1 {
		t.Fatal("failed batch not retained")
	}
}
func TestExpiredSnapshotGaugesAreAbsent(t *testing.T) {
	a, _, _ := testAdapter(t)
	d := folderState()
	d.JobsSuccess = time.Now().Add(-time.Hour)
	for _, m := range a.folderMetrics("1", d, time.Now()) {
		if m.Name == "uipath.jobs" {
			t.Fatal("stale gauge exported")
		}
	}
}
func TestPartialSuccessQuarantinesWithoutRetry(t *testing.T) {
	a, _, _ := testAdapter(t)
	a.Sender = partialSender{}
	a.Store.Save("x", 1, []state.Envelope{{Signal: "logs", Payload: []byte("a")}})
	a.Flush(context.Background())
	n, q := a.Store.Counts()
	if n != 0 || q != 1 {
		t.Fatal("partial response not quarantined")
	}
	p, _ := a.Store.Pending(time.Now().Add(time.Hour))
	if len(p) != 0 {
		t.Fatal("partial response scheduled for retry")
	}
}

type partialSender struct{}

func (partialSender) Send(context.Context, string, []byte) error {
	return &telemetry.ExportError{Status: 200, Rejected: 1, Permanent: true}
}

func TestMetricPartialSuccessIsNotReplayed(t *testing.T) {
	a, _, _ := testAdapter(t)
	a.Sender = partialSender{}
	a.Store.Metrics([]byte("snapshot"))
	if e := a.Flush(context.Background()); e == nil {
		t.Fatal("partial success hidden")
	}
	b, _ := a.Store.TakeMetrics()
	if len(b) != 0 {
		t.Fatal("partially accepted metric snapshot will be retried")
	}
	_, q := a.Store.Counts()
	if q != 1 {
		t.Fatal("rejected snapshot not retained")
	}
}

func TestInventoryAndDatasetConfigurationAreExportedWithoutJobs(t *testing.T) {
	a, _, _ := testAdapter(t)
	if err := a.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	payload, err := a.Store.TakeMetrics()
	if err != nil {
		t.Fatal(err)
	}
	var request metricexport.ExportMetricsServiceRequest
	if err := proto.Unmarshal(payload, &request); err != nil {
		t.Fatal(err)
	}
	inventory, disabled := false, false
	for _, rm := range request.ResourceMetrics {
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == "uipath.folder.info" {
					for _, point := range m.GetGauge().DataPoints {
						for _, attr := range point.Attributes {
							if attr.Key == "uipath.folder.name" && attr.Value.GetStringValue() == "1" {
								inventory = true
							}
						}
					}
				}
				if m.Name == "uipath.collector.dataset.enabled" {
					for _, point := range m.GetGauge().DataPoints {
						for _, attr := range point.Attributes {
							if attr.Key == "uipath.dataset" && attr.Value.GetStringValue() == "queues" && point.GetAsDouble() == 0 {
								disabled = true
							}
						}
					}
				}
			}
		}
	}
	if !inventory || !disabled {
		t.Fatal("empty folder inventory or disabled dataset missing")
	}
}

func (f *fakeSource) QueueDefinitions(context.Context, int64) ([]uipath.QueueDefinition, error) {
	return f.definitions, f.queueErr
}
