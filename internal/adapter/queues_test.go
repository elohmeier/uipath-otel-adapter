package adapter

import (
	"context"
	"errors"
	"github.com/elohmeier/uipath-otel-adapter/internal/uipath"
	metricexport "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

func snapshot(t *testing.T, a *Adapter) []*metrics.Metric {
	t.Helper()
	p, e := a.Store.TakeMetrics()
	if e != nil {
		t.Fatal(e)
	}
	var req metricexport.ExportMetricsServiceRequest
	if e = proto.Unmarshal(p, &req); e != nil {
		t.Fatal(e)
	}
	var out []*metrics.Metric
	for _, r := range req.ResourceMetrics {
		for _, s := range r.ScopeMetrics {
			out = append(out, s.Metrics...)
		}
	}
	return out
}
func value(ms []*metrics.Metric, name string, labels map[string]string) (float64, bool) {
	for _, m := range ms {
		if m.Name != name {
			continue
		}
		for _, p := range m.GetGauge().GetDataPoints() {
			a := map[string]string{}
			for _, k := range p.Attributes {
				a[k.Key] = k.Value.GetStringValue()
			}
			match := true
			for k, v := range labels {
				if a[k] != v {
					match = false
				}
			}
			if match {
				return p.GetAsDouble(), true
			}
		}
	}
	return 0, false
}
func expect(t *testing.T, ms []*metrics.Metric, name string, labels map[string]string, want float64) {
	t.Helper()
	v, ok := value(ms, name, labels)
	if !ok || v != want {
		t.Fatalf("%s got %v present=%v want %v", name, v, ok, want)
	}
}
func TestQueuesCompleteSnapshotsAndRecovery(t *testing.T) {
	a, f, _ := testAdapter(t)
	a.Config.Queues = true
	now := time.Now()
	future := now.Add(time.Hour)
	past := now.Add(-time.Minute)
	start := past.Add(-30 * time.Second)
	f.definitions = []uipath.QueueDefinition{{ID: 10, Name: "Work"}, {ID: 11, Name: "Empty"}}
	f.items = []uipath.QueueItem{
		{ID: 1, QueueDefinitionID: 10, Status: "New", CreationTime: start, DueDate: &past},
		{ID: 2, QueueDefinitionID: 10, Status: "New", CreationTime: start, DeferDate: &future},
		{ID: 3, QueueDefinitionID: 10, Status: "Failed", CreationTime: start, StartProcessing: &start, EndProcessing: &past, RetryNumber: 1, ProcessingExceptionType: "BusinessException"},
	}
	if e := a.Poll(context.Background()); e != nil {
		t.Fatal(e)
	}
	ms := snapshot(t, a)
	q := map[string]string{"uipath.queue.id": "10"}
	expect(t, ms, "uipath.queue.eligible", q, 1)
	expect(t, ms, "uipath.queue.deferred", q, 1)
	expect(t, ms, "uipath.queue.overdue", q, 1)
	expect(t, ms, "uipath.queue.retry_attempts", q, 1)
	expect(t, ms, "uipath.queue.processing.mean_duration", q, 30)
	expect(t, ms, "uipath.queue.items", map[string]string{"uipath.queue.id": "11", "uipath.queue.state": "New"}, 0)
	if _, ok := value(ms, "uipath.queue.processing.mean_duration", map[string]string{"uipath.queue.id": "11"}); ok {
		t.Fatal("empty queue has fabricated duration")
	}
	// Repeated reads are snapshots, not duplicate transition increments.
	if e := a.Poll(context.Background()); e != nil {
		t.Fatal(e)
	}
	expect(t, snapshot(t, a), "uipath.queue.retry_attempts", q, 1)
	f.queueErr = &uipath.HTTPError{Status: 403}
	if e := a.Poll(context.Background()); e == nil {
		t.Fatal("forbidden dataset hidden")
	}
	ms = snapshot(t, a)
	expect(t, ms, "uipath.collector.dataset.status", map[string]string{"uipath.dataset": "queues"}, 3)
	if _, ok := value(ms, "uipath.queue.items", nil); ok {
		t.Fatal("failed queue snapshot still exported")
	}
	f.queueErr = nil
	f.items = nil
	if e := a.Poll(context.Background()); e != nil {
		t.Fatal(e)
	}
	expect(t, snapshot(t, a), "uipath.queue.items", map[string]string{"uipath.queue.id": "10", "uipath.queue.state": "New"}, 0)
}
func TestQueueValidationAndStaleness(t *testing.T) {
	a, _, _ := testAdapter(t)
	a.Config.Queues = true
	now := time.Now()
	defs := []uipath.QueueDefinition{{ID: 10, Name: "Work"}}
	unknown := []uipath.QueueItem{{ID: 1, QueueDefinitionID: 99, Status: "New", CreationTime: now}}
	if validateQueues(defs, unknown, now) == nil {
		t.Fatal("unknown queue was silently ignored")
	}
	d := folderState()
	d.QueueDefinitions = defs
	d.QueuesSuccess = now.Add(-2 * a.Config.Freshness)
	if _, ok := value(a.folderMetrics("1", d, now), "uipath.queue.items", nil); ok {
		t.Fatal("stale queue emitted")
	}
	// Duration-less terminal records must not become fabricated successful coverage.
	if validateQueues(defs, []uipath.QueueItem{{ID: 1, QueueDefinitionID: 10, Status: "Successful", CreationTime: now}}, now) == nil {
		t.Fatal("missing completion accepted")
	}
}
func TestDiscoveryAndDisappearedFoldersRemainCoverageGaps(t *testing.T) {
	a, f, _ := testAdapter(t)
	ctx := context.Background()
	if e := a.Poll(ctx); e != nil {
		t.Fatal(e)
	}
	f.folders = []uipath.Folder{}
	if e := a.Poll(ctx); e != nil {
		t.Fatal(e)
	}
	ms := snapshot(t, a)
	expect(t, ms, "uipath.collector.folder.available", map[string]string{"uipath.folder.id": "1"}, 0)
	expect(t, ms, "uipath.folder.info", map[string]string{"uipath.folder.id": "1"}, 1)
	if _, ok := value(ms, "uipath.jobs", nil); ok {
		t.Fatal("disappeared folder emits job zeros")
	}
	f.discoveryErr = errors.New("unavailable")
	if e := a.Poll(ctx); e == nil {
		t.Fatal("discovery gap hidden")
	}
	expect(t, snapshot(t, a), "uipath.collector.discovery.success", nil, 0)
	f.discoveryErr = nil
	f.folders = nil
	if e := a.Poll(ctx); e != nil {
		t.Fatal(e)
	}
	expect(t, snapshot(t, a), "uipath.collector.folder.available", nil, 1)
	f.queueErr = &uipath.HTTPError{Status: 403}
	a.Config.Queues = false
	if e := a.Poll(ctx); e != nil {
		t.Fatal(e)
	}
	expect(t, snapshot(t, a), "uipath.collector.dataset.status", map[string]string{"uipath.dataset": "queues"}, 0)
}

func TestQueueRollingWindowExcludesOldHistory(t *testing.T) {
	a, _, _ := testAdapter(t)
	now := time.Now()
	end := now.Add(-48 * time.Hour)
	start := end.Add(-time.Minute)
	d := folderState()
	d.QueueDefinitions = []uipath.QueueDefinition{{ID: 10, Name: "Work"}}
	d.Queues = []uipath.QueueItem{{ID: 1, QueueDefinitionID: 10, Status: "Failed", CreationTime: start, StartProcessing: &start, EndProcessing: &end, RetryNumber: 2}}
	ms := a.queueMetrics("1", d, now)
	expect(t, ms, "uipath.queue.transactions", map[string]string{"uipath.queue.state": "Failed"}, 0)
	expect(t, ms, "uipath.queue.retry_attempts", nil, 0)
	expect(t, ms, "uipath.queue.processing.samples", nil, 0)
}
