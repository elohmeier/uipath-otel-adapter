package adapter

import (
	"context"
	"github.com/elohmeier/uipath-otel-adapter/internal/state"
	"github.com/elohmeier/uipath-otel-adapter/internal/uipath"
	"testing"
	"time"
)

func TestFailedCheckpointDoesNotAdvanceReportedSuccess(t *testing.T) {
	for _, dataset := range []string{"jobs", "logs"} {
		t.Run(dataset, func(t *testing.T) {
			a, f, _ := testAdapter(t)
			ctx := context.Background()
			previous := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
			d := folderState()
			d.Started = previous
			d.JobsSuccess = previous
			d.LogsSuccess = previous
			if e := a.Store.Save("folder/1", d, nil); e != nil {
				t.Fatal(e)
			}
			envelopes := make([]state.Envelope, 20)
			for i := range envelopes {
				envelopes[i] = state.Envelope{Signal: "logs", Payload: []byte("synthetic")}
			}
			if e := a.Store.Save("full", struct{}{}, envelopes); e != nil {
				t.Fatal(e)
			}
			end := time.Now().Add(-time.Second)
			start := end.Add(-time.Minute)
			if dataset == "jobs" {
				f.jobs = []uipath.Job{{Key: "job-a", ReleaseName: "Example process", State: "Successful", CreationTime: start, StartTime: &start, EndTime: &end}}
			} else {
				f.logs = []uipath.RobotLog{{ID: 1, TimeStamp: end, Level: "Info"}}
			}
			if e := a.Poll(ctx); e == nil {
				t.Fatal("expected full outbox")
			}
			labels := map[string]string{"uipath.dataset": dataset}
			expect(t, snapshot(t, a), "uipath.collector.scrape.success", labels, 0)
			expect(t, snapshot(t, a), "uipath.collector.last_success_time", labels, float64(previous.Unix()))
			var persisted FolderState
			if e := a.Store.Load("folder/1", &persisted); e != nil {
				t.Fatal(e)
			}
			last := persisted.JobsSuccess
			if dataset == "logs" {
				last = persisted.LogsSuccess
			}
			if !last.Equal(previous) {
				t.Fatal("failed commit changed persisted success")
			}
		})
	}
}
func TestMissingDurationIsOmittedButMeasuredZeroIsRetained(t *testing.T) {
	a, f, _ := testAdapter(t)
	end := time.Now().Add(-time.Minute)
	f.jobs = []uipath.Job{{Key: "stopped", ReleaseName: "Example process", State: "Stopped", CreationTime: end.Add(-time.Minute), EndTime: &end}}
	if e := a.Poll(context.Background()); e != nil {
		t.Fatal(e)
	}
	if _, ok := value(snapshot(t, a), "uipath.job.last_duration", nil); ok {
		t.Fatal("missing start time fabricated duration")
	}
	f.jobs = []uipath.Job{{Key: "zero", ReleaseName: "Example process", State: "Successful", CreationTime: end, StartTime: &end, EndTime: &end}}
	for range 2 {
		if e := a.Poll(context.Background()); e != nil {
			t.Fatal(e)
		}
		expect(t, snapshot(t, a), "uipath.job.last_duration", nil, 0)
	}
}
