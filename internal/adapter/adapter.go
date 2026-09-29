// Package adapter reconciles source datasets, checkpoints and telemetry.
package adapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"time"

	"github.com/elohmeier/uipath-otel-adapter/internal/config"
	"github.com/elohmeier/uipath-otel-adapter/internal/state"
	"github.com/elohmeier/uipath-otel-adapter/internal/telemetry"
	"github.com/elohmeier/uipath-otel-adapter/internal/uipath"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
	traces "go.opentelemetry.io/proto/otlp/trace/v1"
)

type Source interface {
	Folders(context.Context) ([]uipath.Folder, error)
	Jobs(context.Context, int64, time.Time) ([]uipath.Job, error)
	Logs(context.Context, int64, time.Time) ([]uipath.RobotLog, error)
	QueueDefinitions(context.Context, int64) ([]uipath.QueueDefinition, error)
	QueueItems(context.Context, int64, time.Time) ([]uipath.QueueItem, error)
}
type Sender interface {
	Send(context.Context, string, []byte) error
}
type Adapter struct {
	Config  config.Config
	Source  Source
	Store   *state.Store
	Sender  Sender
	Started time.Time
}
type Process struct {
	Completed                                 map[string]uint64
	Duration, Wait                            telemetry.Histogram
	LastSuccess, LastDuration, LastCompletion float64
}
type FolderState struct {
	QueueDefinitions                                                       []uipath.QueueDefinition
	Name                                                                   string
	Started, JobCursor, LogCursor, JobsSuccess, LogsSuccess, QueuesSuccess time.Time
	SeenJobs                                                               map[string]time.Time
	TraceJobs                                                              map[string]time.Time
	SeenLogs                                                               map[string]time.Time
	Processes                                                              map[string]*Process
	Jobs                                                                   []uipath.Job
	Queues                                                                 []uipath.QueueItem
}

func folderState() FolderState {
	return FolderState{SeenJobs: map[string]time.Time{}, TraceJobs: map[string]time.Time{}, SeenLogs: map[string]time.Time{}, Processes: map[string]*Process{}}
}
func Binding(c config.Config) string {
	h := sha256.Sum256([]byte(c.URL + "\x00" + c.Installation + "\x00" + c.Tenant))
	return hex.EncodeToString(h[:])
}
func terminal(j uipath.Job) bool {
	return j.State == "Successful" || j.State == "Faulted" || j.State == "Stopped"
}
func (a *Adapter) Poll(ctx context.Context) error {
	now := time.Now().UTC()
	folders, err := a.Source.Folders(ctx)
	var domain, self []*metrics.Metric
	discovery := 0.
	if err == nil {
		discovery = 1
	}
	self = append(self, telemetry.Gauge("uipath.collector.discovery.success", "1", discovery, nil, now))
	if err != nil {
		slog.Warn("folder discovery failed", "error", err)
	}
	if err == nil {
		inventory, availability, e := a.inventory(folders, now)
		if e != nil {
			return e
		}
		domain = append(domain, inventory...)
		self = append(self, availability...)
	}
	failures := 0
	for _, folder := range folders {
		if len(a.Config.FolderIDs) > 0 && !a.Config.FolderIDs[folder.ID] {
			continue
		}
		id := strconv.FormatInt(folder.ID, 10)
		key := "folder/" + id
		d := folderState()
		if e := a.Store.Load(key, &d); e != nil {
			return e
		}
		if d.Started.IsZero() {
			d.Started = now
		}
		d.Name = folder.Name

		for dataset, enabled := range map[string]bool{"jobs": true, "logs": a.Config.Logs, "queues": a.Config.Queues} {
			value := 0.
			if enabled {
				value = 1
			}
			if !enabled {
				self = append(self, telemetry.Gauge("uipath.collector.dataset.status", "", 0, map[string]string{"uipath.folder.id": id, "uipath.dataset": dataset}, now))
			}
			self = append(self, telemetry.Gauge("uipath.collector.dataset.enabled", "1", value, map[string]string{"uipath.folder.id": id, "uipath.dataset": dataset}, now))
		}
		attrs := map[string]string{"uipath.folder.id": id}
		since := d.JobCursor.Add(-a.Config.Overlap)
		if d.JobCursor.IsZero() {
			since = now.Add(-a.Config.Lookback)
		}
		jobs, e := a.Source.Jobs(ctx, folder.ID, since)
		if e == nil {
			e = a.jobs(key, id, &d, jobs, now, since)
		}
		if e != nil {
			failures++
			slog.Warn("dataset collection failed", "dataset", "jobs", "error", e)
			d = folderState()
			if x := a.Store.Load(key, &d); x != nil {
				return x
			}
			if d.Started.IsZero() {
				d.Started = now
			}
		}
		self = append(self, a.health("jobs", attrs, d.JobsSuccess, e, now)...)
		d.Name = folder.Name
		if a.Config.Logs {
			since := d.LogCursor.Add(-a.Config.Overlap)
			if d.LogCursor.IsZero() {
				since = now.Add(-a.Config.LogLookback)
			}
			records, e := a.Source.Logs(ctx, folder.ID, since)
			if e == nil {
				e = a.logs(key, id, &d, records, now, since)
			}
			if e != nil {
				failures++
				slog.Warn("dataset collection failed", "dataset", "logs", "error", e)
				d = folderState()
				if x := a.Store.Load(key, &d); x != nil {
					return x
				}
				if d.Started.IsZero() {
					d.Started = now
				}
			}
			self = append(self, a.health("logs", attrs, d.LogsSuccess, e, now)...)
		}
		d.Name = folder.Name
		if a.Config.Queues {
			lastQueueSuccess := d.QueuesSuccess
			definitions, e := a.Source.QueueDefinitions(ctx, folder.ID)
			var items []uipath.QueueItem
			if e == nil {
				items, e = a.Source.QueueItems(ctx, folder.ID, now.Add(-a.queueLookback()))
			}
			if e == nil {
				e = validateQueues(definitions, items, now)
			}
			if e == nil {
				d.QueueDefinitions = definitions
				d.Queues = items
				d.QueuesSuccess = now
				e = a.Store.Save(key, d, nil)
			}
			if e != nil {
				d.QueuesSuccess = lastQueueSuccess
			}
			self = append(self, a.health("queues", attrs, d.QueuesSuccess, e, now)...)
			if e != nil {
				failures++
				slog.Warn("dataset collection failed", "dataset", "queues", "error", e)
				d.QueuesSuccess = time.Time{} // Never present a failed snapshot as an empty queue.
			}
		}
		domain = append(domain, a.folderMetrics(id, d, now)...)
	}
	pending, quarantine := a.Store.Counts()
	self = append(self, telemetry.Gauge("uipath.collector.outbox.pending", "{batch}", float64(pending), nil, now), telemetry.Gauge("uipath.collector.outbox.quarantined", "{batch}", float64(quarantine), nil, now))
	payload, e := telemetry.MetricPayload(a.Config, telemetry.Merge(domain), telemetry.Merge(self))
	if e != nil {
		return e
	}
	if e = a.Store.Metrics(payload); e != nil {
		return e
	}
	slog.Info("collection cycle complete", "folders", len(folders), "failed_datasets", failures, "pending_batches", pending, "quarantined_batches", quarantine)
	if err != nil {
		return err
	}
	if failures > 0 {
		return fmt.Errorf("%d datasets incomplete", failures)
	}
	return nil
}
func (a *Adapter) health(dataset string, attrs map[string]string, last time.Time, err error, now time.Time) []*metrics.Metric {
	at := clone(attrs)
	at["uipath.dataset"] = dataset
	v := 1.
	if err != nil {
		v = 0
	}
	status := 1.
	if err != nil {
		status = 2
		var httpErr *uipath.HTTPError
		if errors.As(err, &httpErr) && httpErr.Status == 403 {
			status = 3
		}
	}
	ts := 0.
	if !last.IsZero() {
		ts = float64(last.Unix())
	}
	return []*metrics.Metric{telemetry.Gauge("uipath.collector.dataset.status", "", status, at, now), telemetry.Gauge("uipath.collector.scrape.success", "1", v, at, now), telemetry.Gauge("uipath.collector.last_success_time", "s", ts, at, now)}
}
func (a *Adapter) jobs(key, id string, d *FolderState, jobs []uipath.Job, now, since time.Time) error {
	var ls []*logs.LogRecord
	var spans []*traces.Span
	for _, j := range jobs {
		if j.Key == "" || j.CreationTime.IsZero() {
			return errors.New("job lacks stable key or creation timestamp")
		}
		if !terminal(j) {
			continue
		}
		if _, ok := d.SeenJobs[j.Key]; ok {
			continue
		}
		if j.EndTime == nil || j.EndTime.IsZero() {
			return errors.New("terminal job lacks end timestamp")
		}
		if j.EndTime.After(now.Add(time.Minute)) {
			return errors.New("job end timestamp is in the future")
		}
		d.SeenJobs[j.Key] = *j.EndTime
		record := telemetry.JobLog(a.Config, id, j, now)
		record.Attributes = append(record.Attributes, telemetry.Attrs(map[string]string{"uipath.folder.name": d.Name})...)
		ls = append(ls, record)
		if span := telemetry.JobSpan(a.Config, id, j); span != nil {
			span.Attributes = append(span.Attributes, telemetry.Attrs(map[string]string{"uipath.folder.name": d.Name})...)
			spans = append(spans, span)
			d.TraceJobs[j.Key] = *j.EndTime
		}
		p := d.Processes[j.ReleaseName]
		if p == nil {
			p = &Process{Completed: map[string]uint64{"Successful": 0, "Faulted": 0, "Stopped": 0}}
			d.Processes[j.ReleaseName] = p
		}
		if j.State == "Successful" && float64(j.EndTime.Unix()) > p.LastSuccess {
			p.LastSuccess = float64(j.EndTime.Unix())
		}
		if j.StartTime != nil && !j.EndTime.Before(*j.StartTime) && float64(j.EndTime.UnixNano()) > p.LastCompletion {
			p.LastCompletion = float64(j.EndTime.UnixNano())
			p.LastDuration = j.EndTime.Sub(*j.StartTime).Seconds()
		}
		// Historical bootstrap spans/logs are useful, but must not inflate live counters.
		if !j.EndTime.Before(d.Started) {
			p.Completed[j.State]++
			if j.StartTime != nil && !j.EndTime.Before(*j.StartTime) {
				p.Duration.Observe(j.EndTime.Sub(*j.StartTime).Seconds())
				if !j.StartTime.Before(j.CreationTime) {
					p.Wait.Observe(j.StartTime.Sub(j.CreationTime).Seconds())
				}
			}
		}
	}
	envs, e := a.envelopes(ls, spans)
	if e != nil {
		return e
	}
	d.Jobs = jobs
	d.JobsSuccess = now
	d.JobCursor = now
	// Retain identities throughout both source replay horizons (and correlation).
	horizon := now.Add(-max(a.Config.Lookback, a.Config.LogLookback) - a.Config.Overlap)
	for k, t := range d.SeenJobs {
		if t.Before(horizon) {
			delete(d.SeenJobs, k)
			delete(d.TraceJobs, k)
		}
	}
	return a.Store.Save(key, d, envs)
}
func (a *Adapter) logs(key, id string, d *FolderState, rows []uipath.RobotLog, now, since time.Time) error {
	var ls []*logs.LogRecord
	maxTime := d.LogCursor
	for _, l := range rows {
		if l.TimeStamp.IsZero() || !l.TimeStamp.After(since) || l.TimeStamp.After(now.Add(time.Minute)) {
			return errors.New("log timestamp violates requested window")
		}
		if l.ID <= 0 {
			return errors.New("log source has no stable positive ID; lossless polling requires a source fingerprint or native forwarding")
		}
		eventID := "robot/" + id + "/" + strconv.FormatInt(l.ID, 10)
		if l.TimeStamp.After(maxTime) {
			maxTime = l.TimeStamp
		}
		if seenTime, ok := d.SeenLogs[eventID]; ok {
			if !seenTime.Equal(l.TimeStamp) {
				return errors.New("log ID reused with a different timestamp; coverage incomplete")
			}
			continue
		}
		_, correlated := d.TraceJobs[l.JobKey]
		record := telemetry.RobotLog(a.Config, id, l, eventID, correlated, now)
		record.Attributes = append(record.Attributes, telemetry.Attrs(map[string]string{"uipath.folder.name": d.Name})...)
		ls = append(ls, record)
		d.SeenLogs[eventID] = l.TimeStamp
	}
	envs, e := a.envelopes(ls, nil)
	if e != nil {
		return e
	}
	// On an empty window use its start, not poll time: retain bounded late-arrival coverage.
	if maxTime.IsZero() {
		maxTime = since.Add(a.Config.Overlap)
	}
	d.LogCursor = maxTime
	d.LogsSuccess = now
	prune := d.LogCursor.Add(-a.Config.Overlap)
	for k, t := range d.SeenLogs {
		if !t.After(prune) {
			delete(d.SeenLogs, k)
		}
	}
	return a.Store.Save(key, d, envs)
}
func (a *Adapter) envelopes(ls []*logs.LogRecord, spans []*traces.Span) ([]state.Envelope, error) {
	var out []state.Envelope
	for len(ls) > 0 {
		n := min(100, len(ls))
		b, e := telemetry.LogPayload(a.Config, ls[:n])
		if e != nil {
			return nil, e
		}
		out = append(out, state.Envelope{Signal: "logs", Payload: b})
		ls = ls[n:]
	}
	for len(spans) > 0 {
		n := min(100, len(spans))
		b, e := telemetry.TracePayload(a.Config, spans[:n])
		if e != nil {
			return nil, e
		}
		out = append(out, state.Envelope{Signal: "traces", Payload: b})
		spans = spans[n:]
	}
	return out, nil
}
func (a *Adapter) folderMetrics(id string, d FolderState, now time.Time) []*metrics.Metric {
	var out []*metrics.Metric
	attrs := map[string]string{"uipath.folder.id": id}
	if !d.JobsSuccess.IsZero() && now.Sub(d.JobsSuccess) <= a.Config.Freshness {
		counts := map[string]int{"Pending": 0, "Running": 0, "Stopping": 0, "Terminating": 0, "Suspended": 0, "Resumed": 0}
		oldPending, oldRunning := 0., 0.
		for _, j := range d.Jobs {
			if terminal(j) {
				continue
			}
			counts[j.State]++
			if j.State == "Pending" {
				oldPending = max(oldPending, now.Sub(j.CreationTime).Seconds())
			}
			if j.State == "Running" && j.StartTime != nil {
				oldRunning = max(oldRunning, now.Sub(*j.StartTime).Seconds())
			}
		}
		keys := make([]string, 0, len(counts))
		for state := range counts {
			keys = append(keys, state)
		}
		sort.Strings(keys)
		for _, state := range keys {
			at := clone(attrs)
			at["uipath.job.state"] = state
			out = append(out, telemetry.Gauge("uipath.jobs", "{job}", float64(counts[state]), at, now))
		}
		out = append(out, telemetry.Gauge("uipath.job.oldest_pending_age", "s", oldPending, attrs, now), telemetry.Gauge("uipath.job.oldest_running_age", "s", oldRunning, attrs, now))
	}
	for name, p := range d.Processes {
		at := clone(attrs)
		at["uipath.process.name"] = name
		for _, state := range []string{"Successful", "Faulted", "Stopped"} {
			ac := clone(at)
			ac["uipath.job.state"] = state
			out = append(out, telemetry.Counter("uipath.jobs.completed", p.Completed[state], ac, d.Started, now))
		}
		if p.Duration.Count > 0 {
			out = append(out, telemetry.Hist("uipath.job.duration", p.Duration, at, d.Started, now))
		}
		if p.Wait.Count > 0 {
			out = append(out, telemetry.Hist("uipath.job.queue_wait", p.Wait, at, d.Started, now))
		}
		if p.LastSuccess > 0 {
			out = append(out, telemetry.Gauge("uipath.process.last_success_time", "s", p.LastSuccess, at, now))
		}
		if p.LastCompletion > 0 {
			out = append(out, telemetry.Gauge("uipath.job.last_duration", "s", p.LastDuration, at, now))
		}
	}
	if a.Config.Queues && !d.QueuesSuccess.IsZero() && now.Sub(d.QueuesSuccess) <= a.Config.Freshness {
		out = append(out, a.queueMetrics(id, d, now)...)
	}
	return out
}
func clone(m map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Flush attempts each due envelope independently; a failed signal does not block others.
func (a *Adapter) Flush(ctx context.Context) error {
	var errs []error
	payload, e := a.Store.TakeMetrics()
	if e != nil {
		return e
	}
	if len(payload) > 0 {
		if e = a.Sender.Send(ctx, "metrics", payload); e != nil {
			var ex *telemetry.ExportError
			if errors.As(e, &ex) && ex.Permanent {
				if x := a.Store.RejectMetrics(payload); x != nil {
					return x
				}
			}
			errs = append(errs, e)
		} else if e = a.Store.AckMetrics(payload); e != nil {
			return e
		}
	}
	pending, e := a.Store.Pending(time.Now())
	if e != nil {
		return e
	}
	blocked := map[string]bool{}
	for _, p := range pending {
		if blocked[p.Signal] {
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if e = a.Sender.Send(ctx, p.Signal, p.Payload); e != nil {
			var ex *telemetry.ExportError
			permanent := errors.As(e, &ex) && ex.Permanent
			delay := time.Duration(0)
			if ex != nil {
				delay = ex.RetryAfter
			}
			if x := a.Store.Fail(p, permanent, delay); x != nil {
				return x
			}
			errs = append(errs, e)
			if !permanent {
				blocked[p.Signal] = true
			}
		} else if e = a.Store.Ack(p.ID); e != nil {
			return e
		}
	}
	return errors.Join(errs...)
}
