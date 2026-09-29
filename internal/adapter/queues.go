package adapter

import (
	"errors"
	"strconv"
	"time"

	"github.com/elohmeier/uipath-otel-adapter/internal/telemetry"
	"github.com/elohmeier/uipath-otel-adapter/internal/uipath"
	metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func (a *Adapter) queueLookback() time.Duration {
	if a.Config.QueueLookback > 0 {
		return a.Config.QueueLookback
	}
	return 24 * time.Hour
}
func validateQueues(defs []uipath.QueueDefinition, items []uipath.QueueItem, now time.Time) error {
	known := map[int64]bool{}
	for _, d := range defs {
		if d.ID <= 0 || known[d.ID] {
			return errors.New("invalid or duplicate queue identity")
		}
		known[d.ID] = true
	}
	seen := map[int64]bool{}
	for _, q := range items {
		if q.ID <= 0 || seen[q.ID] || !known[q.QueueDefinitionID] || q.CreationTime.IsZero() || q.CreationTime.After(now.Add(time.Minute)) || q.RetryNumber < 0 {
			return errors.New("invalid queue item identity or metadata; coverage incomplete")
		}
		seen[q.ID] = true
		switch q.Status {
		case "New", "InProgress", "Successful", "Failed", "Abandoned", "Retried", "Deleted":
		default:
			return errors.New("unknown queue item state; coverage incomplete")
		}
		if q.Status != "New" && q.Status != "InProgress" && (q.EndProcessing == nil || q.EndProcessing.IsZero()) {
			return errors.New("terminal queue item has no completion time")
		}
		if q.EndProcessing != nil && (q.EndProcessing.After(now.Add(time.Minute)) || (q.StartProcessing != nil && q.EndProcessing.Before(*q.StartProcessing))) {
			return errors.New("invalid queue processing interval")
		}
	}
	return nil
}

// Queue metrics are bounded snapshots, not cumulative transition counters.
// The folder dimension preserves authorization context; consumers must deduplicate
// linked queue views by installation, tenant and queue ID before fleet totals.
func (a *Adapter) queueMetrics(folder string, d FolderState, now time.Time) []*metrics.Metric {
	var out []*metrics.Metric
	since := now.Add(-a.queueLookback())
	for _, def := range d.QueueDefinitions {
		attrs := map[string]string{"uipath.folder.id": folder, "uipath.queue.id": strconv.FormatInt(def.ID, 10)}
		info := clone(attrs)
		info["uipath.queue.name"] = def.Name
		if def.Name == "" {
			info["uipath.queue.name"] = strconv.FormatInt(def.ID, 10)
		}
		out = append(out, telemetry.Gauge("uipath.queue.info", "", 1, info, now))
		emit := func(name, unit string, value float64) {
			out = append(out, telemetry.Gauge(name, unit, value, attrs, now))
		}
		states := map[string]float64{"New": 0, "InProgress": 0}
		outcomes := map[string]float64{"Successful": 0, "Failed": 0, "Abandoned": 0, "Retried": 0, "Deleted": 0}
		failures := map[string]float64{"BusinessException": 0, "ApplicationException": 0, "Unknown": 0}
		var eligible, deferred, oldest, overdue, retried, samples, durationSum, durationMax float64
		for _, q := range d.Queues {
			if q.QueueDefinitionID != def.ID {
				continue
			}
			active := q.Status == "New" || q.Status == "InProgress"
			if active {
				states[q.Status]++
				if q.DueDate != nil && q.DueDate.Before(now) {
					overdue++
				}
				if q.Status == "New" {
					if q.DeferDate != nil && q.DeferDate.After(now) {
						deferred++
					} else {
						eligible++
						ready := q.CreationTime
						if q.DeferDate != nil && q.DeferDate.After(ready) {
							ready = *q.DeferDate
						}
						oldest = max(oldest, now.Sub(ready).Seconds())
					}
				}
				continue
			}
			if q.EndProcessing == nil || q.EndProcessing.Before(since) || q.EndProcessing.After(now) {
				continue
			}
			outcomes[q.Status]++
			if q.RetryNumber > 0 {
				retried++
			}
			if q.Status == "Failed" || q.Status == "Retried" {
				kind := q.ProcessingExceptionType
				if kind != "BusinessException" && kind != "ApplicationException" {
					kind = "Unknown"
				}
				failures[kind]++
			}
			if q.StartProcessing != nil && !q.StartProcessing.IsZero() && !q.EndProcessing.Before(*q.StartProcessing) {
				duration := q.EndProcessing.Sub(*q.StartProcessing).Seconds()
				samples++
				durationSum += duration
				durationMax = max(durationMax, duration)
			}
		}
		for status, value := range states {
			at := clone(attrs)
			at["uipath.queue.state"] = status
			out = append(out, telemetry.Gauge("uipath.queue.items", "{item}", value, at, now))
		}
		for status, value := range outcomes {
			at := clone(attrs)
			at["uipath.queue.state"] = status
			out = append(out, telemetry.Gauge("uipath.queue.transactions", "{item}", value, at, now))
		}
		for kind, value := range failures {
			at := clone(attrs)
			at["uipath.queue.exception.type"] = kind
			out = append(out, telemetry.Gauge("uipath.queue.exceptions", "{item}", value, at, now))
		}
		emit("uipath.queue.eligible", "{item}", eligible)
		emit("uipath.queue.deferred", "{item}", deferred)
		emit("uipath.queue.oldest_pending_age", "s", oldest)
		emit("uipath.queue.overdue", "{item}", overdue)
		emit("uipath.queue.retry_attempts", "{item}", retried)
		emit("uipath.queue.history_window", "s", a.queueLookback().Seconds())
		emit("uipath.queue.processing.samples", "{item}", samples)
		if samples > 0 {
			emit("uipath.queue.processing.mean_duration", "s", durationSum/samples)
			emit("uipath.queue.processing.max_duration", "s", durationMax)
		}
	}
	return out
}
