package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/elohmeier/uipath-otel-adapter/internal/telemetry"
	"github.com/elohmeier/uipath-otel-adapter/internal/uipath"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
)

const maxSnapshotJobs = 1000
const maxSnapshotBytes = 1024 * 1024

type runtimeSource interface {
	MachineRuntimes(context.Context) ([]uipath.MachineRuntime, error)
}

// State codes deliberately separate disconnected, zero capacity and missing
// observations. A successful read alone cannot establish a usable runtime.
func runtimeStatus(r uipath.MachineRuntime, now time.Time, freshness time.Duration) float64 {
	if r.MaintenanceMode == "Enabled" {
		return 4
	}
	if r.Status == "Disconnected" {
		return 5
	}
	if r.IsUnresponsive {
		return 6
	}
	if now.Sub(r.ReportingTime) > freshness || r.Status == "Unknown" {
		return 0
	}
	if *r.Runtimes == 0 {
		return 7
	}
	if r.Status == "Busy" && *r.UsedRuntimes == 0 {
		return 0
	}
	if *r.UsedRuntimes == 0 {
		return 1
	}
	if *r.UsedRuntimes < *r.Runtimes {
		return 2
	}
	return 3
}

func validateRuntimes(rows []uipath.MachineRuntime, now time.Time) error {
	seen := map[string]bool{}
	for _, r := range rows {
		key := fmt.Sprintf("%d/%s", r.SessionID, r.RuntimeType)
		if r.SessionID <= 0 || r.MachineID <= 0 || r.HostMachineName == "" || r.RuntimeType == "" || seen[key] || r.Runtimes == nil || r.UsedRuntimes == nil || *r.Runtimes < 0 || *r.UsedRuntimes < 0 || *r.UsedRuntimes > *r.Runtimes || r.ReportingTime.IsZero() || r.ReportingTime.After(now.Add(time.Minute)) {
			return errors.New("invalid or duplicate machine runtime; coverage incomplete")
		}
		seen[key] = true
		switch r.Status {
		case "Available", "Busy", "Disconnected", "Unknown":
		default:
			return errors.New("unknown runtime connection status")
		}
		switch r.MaintenanceMode {
		case "Default", "Enabled":
		default:
			return errors.New("unknown runtime maintenance mode")
		}
	}
	return nil
}

func (a *Adapter) runtimes(ctx context.Context, now time.Time) (domain, self []*metrics.Metric, err error) {
	status := 0.
	var last time.Time
	if err = a.Store.Load("runtimes/last-success", &last); err != nil {
		return
	}
	if a.Config.Runtimes {
		source, ok := a.Source.(runtimeSource)
		var rows []uipath.MachineRuntime
		if !ok {
			err = errors.New("source does not support machine runtimes")
		} else {
			rows, err = source.MachineRuntimes(ctx)
		}
		if err == nil {
			err = validateRuntimes(rows, now)
		}
		if err == nil {
			err = a.Store.Save("runtimes/last-success", now, nil)
		}
		status = 1
		if err != nil {
			status = 2
			var h *uipath.HTTPError
			if errors.As(err, &h) && h.Status == 403 {
				status = 3
			}
		} else {
			last = now
			for _, r := range rows {
				attrs := map[string]string{"uipath.session.id": strconv.FormatInt(r.SessionID, 10), "uipath.machine.id": strconv.FormatInt(r.MachineID, 10), "uipath.host.name": r.HostMachineName, "uipath.runtime.type": r.RuntimeType}
				for _, m := range []struct {
					name, unit string
					value      float64
				}{
					{"uipath.runtime.status", "", runtimeStatus(r, now, a.Config.Freshness)},
					{"uipath.runtime.capacity", "{slot}", float64(*r.Runtimes)},
					{"uipath.runtime.used", "{slot}", float64(*r.UsedRuntimes)},
					{"uipath.runtime.last_heartbeat", "s", float64(r.ReportingTime.UnixMilli()) / 1000},
					{"uipath.runtime.observed_time", "s", float64(now.UnixMilli()) / 1000},
				} {
					domain = append(domain, telemetry.Gauge(m.name, m.unit, m.value, attrs, now))
				}
			}
			domain = append(domain, telemetry.Gauge("uipath.runtime.observed", "{runtime}", float64(len(rows)), nil, now))
		}
	}
	ts := 0.
	if !last.IsZero() {
		ts = float64(last.UnixMilli()) / 1000
	}
	self = append(self, telemetry.Gauge("uipath.collector.runtimes.status", "", status, nil, now), telemetry.Gauge("uipath.collector.runtimes.last_success_time", "s", ts, nil, now), telemetry.Gauge("uipath.collector.freshness", "s", a.Config.Freshness.Seconds(), nil, now))
	return
}

type activeJob struct {
	JobKey   string     `json:"job_key"`
	Process  string     `json:"process"`
	FolderID string     `json:"folder_id"`
	Folder   string     `json:"folder"`
	Host     string     `json:"host"`
	Robot    string     `json:"robot"`
	Username string     `json:"robot_username,omitempty"`
	State    string     `json:"state"`
	Priority string     `json:"priority"`
	Runtime  string     `json:"runtime"`
	Created  time.Time  `json:"created_at"`
	Started  *time.Time `json:"started_at,omitempty"`
	Elapsed  *float64   `json:"elapsed_seconds,omitempty"`
	Waiting  *float64   `json:"waiting_seconds,omitempty"`
}

func (a *Adapter) activeJob(folder uipath.Folder, j uipath.Job, now time.Time) activeJob {
	r := activeJob{JobKey: j.Key, Process: j.ReleaseName, FolderID: strconv.FormatInt(folder.ID, 10), Folder: folder.Name, Host: j.HostMachineName, State: j.State, Priority: j.JobPriority, Runtime: j.RuntimeType, Created: j.CreationTime, Started: j.StartTime}
	if j.Robot != nil {
		r.Robot = j.Robot.Name
		if a.Config.IncludeRobotUsernames {
			r.Username = j.Robot.Username
		}
	}
	if j.StartTime != nil {
		v := max(0., now.Sub(*j.StartTime).Seconds())
		r.Elapsed = &v
	}
	if j.State == "Pending" {
		v := max(0., now.Sub(j.CreationTime).Seconds())
		r.Waiting = &v
	}
	return r
}

// One document is an atomic view for this source. Empty and failed observations
// replace earlier views. Consumers select the newest record before testing status.
func (a *Adapter) snapshot(rows map[string]activeJob, complete bool, now time.Time) error {
	status := "complete"
	if !complete {
		status = "incomplete"
	}
	if len(rows) > maxSnapshotJobs {
		status = "capped"
	}
	b, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	if len(b) > maxSnapshotBytes {
		status = "capped"
	}
	if status != "complete" {
		b = []byte("{}")
	}
	count := len(rows)
	if status != "complete" {
		count = 0
	}
	r := &logs.LogRecord{TimeUnixNano: uint64(now.UnixNano()), ObservedTimeUnixNano: uint64(now.UnixNano()), SeverityNumber: logs.SeverityNumber_SEVERITY_NUMBER_INFO, SeverityText: "INFO", Body: telemetry.Text(string(b)), Attributes: telemetry.Attrs(map[string]string{
		"uipath.folder.id": "_all", "uipath.event.kind": "jobs.snapshot", "uipath.event.id": "snapshot/" + strconv.FormatInt(now.UnixNano(), 10), "uipath.snapshot.status": status,
		"uipath.snapshot.valid_until": now.Add(a.Config.Freshness).Format(time.RFC3339Nano),
	})}
	r.Attributes = append(r.Attributes, &common.KeyValue{Key: "uipath.snapshot.jobs", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: int64(count)}}})
	envs, err := a.envelopes([]*logs.LogRecord{r}, nil)
	if err != nil {
		return err
	}
	if err = a.Store.Save("snapshot/jobs", now, envs); err != nil {
		return err
	}
	if status != "complete" {
		return errors.New("active job snapshot incomplete or capped")
	}
	return nil
}
