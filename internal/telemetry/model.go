// Package telemetry models UiPath observations using the native OTLP data model.
package telemetry

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/elohmeier/uipath-otel-adapter/internal/config"
	"github.com/elohmeier/uipath-otel-adapter/internal/uipath"
	logexport "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricexport "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	traceexport "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	traces "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

const Version = "0.1.0-dev"
const SchemaURL = "https://opentelemetry.io/schemas/1.43.0"

var Bounds = []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 14400}

type Histogram struct {
	Count   uint64
	Sum     float64
	Buckets []uint64
}

func (h *Histogram) Observe(v float64) {
	if len(h.Buckets) != len(Bounds)+1 {
		h.Buckets = make([]uint64, len(Bounds)+1)
	}
	i := sort.SearchFloat64s(Bounds, v)
	h.Buckets[i]++
	h.Count++
	h.Sum += v
}
func Attrs(m map[string]string) []*common.KeyValue {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*common.KeyValue, 0, len(m))
	for _, k := range keys {
		out = append(out, &common.KeyValue{Key: k, Value: Text(m[k])})
	}
	return out
}
func Text(v string) *common.AnyValue {
	return &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: v}}
}
func IDs(installation, tenant, folder, job string) ([]byte, []byte) {
	h := sha256.Sum256([]byte("uipath-trace-v1\x00" + installation + "\x00" + tenant + "\x00" + folder + "\x00" + job))
	s := sha256.Sum256([]byte("uipath-span-v1\x00" + hex.EncodeToString(h[:16])))
	return h[:16], s[:8]
}
func resourceFor(c config.Config, service string) *resource.Resource {
	// This identifies a logical source partition, not a robot or a backend node.
	// The adapter has exactly one polling authority per installation/tenant.
	identity, _ := json.Marshal([]string{"uipath-source-v1", c.Installation, c.Tenant, c.Environment})
	// SemConv recommends this namespace for UUIDv5 IDs derived from inherent identity.
	namespace := []byte{0x4d, 0x63, 0x00, 0x9a, 0x8d, 0x0f, 0x11, 0xee, 0xaa, 0xd7, 0x4c, 0x79, 0x6e, 0xd8, 0xe3, 0x20}
	sum := sha1.Sum(append(namespace, identity...)) // UUIDv5 identity, not a security primitive.
	id := sum[:16]
	id[6] = (id[6] & 0x0f) | 0x50
	id[8] = (id[8] & 0x3f) | 0x80
	instance := fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:])
	return &resource.Resource{Attributes: Attrs(map[string]string{
		"service.name": service, "service.namespace": "uipath", "service.instance.id": instance,
		"deployment.environment.name": c.Environment, "uipath.installation": c.Installation, "uipath.tenant.id": c.Tenant,
	})}
}
func Scope() *common.InstrumentationScope {
	return &common.InstrumentationScope{Name: "github.com/elohmeier/uipath-otel-adapter", Version: Version}
}
func LogPayload(c config.Config, records []*logs.LogRecord) ([]byte, error) {
	r := &logexport.ExportLogsServiceRequest{ResourceLogs: []*logs.ResourceLogs{{Resource: resourceFor(c, "uipath-orchestrator"), SchemaUrl: SchemaURL, ScopeLogs: []*logs.ScopeLogs{{Scope: Scope(), LogRecords: records}}}}}
	if c.LogRecordUID {
		if _, err := addLogRecordUIDs(r); err != nil {
			return nil, err
		}
	}
	return proto.Marshal(r)
}
func TracePayload(c config.Config, spans []*traces.Span) ([]byte, error) {
	return proto.Marshal(&traceexport.ExportTraceServiceRequest{ResourceSpans: []*traces.ResourceSpans{{Resource: resourceFor(c, "uipath-orchestrator"), SchemaUrl: SchemaURL, ScopeSpans: []*traces.ScopeSpans{{Scope: Scope(), Spans: spans}}}}})
}
func MetricPayload(c config.Config, domain, self []*metrics.Metric) ([]byte, error) {
	return proto.Marshal(&metricexport.ExportMetricsServiceRequest{ResourceMetrics: []*metrics.ResourceMetrics{
		{Resource: resourceFor(c, "uipath-orchestrator"), SchemaUrl: SchemaURL, ScopeMetrics: []*metrics.ScopeMetrics{{Scope: Scope(), Metrics: domain}}},
		{Resource: resourceFor(c, "uipath-otel-adapter"), SchemaUrl: SchemaURL, ScopeMetrics: []*metrics.ScopeMetrics{{Scope: Scope(), Metrics: self}}},
	}})
}
func JobSpan(c config.Config, folder string, j uipath.Job) *traces.Span {
	if j.Key == "" || j.StartTime == nil || j.EndTime == nil || j.StartTime.IsZero() || j.EndTime.Before(*j.StartTime) {
		return nil
	}
	t, s := IDs(c.Installation, c.Tenant, folder, j.Key)
	status := &traces.Status{}
	if j.State == "Faulted" {
		status.Code = traces.Status_STATUS_CODE_ERROR
	}
	r := &traces.Span{TraceId: t, SpanId: s, Name: "uipath.job " + j.ReleaseName, Kind: traces.Span_SPAN_KIND_INTERNAL, StartTimeUnixNano: uint64(j.StartTime.UnixNano()), EndTimeUnixNano: uint64(j.EndTime.UnixNano()), Status: status, Attributes: Attrs(map[string]string{"uipath.folder.id": folder, "uipath.job.key": j.Key, "uipath.job.state": j.State, "uipath.process.name": j.ReleaseName, "uipath.trace.origin": "orchestrator_api"})}
	r.Attributes = append(r.Attributes, Assignment(c, j)...)
	return r
}
func JobLog(c config.Config, folder string, j uipath.Job, now time.Time) *logs.LogRecord {
	ts := now
	if j.EndTime != nil {
		ts = *j.EndTime
	}
	level := "INFO"
	if j.State == "Faulted" {
		level = "ERROR"
	}
	attrs := map[string]string{"uipath.folder.id": folder, "uipath.job.key": j.Key, "uipath.job.state": j.State, "uipath.process.name": j.ReleaseName, "uipath.event.kind": "job.completed", "uipath.event.id": "job/" + folder + "/" + j.Key}
	r := &logs.LogRecord{TimeUnixNano: uint64(ts.UnixNano()), ObservedTimeUnixNano: uint64(now.UnixNano()), SeverityNumber: Severity(level), SeverityText: level, Body: Text("UiPath job completed: " + j.State), Attributes: Attrs(attrs)}
	r.Attributes = append(r.Attributes, Assignment(c, j)...)
	if j.StartTime != nil && j.EndTime != nil && !j.EndTime.Before(*j.StartTime) {
		r.Attributes = append(r.Attributes, &common.KeyValue{Key: "uipath.job.duration", Value: &common.AnyValue{Value: &common.AnyValue_DoubleValue{DoubleValue: j.EndTime.Sub(*j.StartTime).Seconds()}}})
	}
	if JobSpan(c, folder, j) != nil {
		r.TraceId, r.SpanId = IDs(c.Installation, c.Tenant, folder, j.Key)
	}
	return r
}
func RobotLog(c config.Config, folder string, l uipath.RobotLog, eventID string, correlated bool, now time.Time) *logs.LogRecord {
	body := "Robot log (message collection disabled)"
	if c.IncludeMessages {
		body = l.Message
	}
	r := &logs.LogRecord{TimeUnixNano: uint64(l.TimeStamp.UnixNano()), ObservedTimeUnixNano: uint64(now.UnixNano()), SeverityNumber: Severity(l.Level), SeverityText: l.Level, Body: Text(body), Attributes: Attrs(map[string]string{"uipath.folder.id": folder, "uipath.job.key": l.JobKey, "uipath.process.name": l.ProcessName, "uipath.event.kind": "robot.log", "uipath.event.id": eventID})}
	if correlated && l.JobKey != "" {
		r.TraceId, r.SpanId = IDs(c.Installation, c.Tenant, folder, l.JobKey)
	}
	return r
}
func Severity(s string) logs.SeverityNumber {
	switch strings.ToLower(s) {
	case "trace", "verbose":
		return logs.SeverityNumber_SEVERITY_NUMBER_TRACE
	case "debug":
		return logs.SeverityNumber_SEVERITY_NUMBER_DEBUG
	case "info", "information":
		return logs.SeverityNumber_SEVERITY_NUMBER_INFO
	case "warn", "warning":
		return logs.SeverityNumber_SEVERITY_NUMBER_WARN
	case "error":
		return logs.SeverityNumber_SEVERITY_NUMBER_ERROR
	case "fatal", "critical":
		return logs.SeverityNumber_SEVERITY_NUMBER_FATAL
	default:
		return logs.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED
	}
}
func Gauge(name, unit string, value float64, a map[string]string, now time.Time) *metrics.Metric {
	return &metrics.Metric{Name: name, Unit: unit, Data: &metrics.Metric_Gauge{Gauge: &metrics.Gauge{DataPoints: []*metrics.NumberDataPoint{{Attributes: Attrs(a), TimeUnixNano: uint64(now.UnixNano()), Value: &metrics.NumberDataPoint_AsDouble{AsDouble: value}}}}}}
}
func Counter(name string, value uint64, a map[string]string, start, now time.Time) *metrics.Metric {
	return &metrics.Metric{Name: name, Unit: "{job}", Data: &metrics.Metric_Sum{Sum: &metrics.Sum{AggregationTemporality: metrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE, IsMonotonic: true, DataPoints: []*metrics.NumberDataPoint{{Attributes: Attrs(a), StartTimeUnixNano: uint64(start.UnixNano()), TimeUnixNano: uint64(now.UnixNano()), Value: &metrics.NumberDataPoint_AsInt{AsInt: int64(value)}}}}}}
}
func Hist(name string, h Histogram, a map[string]string, start, now time.Time) *metrics.Metric {
	return &metrics.Metric{Name: name, Unit: "s", Data: &metrics.Metric_Histogram{Histogram: &metrics.Histogram{AggregationTemporality: metrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE, DataPoints: []*metrics.HistogramDataPoint{{Attributes: Attrs(a), StartTimeUnixNano: uint64(start.UnixNano()), TimeUnixNano: uint64(now.UnixNano()), Count: h.Count, Sum: &h.Sum, ExplicitBounds: Bounds, BucketCounts: h.Buckets}}}}}
}

// Merge points with the same instrument name into a single OTLP metric.
func Merge(in []*metrics.Metric) []*metrics.Metric {
	var out []*metrics.Metric
	by := map[string]*metrics.Metric{}
	for _, m := range in {
		old := by[m.Name]
		if old == nil {
			by[m.Name] = m
			out = append(out, m)
			continue
		}
		switch {
		case m.GetGauge() != nil:
			old.GetGauge().DataPoints = append(old.GetGauge().DataPoints, m.GetGauge().DataPoints...)
		case m.GetSum() != nil:
			old.GetSum().DataPoints = append(old.GetSum().DataPoints, m.GetSum().DataPoints...)
		case m.GetHistogram() != nil:
			old.GetHistogram().DataPoints = append(old.GetHistogram().DataPoints, m.GetHistogram().DataPoints...)
		}
	}
	return out
}
