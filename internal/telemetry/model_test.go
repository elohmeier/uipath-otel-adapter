package telemetry

import (
	"context"
	"errors"
	metricexport "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	resource "go.opentelemetry.io/proto/otlp/resource/v1"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/elohmeier/uipath-otel-adapter/internal/config"
	"github.com/elohmeier/uipath-otel-adapter/internal/uipath"
	logexport "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	traceexport "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	traces "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestJobSpanAndLogCorrelation(t *testing.T) {
	c := config.Config{Installation: "demo", Tenant: "tenant-a"}
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	end := start.Add(90 * time.Second)
	j := uipath.Job{Key: "11111111-1111-4111-8111-111111111111", State: "Faulted", ReleaseName: "Invoice processing", StartTime: &start, EndTime: &end}
	span := JobSpan(c, "12", j)
	if span == nil || span.EndTimeUnixNano-span.StartTimeUnixNano != uint64(90*time.Second) || span.Status.Code != traces.Status_STATUS_CODE_ERROR {
		t.Fatal("incorrect job span")
	}
	b, e := TracePayload(c, []*traces.Span{span})
	if e != nil {
		t.Fatal(e)
	}
	r := new(traceexport.ExportTraceServiceRequest)
	if e = proto.Unmarshal(b, r); e != nil {
		t.Fatal(e)
	}
	got := r.ResourceSpans[0].ScopeSpans[0].Spans[0]
	l := JobLog(c, "12", j, end.Add(time.Second))
	if string(got.TraceId) != string(l.TraceId) || string(got.SpanId) != string(l.SpanId) {
		t.Fatal("correlation differs")
	}
	other, _ := IDs(c.Installation, "tenant-b", "12", j.Key)
	if string(other) == string(span.TraceId) {
		t.Fatal("tenant identity collision")
	}
	j.StartTime = nil
	if JobSpan(c, "12", j) != nil {
		t.Fatal("fabricated missing start time")
	}
	if len(JobLog(c, "12", j, end).TraceId) != 0 {
		t.Fatal("log points at nonexistent span")
	}
}
func TestHistogramUsesNonCumulativeBuckets(t *testing.T) {
	var h Histogram
	for _, v := range []float64{0.5, 1, 2, 50000} {
		h.Observe(v)
	}
	if h.Count != 4 || h.Buckets[0] != 2 || h.Buckets[1] != 1 || h.Buckets[len(h.Buckets)-1] != 1 {
		t.Fatal(h)
	}
}
func TestOTLPPartialSuccessIsPermanent(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/logs" || r.Header.Get("Content-Type") != "application/x-protobuf" {
			t.Error("wrong OTLP request")
		}
		b, _ := proto.Marshal(&logexport.ExportLogsServiceResponse{PartialSuccess: &logexport.ExportLogsPartialSuccess{RejectedLogRecords: 1}})
		_, _ = w.Write(b)
	}))
	defer s.Close()
	ex := Exporter{HTTP: s.Client(), Endpoint: s.URL}
	e := ex.Send(context.Background(), "logs", nil)
	var ee *ExportError
	if !errors.As(e, &ee) || !ee.Permanent || ee.Rejected != 1 {
		t.Fatalf("got %v", e)
	}
}
func TestRetryableAndPermanentHTTP(t *testing.T) {
	for _, status := range []int{429, 503, 400, 401} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
			defer s.Close()
			e := (&Exporter{HTTP: s.Client(), Endpoint: s.URL}).Send(context.Background(), "traces", nil)
			var ee *ExportError
			if !errors.As(e, &ee) || ee.Permanent != (status == 400 || status == 401) {
				t.Fatalf("unexpected %v", e)
			}
		})
	}
}
func TestMessageOptIn(t *testing.T) {
	l := uipath.RobotLog{ID: 10, Message: "private fixture content", TimeStamp: time.Now()}
	r := RobotLog(config.Config{}, "1", l, "id", false, time.Now())
	if r.Body.GetStringValue() == l.Message {
		t.Fatal("message opt-out ignored")
	}
	if len(r.TraceId) > 0 {
		t.Fatal("fabricated correlation")
	}
}

func TestRetryAfterDefersOnlyAffectedSignal(t *testing.T) {
	calls := map[string]int{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls[r.URL.Path]++
		if r.URL.Path == "/v1/metrics" {
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(429)
		}
	}))
	defer s.Close()
	e := &Exporter{HTTP: s.Client(), Endpoint: s.URL}
	e.Send(context.Background(), "metrics", nil)
	e.Send(context.Background(), "metrics", nil)
	if err := e.Send(context.Background(), "traces", nil); err != nil {
		t.Fatal(err)
	}
	if calls["/v1/metrics"] != 1 || calls["/v1/traces"] != 1 {
		t.Fatal("Retry-After or signal independence violated")
	}
}

func TestResourceIdentityAcrossSignalsAndSources(t *testing.T) {
	c := config.Config{Installation: "demo", Tenant: "tenant-a", Environment: "test"}
	attrs := func(r *resource.Resource) map[string]string {
		result := map[string]string{}
		for _, a := range r.Attributes {
			result[a.Key] = a.Value.GetStringValue()
		}
		return result
	}
	payload, err := MetricPayload(c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var metrics metricexport.ExportMetricsServiceRequest
	if err := proto.Unmarshal(payload, &metrics); err != nil {
		t.Fatal(err)
	}
	domain := attrs(metrics.ResourceMetrics[0].Resource)
	self := attrs(metrics.ResourceMetrics[1].Resource)
	if domain["service.name"] == self["service.name"] || domain["service.namespace"] != "uipath" {
		t.Fatal("service roles must remain distinct")
	}
	id := domain["service.instance.id"]
	if len(id) != 36 || id[14] != '5' || self["service.instance.id"] != id {
		t.Fatal("expected a stable source-scoped UUIDv5")
	}
	lp, _ := LogPayload(c, nil)
	var logs logexport.ExportLogsServiceRequest
	if err := proto.Unmarshal(lp, &logs); err != nil {
		t.Fatal(err)
	}
	tp, _ := TracePayload(c, nil)
	var spans traceexport.ExportTraceServiceRequest
	if err := proto.Unmarshal(tp, &spans); err != nil {
		t.Fatal(err)
	}
	if attrs(logs.ResourceLogs[0].Resource)["service.instance.id"] != id || attrs(spans.ResourceSpans[0].Resource)["service.instance.id"] != id {
		t.Fatal("domain identity differs by signal")
	}
	for _, other := range []config.Config{
		{Installation: "demo-2", Tenant: c.Tenant, Environment: c.Environment},
		{Installation: c.Installation, Tenant: "tenant-b", Environment: c.Environment},
		{Installation: c.Installation, Tenant: c.Tenant, Environment: "production"},
		{Installation: "demo/tenant-a", Tenant: "test", Environment: ""},
	} {
		if attrs(resourceFor(other, "uipath-orchestrator"))["service.instance.id"] == id {
			t.Fatal("different source identities collided")
		}
	}
	if attrs(resourceFor(c, "uipath-orchestrator"))["service.instance.id"] != id {
		t.Fatal("identity changed on repeat export")
	}
	if domain["uipath.installation"] != c.Installation || domain["uipath.tenant.id"] != c.Tenant {
		t.Fatal("source metadata lost")
	}
}
