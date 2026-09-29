package telemetry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/elohmeier/uipath-otel-adapter/internal/config"
	logexport "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricexport "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	traceexport "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

type Exporter struct {
	mu        sync.Mutex
	notBefore map[string]time.Time
	HTTP      *http.Client
	Endpoint  string
	Headers   map[string]string
}
type ExportError struct {
	Status     int
	Rejected   int64
	Permanent  bool
	RetryAfter time.Duration
}

func (e *ExportError) Error() string {
	if e.Rejected > 0 {
		return fmt.Sprintf("OTLP partial success: %d rejected records", e.Rejected)
	}
	return fmt.Sprintf("OTLP HTTP %d", e.Status)
}
func NewExporter(c config.Config) (*Exporter, error) {
	hc, e := config.HTTPClient(c.Timeout, c.OTLPCA, false, c.OTLPClientCert, c.OTLPClientKey)
	if e != nil {
		return nil, e
	}
	return &Exporter{HTTP: hc, Endpoint: c.Endpoint, Headers: c.Headers}, nil
}
func (e *Exporter) Send(ctx context.Context, signal string, payload []byte) error {
	if signal != "metrics" && signal != "logs" && signal != "traces" {
		return errors.New("invalid OTLP signal")
	}
	e.mu.Lock()
	delay := time.Until(e.notBefore[signal])
	e.mu.Unlock()
	if delay > 0 {
		return &ExportError{Status: 429, RetryAfter: delay}
	}
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(e.Endpoint, "/")+"/v1/"+signal, bytes.NewReader(payload))
	if err != nil {
		return errors.New("invalid OTLP endpoint")
	}
	for k, v := range e.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	resp, err := e.HTTP.Do(req)
	if err != nil {
		return errors.New("OTLP transport failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		delay := time.Duration(0)
		if n, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && n > 0 {
			delay = time.Duration(n) * time.Second
		}
		if t, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil && time.Until(t) > delay {
			delay = time.Until(t)
		}
		if delay > 0 {
			e.mu.Lock()
			if e.notBefore == nil {
				e.notBefore = map[string]time.Time{}
			}
			e.notBefore[signal] = time.Now().Add(delay)
			e.mu.Unlock()
		}
		return &ExportError{Status: resp.StatusCode, Permanent: resp.StatusCode != 429 && resp.StatusCode != 502 && resp.StatusCode != 503 && resp.StatusCode != 504, RetryAfter: delay}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return errors.New("read OTLP response failed")
	}
	var rejected int64
	switch signal {
	case "logs":
		r := new(logexport.ExportLogsServiceResponse)
		err = proto.Unmarshal(body, r)
		rejected = r.GetPartialSuccess().GetRejectedLogRecords()
	case "metrics":
		r := new(metricexport.ExportMetricsServiceResponse)
		err = proto.Unmarshal(body, r)
		rejected = r.GetPartialSuccess().GetRejectedDataPoints()
	case "traces":
		r := new(traceexport.ExportTraceServiceResponse)
		err = proto.Unmarshal(body, r)
		rejected = r.GetPartialSuccess().GetRejectedSpans()
	}
	if err != nil {
		return errors.New("invalid OTLP protobuf response")
	}
	if rejected > 0 {
		return &ExportError{Status: 200, Rejected: rejected, Permanent: true}
	}
	return nil
}
