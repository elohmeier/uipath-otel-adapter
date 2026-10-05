// Package config keeps deployment details outside the adapter's source tree.
package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	URL, TokenURL, ClientID, Secret, Scopes, DialAddress, CAFile                    string
	Installation, Tenant, Environment, StatePath, Listen                            string
	Endpoint, OTLPCA, OTLPClientCert, OTLPClientKey                                 string
	Headers                                                                         map[string]string
	FolderIDs                                                                       map[int64]bool
	PollInterval, Lookback, LogLookback, Overlap, Timeout, Freshness, QueueLookback time.Duration
	PageSize, MaxRecords, MaxPending                                                int
	Insecure, IncludeMessages, LogRecordUID, Logs, Queues, Once                     bool
	Runtimes, JobSnapshots, IncludeRobotUsernames                                   bool
}

func Load() (Config, error) {
	c := Config{
		URL: os.Getenv("UIPATH_URL"), TokenURL: os.Getenv("UIPATH_TOKEN_URL"),
		ClientID: os.Getenv("UIPATH_CLIENT_ID"), Secret: os.Getenv("UIPATH_CLIENT_SECRET"),
		Scopes:      env("UIPATH_SCOPES", "OR.Folders.Read OR.Jobs.Read OR.Monitoring.Read"),
		DialAddress: os.Getenv("UIPATH_DIAL_ADDRESS"), CAFile: os.Getenv("UIPATH_CA_FILE"),
		Installation: env("UIPATH_INSTALLATION", "default"), Tenant: env("UIPATH_TENANT", "default"),
		Environment: env("UIPATH_ENVIRONMENT", "development"), StatePath: env("STATE_PATH", ".local/state.db"),
		Listen:         env("LISTEN_ADDRESS", "127.0.0.1:8088"),
		Endpoint:       env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:14318"),
		OTLPCA:         os.Getenv("OTEL_EXPORTER_OTLP_CERTIFICATE"),
		OTLPClientCert: os.Getenv("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE"),
		OTLPClientKey:  os.Getenv("OTEL_EXPORTER_OTLP_CLIENT_KEY"),
		Headers:        map[string]string{}, FolderIDs: map[int64]bool{},
	}
	if f := os.Getenv("UIPATH_CLIENT_SECRET_FILE"); f != "" {
		b, e := os.ReadFile(f)
		if e != nil {
			return c, fmt.Errorf("read client secret file: %w", e)
		}
		c.Secret = strings.TrimSpace(string(b))
	}
	for _, v := range []struct {
		name     string
		dst      *time.Duration
		fallback string
	}{
		{"POLL_INTERVAL", &c.PollInterval, "60s"}, {"INITIAL_LOOKBACK", &c.Lookback, "24h"},
		{"QUEUE_HISTORY_LOOKBACK", &c.QueueLookback, "24h"}, {"LOG_INITIAL_LOOKBACK", &c.LogLookback, "1h"}, {"LOG_OVERLAP", &c.Overlap, "5m"},
		{"HTTP_TIMEOUT", &c.Timeout, "30s"}, {"METRIC_FRESHNESS", &c.Freshness, "3m"},
	} {
		d, e := time.ParseDuration(env(v.name, v.fallback))
		if e != nil || d <= 0 {
			return c, fmt.Errorf("%s must be a positive duration", v.name)
		}
		*v.dst = d
	}
	for _, v := range []struct {
		name     string
		dst      *int
		fallback string
	}{
		{"PAGE_SIZE", &c.PageSize, "100"}, {"MAX_RECORDS", &c.MaxRecords, "5000"}, {"MAX_PENDING_BATCHES", &c.MaxPending, "2000"},
	} {
		n, e := strconv.Atoi(env(v.name, v.fallback))
		if e != nil || n < 1 {
			return c, fmt.Errorf("%s must be positive", v.name)
		}
		*v.dst = n
	}
	for _, v := range []struct {
		name     string
		dst      *bool
		fallback string
	}{
		{"UIPATH_TLS_INSECURE", &c.Insecure, "false"}, {"INCLUDE_LOG_MESSAGES", &c.IncludeMessages, "false"},
		{"INCLUDE_LOG_RECORD_UID", &c.LogRecordUID, "false"},
		{"COLLECT_RUNTIMES", &c.Runtimes, "false"}, {"COLLECT_JOB_SNAPSHOTS", &c.JobSnapshots, "false"},
		{"INCLUDE_ROBOT_USERNAMES", &c.IncludeRobotUsernames, "false"},
		{"COLLECT_LOGS", &c.Logs, "true"}, {"COLLECT_QUEUES", &c.Queues, "false"},
	} {
		b, e := strconv.ParseBool(env(v.name, v.fallback))
		if e != nil {
			return c, fmt.Errorf("%s must be a boolean", v.name)
		}
		*v.dst = b
	}
	if c.Queues && os.Getenv("UIPATH_SCOPES") == "" {
		c.Scopes += " OR.Queues.Read"
	}
	if c.Runtimes && os.Getenv("UIPATH_SCOPES") == "" {
		c.Scopes += " OR.Robots.Read"
	}
	if c.PageSize > 1000 || c.MaxRecords > 10000 {
		return c, fmt.Errorf("PAGE_SIZE must be <=1000 and MAX_RECORDS <=10000")
	}
	for _, id := range strings.Split(os.Getenv("UIPATH_FOLDER_IDS"), ",") {
		if strings.TrimSpace(id) == "" {
			continue
		}
		n, e := strconv.ParseInt(strings.TrimSpace(id), 10, 64)
		if e != nil || n <= 0 {
			return c, fmt.Errorf("invalid folder ID")
		}
		c.FolderIDs[n] = true
	}
	for _, part := range strings.Split(os.Getenv("OTEL_EXPORTER_OTLP_HEADERS"), ",") {
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			return c, fmt.Errorf("invalid OTLP header")
		}
		decoded, e := url.PathUnescape(v)
		if e != nil {
			return c, fmt.Errorf("invalid OTLP header encoding")
		}
		c.Headers[strings.TrimSpace(k)] = decoded
	}
	if p := os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"); p != "" && p != "http/protobuf" {
		return c, fmt.Errorf("only OTLP http/protobuf is supported")
	}
	for _, v := range []struct{ name, raw string }{{"UIPATH_URL", c.URL}, {"OTEL_EXPORTER_OTLP_ENDPOINT", c.Endpoint}} {
		u, e := url.Parse(v.raw)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return c, fmt.Errorf("%s must be an HTTP(S) base URL without credentials, query or fragment", v.name)
		}
	}
	if c.TokenURL == "" {
		c.TokenURL = strings.TrimRight(c.URL, "/") + "/identity/connect/token"
	}
	u, e := url.Parse(c.TokenURL)
	if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return c, fmt.Errorf("invalid UIPATH_TOKEN_URL")
	}
	if c.ClientID == "" || c.Secret == "" {
		return c, fmt.Errorf("UIPATH_CLIENT_ID and a client secret are required")
	}
	return c, nil
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func HTTPClient(timeout time.Duration, ca string, insecure bool, cert, key string) (*http.Client, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: insecure} // Explicit development opt-in only.
	if ca != "" {
		b, e := os.ReadFile(ca)
		if e != nil {
			return nil, fmt.Errorf("read CA bundle: %w", e)
		}
		pool, e := x509.SystemCertPool()
		if e != nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("CA bundle contains no certificates")
		}
		tc.RootCAs = pool
	}
	if cert != "" || key != "" {
		pair, e := tls.LoadX509KeyPair(cert, key)
		if e != nil {
			return nil, fmt.Errorf("load client TLS certificate: %w", e)
		}
		tc.Certificates = []tls.Certificate{pair}
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = tc
	return &http.Client{Timeout: timeout, Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, nil
}
