package config

import (
	"os"
	"path/filepath"
	"testing"
)

func base(t *testing.T) {
	t.Helper()
	t.Setenv("UIPATH_URL", "https://orchestrator.example.com")
	t.Setenv("UIPATH_CLIENT_ID", "test-client")
	t.Setenv("UIPATH_CLIENT_SECRET", "test-secret")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
}
func TestPrivateSecretFileTakesPrecedence(t *testing.T) {
	base(t)
	f := filepath.Join(t.TempDir(), "secret")
	os.WriteFile(f, []byte("file-secret\n"), 0600)
	t.Setenv("UIPATH_CLIENT_SECRET_FILE", f)
	c, e := Load()
	if e != nil {
		t.Fatal(e)
	}
	if c.Secret != "file-secret" || c.Insecure || c.IncludeMessages {
		t.Fatal("unsafe defaults or wrong secret source")
	}
}
func TestRejectInvalidProtocolAndCredentialsInURL(t *testing.T) {
	base(t)
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
	if _, e := Load(); e == nil {
		t.Fatal("unsupported transport accepted")
	}
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	t.Setenv("UIPATH_URL", "https://user:password@orchestrator.example.com")
	if _, e := Load(); e == nil {
		t.Fatal("URL credentials accepted")
	}
}

func TestLogRecordUIDOptIn(t *testing.T) {
	base(t)
	for _, value := range []string{"", "false", "true", "invalid"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("INCLUDE_LOG_RECORD_UID", value)
			c, err := Load()
			if value == "invalid" {
				if err == nil {
					t.Fatal("invalid opt-in accepted")
				}
				return
			}
			if err != nil || c.LogRecordUID != (value == "true") {
				t.Fatalf("unexpected opt-in: %v, %v", c.LogRecordUID, err)
			}
		})
	}
}
