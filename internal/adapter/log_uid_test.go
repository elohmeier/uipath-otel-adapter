package adapter

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/elohmeier/uipath-otel-adapter/internal/config"
	"github.com/elohmeier/uipath-otel-adapter/internal/state"
	"github.com/elohmeier/uipath-otel-adapter/internal/telemetry"
	"github.com/elohmeier/uipath-otel-adapter/internal/uipath"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
)

type uidSender func(context.Context, string, []byte) error

func (f uidSender) Send(ctx context.Context, signal string, payload []byte) error {
	return f(ctx, signal, payload)
}

func TestLogRecordUIDCrashReplay(t *testing.T) {
	for _, old := range []bool{false, true} {
		t.Run(map[bool]string{false: "new-batch", true: "pre-upgrade-batch"}[old], func(t *testing.T) {
			c := config.Config{Installation: "demo", Tenant: "tenant-a", Environment: "test", LogRecordUID: !old}
			now := time.Now()
			payload, err := telemetry.LogPayload(c, []*logs.LogRecord{
				telemetry.JobLog(c, "12", uipath.Job{Key: "job-a", State: "Successful", EndTime: &now}, now),
				telemetry.RobotLog(c, "12", uipath.RobotLog{ID: 10, TimeStamp: now}, "robot/12/10", false, now),
			})
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "state.db")
			s, err := state.Open(path, "test", 20)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { s.Close() }()
			if err := s.Save("checkpoint", "unchanged", []state.Envelope{{Signal: "logs", Payload: payload}}); err != nil {
				t.Fatal(err)
			}
			c.LogRecordUID = true
			var accepted []byte
			a := &Adapter{Config: c, Store: s, Sender: uidSender(func(_ context.Context, signal string, b []byte) error {
				accepted = append([]byte(nil), b...)
				panic("crash after receiver acceptance, before outbox ACK")
			})}
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("crash point not reached")
					}
				}()
				_ = a.Flush(context.Background())
			}()
			if old && bytes.Equal(accepted, payload) {
				t.Fatal("old batch was not upgraded before transmission")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = state.Open(path, "test", 20)
			if err != nil {
				t.Fatal(err)
			}
			// Changes to running configuration must not change stored source identity.
			c.Environment = "other-environment"
			calls := 0
			a = &Adapter{Config: c, Store: s, Sender: uidSender(func(_ context.Context, signal string, b []byte) error {
				calls++
				if !bytes.Equal(accepted, b) {
					t.Fatal("replay changed log UID or queued telemetry")
				}
				return nil
			})}
			for range 2 {
				if err := a.Flush(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			pending, quarantined := s.Counts()
			var checkpoint string
			if err := s.Load("checkpoint", &checkpoint); err != nil || checkpoint != "unchanged" || calls != 1 || pending != 0 || quarantined != 0 {
				t.Fatal("replay or ACK changed checkpoint/outbox semantics")
			}
		})
	}
}

func TestLogRecordUIDFailureRetainsBatch(t *testing.T) {
	a, _, _ := testAdapter(t)
	a.Config.LogRecordUID = true
	payload := []byte("invalid OTLP payload")
	if err := a.Store.Save("checkpoint", 1, []state.Envelope{{Signal: "logs", Payload: payload}, {Signal: "traces", Payload: []byte("trace")}}); err != nil {
		t.Fatal(err)
	}
	traces := 0
	a.Sender = uidSender(func(_ context.Context, signal string, _ []byte) error {
		if signal != "traces" {
			t.Fatal("invalid logs sent downstream")
		}
		traces++
		return nil
	})
	if err := a.Flush(context.Background()); err == nil {
		t.Fatal("UID preparation failure hidden")
	}
	pending, err := a.Store.Pending(time.Now().Add(time.Hour))
	if err != nil || traces != 1 || len(pending) != 1 || !bytes.Equal(pending[0].Payload, payload) {
		t.Fatal("failed payload lost or other signal blocked")
	}
	// A retryable receiver rejection also retains the upgraded bytes.
	c := config.Config{Installation: "demo", Tenant: "tenant-a", Environment: "test"}
	payload, err = telemetry.LogPayload(c, []*logs.LogRecord{telemetry.JobLog(c, "12", uipath.Job{Key: "job-a"}, time.Now())})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Store.Ack(pending[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := a.Store.Save("checkpoint", 1, []state.Envelope{{Signal: "logs", Payload: payload}}); err != nil {
		t.Fatal(err)
	}
	var sent []byte
	a.Sender = uidSender(func(_ context.Context, _ string, b []byte) error {
		sent = append([]byte(nil), b...)
		return errors.New("temporary receiver failure")
	})
	if err := a.Flush(context.Background()); err == nil {
		t.Fatal("receiver failure hidden")
	}
	pending, err = a.Store.Pending(time.Now().Add(time.Hour))
	if err != nil || len(pending) != 1 || !bytes.Equal(sent, pending[0].Payload) || bytes.Equal(sent, payload) {
		t.Fatal("retry did not persist upgraded payload")
	}
}
