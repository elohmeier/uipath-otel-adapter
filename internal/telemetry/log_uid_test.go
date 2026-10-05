package telemetry

import (
	"bytes"
	"testing"
	"time"

	"github.com/elohmeier/uipath-otel-adapter/internal/config"
	"github.com/elohmeier/uipath-otel-adapter/internal/uipath"
	logexport "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	logs "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

func uidFixture(t *testing.T, c config.Config, folder, key string, observed time.Time) []byte {
	t.Helper()
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	end := start.Add(time.Minute)
	job := uipath.Job{Key: key, State: "Successful", StartTime: &start, EndTime: &end}
	robot := uipath.RobotLog{ID: 10, JobKey: key, Level: "Info", TimeStamp: end}
	b, err := LogPayload(c, []*logs.LogRecord{
		JobLog(c, folder, job, observed),
		RobotLog(c, folder, robot, "robot/"+folder+"/10", true, observed),
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func decodeUIDFixture(t *testing.T, b []byte) *logexport.ExportLogsServiceRequest {
	t.Helper()
	r := new(logexport.ExportLogsServiceRequest)
	if err := proto.Unmarshal(b, r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestLogRecordUIDEmissionAndIdentity(t *testing.T) {
	c := config.Config{Installation: "demo", Tenant: "tenant-a", Environment: "test"}
	now := time.Now()
	old := uidFixture(t, c, "12", "job-a", now)
	for _, r := range decodeUIDFixture(t, old).ResourceLogs[0].ScopeLogs[0].LogRecords {
		if stringAttribute(r.Attributes, "log.record.uid") != "" {
			t.Fatal("development attribute emitted without opt-in")
		}
	}
	c.LogRecordUID = true
	current := decodeUIDFixture(t, uidFixture(t, c, "12", "job-a", now))
	records := current.ResourceLogs[0].ScopeLogs[0].LogRecords
	uids := []string{stringAttribute(records[0].Attributes, "log.record.uid"), stringAttribute(records[1].Attributes, "log.record.uid")}
	if uids[0] != "f6361d1943bf4d1f3e873db9e5615a4ac7da5c9f01a764722c5b71dafb2a3c2a" {
		t.Fatal("versioned UID contract changed")
	}
	if len(uids[0]) != 64 || len(uids[1]) != 64 || uids[0] == uids[1] {
		t.Fatal("expected distinct stable IDs for job and robot records")
	}
	if stringAttribute(records[0].Attributes, "uipath.event.id") != "job/12/job-a" || stringAttribute(records[1].Attributes, "uipath.event.id") != "robot/12/10" {
		t.Fatal("custom IDs must remain for existing consumers")
	}
	retry := decodeUIDFixture(t, uidFixture(t, c, "12", "job-a", now.Add(time.Hour)))
	for i, r := range retry.ResourceLogs[0].ScopeLogs[0].LogRecords {
		if stringAttribute(r.Attributes, "log.record.uid") != uids[i] {
			t.Fatal("collection time changed log identity")
		}
	}
	for _, change := range []func(*config.Config){
		func(c *config.Config) { c.Installation = "demo-2" },
		func(c *config.Config) { c.Tenant = "tenant-b" },
		func(c *config.Config) { c.Environment = "production" },
	} {
		other := c
		change(&other)
		for i, r := range decodeUIDFixture(t, uidFixture(t, other, "12", "job-a", now)).ResourceLogs[0].ScopeLogs[0].LogRecords {
			if stringAttribute(r.Attributes, "log.record.uid") == uids[i] {
				t.Fatal("source identity collision")
			}
		}
	}
	for _, identity := range [][2]string{{"13", "job-a"}, {"12", "job-b"}} {
		r := decodeUIDFixture(t, uidFixture(t, c, identity[0], identity[1], now)).ResourceLogs[0].ScopeLogs[0].LogRecords[0]
		if stringAttribute(r.Attributes, "log.record.uid") == uids[0] {
			t.Fatal("folder or source-record identity collision")
		}
	}
	upgraded, err := WithLogRecordUIDs(old)
	if err != nil || !proto.Equal(decodeUIDFixture(t, upgraded), current) {
		t.Fatalf("old queued payload differs from new emission: %v", err)
	}
	// Never replace existing IDs or rewrite other fields on an already upgraded batch.
	for _, a := range records[0].Attributes {
		if a.Key == "log.record.uid" {
			a.Value = Text("future-version-id")
		}
	}
	saved, _ := proto.Marshal(current)
	again, err := WithLogRecordUIDs(saved)
	if err != nil || !bytes.Equal(saved, again) {
		t.Fatal("existing UID or payload bytes changed")
	}
}

func TestLogRecordUIDRejectsMissingIdentity(t *testing.T) {
	if _, err := WithLogRecordUIDs([]byte("invalid protobuf")); err == nil {
		t.Fatal("invalid queue payload accepted")
	}
	c := config.Config{Installation: "demo", Tenant: "tenant-a", Environment: "test"}
	payload := uidFixture(t, c, "12", "job-a", time.Now())
	for _, key := range []string{"uipath.installation", "uipath.tenant.id", "deployment.environment.name"} {
		r := decodeUIDFixture(t, payload)
		for _, attr := range r.ResourceLogs[0].Resource.Attributes {
			if attr.Key == key {
				attr.Value = Text("")
			}
		}
		b, _ := proto.Marshal(r)
		if _, err := WithLogRecordUIDs(b); err == nil {
			t.Fatalf("missing %s silently received an ambiguous UID", key)
		}
	}
}
