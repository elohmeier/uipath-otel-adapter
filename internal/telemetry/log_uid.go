package telemetry

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	logexport "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
)

// WithLogRecordUIDs upgrades queued logs when the operator opts in. Use identity
// from the saved payload, never current configuration, so crash/replay and later
// environment changes cannot change a queued record's UID. Existing UIDs survive.
func WithLogRecordUIDs(payload []byte) ([]byte, error) {
	r := new(logexport.ExportLogsServiceRequest)
	if err := proto.Unmarshal(payload, r); err != nil {
		return nil, errors.New("cannot decode queued OTLP logs for record UID")
	}
	changed, err := addLogRecordUIDs(r)
	if err != nil {
		return nil, err
	}
	if !changed {
		return payload, nil
	}
	return proto.Marshal(r)
}

func addLogRecordUIDs(r *logexport.ExportLogsServiceRequest) (bool, error) {
	changed := false
	for _, resource := range r.ResourceLogs {
		identity := resource.GetResource().GetAttributes()
		for _, scope := range resource.ScopeLogs {
			for _, record := range scope.LogRecords {
				if stringAttribute(record.Attributes, "log.record.uid") != "" {
					continue
				}
				// SemConv 1.43.0 log.record.uid is Development/Opt-In. A JSON
				// tuple avoids collisions between source/key boundaries.
				parts := []string{"uipath-log-v1",
					stringAttribute(identity, "uipath.installation"),
					stringAttribute(identity, "uipath.tenant.id"),
					stringAttribute(identity, "deployment.environment.name"),
					stringAttribute(record.Attributes, "uipath.folder.id"),
					stringAttribute(record.Attributes, "uipath.event.id")}
				for _, part := range parts[1:] {
					if part == "" {
						return false, errors.New("OTLP log record lacks source identity for record UID")
					}
				}
				b, _ := json.Marshal(parts) // A string slice cannot fail to marshal.
				sum := sha256.Sum256(b)
				uid := &common.KeyValue{Key: "log.record.uid", Value: Text(hex.EncodeToString(sum[:]))}
				// Replace an empty UID instead of introducing a duplicate key.
				found := false
				for i, attribute := range record.Attributes {
					if attribute.Key == uid.Key {
						record.Attributes[i], found = uid, true
						break
					}
				}
				if !found {
					record.Attributes = append(record.Attributes, uid)
				}
				changed = true
			}
		}
	}
	return changed, nil
}

func stringAttribute(attrs []*common.KeyValue, key string) string {
	for _, attr := range attrs {
		if attr.Key == key {
			return attr.Value.GetStringValue()
		}
	}
	return ""
}
