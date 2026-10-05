package telemetry

import (
	"testing"
	"time"

	"github.com/elohmeier/uipath-otel-adapter/internal/config"
	"github.com/elohmeier/uipath-otel-adapter/internal/uipath"
	common "go.opentelemetry.io/proto/otlp/common/v1"
)

func TestExecutionTargetAndAccountPrivacy(t *testing.T) {
	start, end := time.Now().Add(-time.Minute), time.Now()
	j := uipath.Job{Key: "job-a", State: "Successful", StartTime: &start, EndTime: &end, HostMachineName: "robot.example.com", Robot: &uipath.JobRobot{ID: 12, Name: "Example robot", Username: "example-account"}}
	for _, enabled := range []bool{false, true} {
		c := config.Config{IncludeRobotUsernames: enabled}
		for _, attrs := range [][]*common.KeyValue{JobLog(c, "1", j, end).Attributes, JobSpan(c, "1", j).Attributes} {
			values := map[string]string{}
			for _, a := range attrs {
				values[a.Key] = a.Value.GetStringValue()
			}
			if values["uipath.host.name"] != j.HostMachineName || values["uipath.robot.id"] != "12" || (values["uipath.robot.username"] != "") != enabled {
				t.Fatal("execution target or account privacy lost")
			}
			if _, exists := values["host.id"]; exists {
				t.Fatal("robot/template ID is not host.id")
			}
		}
	}
}
