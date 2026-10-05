package telemetry

import (
	"github.com/elohmeier/uipath-otel-adapter/internal/config"
	"github.com/elohmeier/uipath-otel-adapter/internal/uipath"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	"strconv"
)

// These attributes describe a remote execution target, not the host of the
// logical Orchestrator resource. Account identities are explicitly opt-in.
func Assignment(c config.Config, j uipath.Job) []*common.KeyValue {
	a := map[string]string{}
	for k, v := range map[string]string{"uipath.host.name": j.HostMachineName, "uipath.job.priority": j.JobPriority, "uipath.runtime.type": j.RuntimeType, "uipath.job.source": j.SourceType} {
		if v != "" {
			a[k] = v
		}
	}
	if j.Robot != nil {
		if j.Robot.ID > 0 {
			a["uipath.robot.id"] = strconv.FormatInt(j.Robot.ID, 10)
		}
		if j.Robot.Name != "" {
			a["uipath.robot.name"] = j.Robot.Name
		}
		if c.IncludeRobotUsernames && j.Robot.Username != "" {
			a["uipath.robot.username"] = j.Robot.Username
		}
	}
	return Attrs(a)
}
