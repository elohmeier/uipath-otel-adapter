package adapter

import (
	"github.com/elohmeier/uipath-otel-adapter/internal/telemetry"
	"github.com/elohmeier/uipath-otel-adapter/internal/uipath"
	metrics "go.opentelemetry.io/proto/otlp/metrics/v1"
	"strconv"
	"time"
)

// Remember authorized folders across restarts: loss of visibility is a coverage
// gap until the folder reappears or an operator explicitly removes it from scope.
func (a *Adapter) inventory(folders []uipath.Folder, now time.Time) ([]*metrics.Metric, []*metrics.Metric, error) {
	known := map[string]string{}
	if err := a.Store.Load("inventory/folders", &known); err != nil {
		return nil, nil, err
	}
	current := map[string]bool{}
	for _, f := range folders {
		if len(a.Config.FolderIDs) > 0 && !a.Config.FolderIDs[f.ID] {
			continue
		}
		id := strconv.FormatInt(f.ID, 10)
		name := f.Name
		if name == "" {
			name = id
		}
		known[id] = name
		current[id] = true
	}
	for id := range a.Config.FolderIDs {
		key := strconv.FormatInt(id, 10)
		if _, ok := known[key]; !ok {
			known[key] = key
		}
	}
	var domain, self []*metrics.Metric
	for id, name := range known {
		n, _ := strconv.ParseInt(id, 10, 64)
		if len(a.Config.FolderIDs) > 0 && !a.Config.FolderIDs[n] {
			continue
		}
		domain = append(domain, telemetry.Gauge("uipath.folder.info", "", 1, map[string]string{"uipath.folder.id": id, "uipath.folder.name": name}, now))
		available := 0.
		if current[id] {
			available = 1
		}
		self = append(self, telemetry.Gauge("uipath.collector.folder.available", "1", available, map[string]string{"uipath.folder.id": id}, now))
	}
	if err := a.Store.Save("inventory/folders", known, nil); err != nil {
		return nil, nil, err
	}
	return domain, self, nil
}
