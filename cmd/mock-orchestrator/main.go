// mock-orchestrator serves synthetic data only. Never use it as an auth example.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/elohmeier/uipath-otel-adapter/internal/uipath"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /identity/connect/token", func(w http.ResponseWriter, r *http.Request) {
		respond(w, map[string]any{"access_token": "synthetic-demo-token", "expires_in": 3600})
	})
	mux.HandleFunc("GET /odata/Folders", func(w http.ResponseWriter, r *http.Request) {
		respond(w, map[string]any{"value": []uipath.Folder{{ID: 1, Name: "Demo automations"}}})
	})
	mux.HandleFunc("GET /odata/Jobs", func(w http.ResponseWriter, r *http.Request) {
		now := time.Now().UTC()
		var rows []uipath.Job
		for n := now.Unix()/30 - 12; n < now.Unix()/30; n++ {
			end := time.Unix(n*30, 0).UTC()
			start := end.Add(-time.Duration(15+n%40) * time.Second)
			state := "Successful"
			if n%5 == 0 {
				state = "Faulted"
			}
			rows = append(rows, uipath.Job{ID: n, Key: key(n), ReleaseName: "Invoice processing", State: state, CreationTime: start.Add(-10 * time.Second), StartTime: &start, EndTime: &end})
		}
		start := now.Add(-45 * time.Second)
		rows = append(rows, uipath.Job{ID: 1, Key: key(1), ReleaseName: "Invoice processing", State: "Running", CreationTime: start.Add(-10 * time.Second), StartTime: &start}, uipath.Job{ID: 2, Key: key(2), ReleaseName: "Document routing", State: "Pending", CreationTime: now.Add(-20 * time.Second)})
		respond(w, map[string]any{"value": page(rows, r)})
	})
	mux.HandleFunc("GET /odata/RobotLogs", func(w http.ResponseWriter, r *http.Request) {
		now := time.Now().UTC()
		var rows []uipath.RobotLog
		since := time.Time{}
		if _, s, ok := strings.Cut(r.URL.Query().Get("$filter"), "TimeStamp gt "); ok {
			since, _ = time.Parse(time.RFC3339Nano, s)
		}
		for n := now.Unix()/30 - 12; n < now.Unix()/30; n++ {
			ts := time.Unix(n*30, 0).UTC()
			if !ts.After(since) {
				continue
			}
			level := "Info"
			msg := "Synthetic invoice processed"
			if n%5 == 0 {
				level = "Error"
				msg = "Synthetic validation failure"
			}
			rows = append(rows, uipath.RobotLog{ID: n, JobKey: key(n), TimeStamp: ts, Level: level, Message: msg, ProcessName: "Invoice processing"})
		}
		respond(w, map[string]any{"value": page(rows, r)})
	})
	mux.HandleFunc("GET /odata/QueueDefinitions", func(w http.ResponseWriter, r *http.Request) {
		respond(w, map[string]any{"value": page([]uipath.QueueDefinition{{ID: 10, Name: "Invoice work"}, {ID: 11, Name: "Empty queue"}}, r)})
	})
	mux.HandleFunc("GET /odata/QueueItems", func(w http.ResponseWriter, r *http.Request) {
		now := time.Now().UTC()
		start := now.Add(-2 * time.Minute)
		end := now.Add(-time.Minute)
		future := now.Add(time.Hour)
		past := now.Add(-time.Minute)
		rows := []uipath.QueueItem{
			{ID: 1, QueueDefinitionID: 10, Status: "New", CreationTime: now.Add(-10 * time.Minute), DueDate: &past},
			{ID: 2, QueueDefinitionID: 10, Status: "New", CreationTime: now.Add(-time.Hour), DeferDate: &future},
			{ID: 3, QueueDefinitionID: 10, Status: "InProgress", CreationTime: start, StartProcessing: &start},
			{ID: 4, QueueDefinitionID: 10, Status: "Successful", CreationTime: start, StartProcessing: &start, EndProcessing: &end},
			{ID: 5, QueueDefinitionID: 10, Status: "Failed", CreationTime: start, StartProcessing: &start, EndProcessing: &end, RetryNumber: 1, ProcessingExceptionType: "BusinessException"},
			{ID: 6, QueueDefinitionID: 10, Status: "Retried", CreationTime: start, StartProcessing: &start, EndProcessing: &end, ProcessingExceptionType: "ApplicationException"},
		}
		respond(w, map[string]any{"value": page(rows, r)})
	})
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}
func key(n int64) string { return fmt.Sprintf("%08x-0000-4000-8000-%012x", n, n) }
func respond(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
func page[T any](v []T, r *http.Request) []T {
	skip, _ := strconv.Atoi(r.URL.Query().Get("$skip"))
	top, _ := strconv.Atoi(r.URL.Query().Get("$top"))
	skip = max(0, min(skip, len(v)))
	if top <= 0 {
		top = len(v)
	}
	return v[skip:min(skip+top, len(v))]
}
