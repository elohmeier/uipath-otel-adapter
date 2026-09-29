package uipath

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/elohmeier/uipath-otel-adapter/internal/config"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestPaginationFolderHeaderAndTokenRefresh(t *testing.T) {
	tokens, calls := 0, 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/identity/connect/token" {
			tokens++
			r.ParseForm()
			if r.Form.Get("grant_type") != "client_credentials" {
				t.Error("grant")
			}
			json.NewEncoder(w).Encode(map[string]any{"access_token": "test-token", "expires_in": 3600})
			return
		}
		calls++
		if calls == 1 {
			w.WriteHeader(401)
			return
		}
		if r.Header.Get("X-UIPATH-OrganizationUnitId") != "42" {
			t.Error("missing folder")
		}
		if r.URL.Query().Get("$skip") == "0" {
			w.Write([]byte(`{"value":[{"Id":1},{"Id":2}]}`))
		} else {
			w.Write([]byte(`{"value":[]}`))
		}
	}))
	defer s.Close()
	c, e := New(config.Config{URL: s.URL, TokenURL: s.URL + "/identity/connect/token", PageSize: 2, MaxRecords: 5, Timeout: time.Second})
	if e != nil {
		t.Fatal(e)
	}
	rows, e := c.List(context.Background(), "/odata/Jobs", 42, url.Values{})
	if e != nil || len(rows) != 2 || tokens != 2 {
		t.Fatalf("rows=%d tokens=%d error=%v", len(rows), tokens, e)
	}
}
func TestCapIsGap(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			w.Write([]byte(`{"access_token":"test","expires_in":3600}`))
			return
		}
		w.Write([]byte(`{"value":[{"Id":1},{"Id":2}]}`))
	}))
	defer s.Close()
	c, _ := New(config.Config{URL: s.URL, TokenURL: s.URL, PageSize: 2, MaxRecords: 2, Timeout: time.Second})
	_, e := c.List(context.Background(), "/odata/Jobs", 1, nil)
	if !errors.Is(e, ErrCapped) {
		t.Fatalf("expected cap error, got %v", e)
	}
}
func TestResponseBodiesNotDisclosed(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
		w.Write([]byte("secret fixture content"))
	}))
	defer s.Close()
	c, _ := New(config.Config{URL: s.URL, TokenURL: s.URL, Timeout: time.Second, PageSize: 10, MaxRecords: 100})
	_, e := c.Folders(context.Background())
	if e == nil || e.Error() != "OAuth HTTP 403" {
		t.Fatalf("unsafe error %v", e)
	}
}

func TestQueueProjectionAndWindow(t *testing.T) {
	since := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			w.Write([]byte(`{"access_token":"test","expires_in":3600}`))
			return
		}
		if r.Header.Get("X-UIPATH-OrganizationUnitId") != "42" {
			t.Error("queue request lost folder scope")
		}
		if r.URL.Path == "/odata/QueueDefinitions" {
			if r.URL.Query().Get("$select") != "Id,Name" {
				t.Error("unexpected inventory projection")
			}
			w.Write([]byte(`{"value":[{"Id":10,"Name":"Work"}]}`))
			return
		}
		want := "Id,QueueDefinitionId,Status,CreationTime,StartProcessing,EndProcessing,RetryNumber,ProcessingExceptionType,DeferDate,DueDate"
		if r.URL.Query().Get("$select") != want {
			t.Error("queue projection changed or includes payload")
		}
		if r.URL.Query().Get("$filter") != "Status eq 'New' or Status eq 'InProgress' or EndProcessing ge "+since.Format(time.RFC3339Nano) {
			t.Error("queue window excludes old active work or lacks completion bound")
		}
		w.Write([]byte(`{"value":[{"Id":1,"QueueDefinitionId":10,"Status":"New","CreationTime":"2026-01-01T00:00:00Z","DeferDate":null,"EndProcessing":null,"ProcessingExceptionType":null}]}`))
	}))
	defer server.Close()
	c, e := New(config.Config{URL: server.URL, TokenURL: server.URL, Timeout: time.Second, PageSize: 10, MaxRecords: 100})
	if e != nil {
		t.Fatal(e)
	}
	defs, e := c.QueueDefinitions(context.Background(), 42)
	if e != nil || len(defs) != 1 {
		t.Fatalf("inventory: %v", e)
	}
	items, e := c.QueueItems(context.Background(), 42, since)
	if e != nil || len(items) != 1 || items[0].EndProcessing != nil {
		t.Fatalf("items: %v", e)
	}
}
