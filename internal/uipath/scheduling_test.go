package uipath

import (
	"context"
	"github.com/elohmeier/uipath-otel-adapter/internal/config"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSchedulingProjections(t *testing.T) {
	includeUser := false
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			w.Write([]byte(`{"access_token":"example","expires_in":3600}`))
			return
		}
		projection := r.URL.Query().Get("$select")
		for _, secret := range []string{"Password", "LicenseKey", "ClientSecret", "InputArguments", "OutputArguments"} {
			if strings.Contains(projection+r.URL.Query().Get("$expand"), secret) {
				t.Fatal("unsafe projection")
			}
		}
		if r.URL.Path == "/odata/Jobs" {
			if strings.Contains(r.URL.Query().Get("$expand"), "Username") != includeUser {
				t.Error("username opt-in")
			}
			if !strings.Contains(projection, "HostMachineName") {
				t.Error("host missing")
			}
		} else {
			if r.Header.Get("X-UIPATH-OrganizationUnitId") != "" {
				t.Error("runtime request folder-scoped")
			}
			if r.URL.Path != "/odata/Sessions/UiPath.Server.Configuration.OData.GetMachineSessionRuntimes" {
				t.Error("wrong runtime endpoint")
			}
			if !strings.Contains(projection, "UsedRuntimes") {
				t.Error("capacity missing")
			}
		}
		w.Write([]byte(`{"value":[]}`))
	}))
	defer s.Close()
	cfg := config.Config{URL: s.URL, TokenURL: s.URL, PageSize: 10, MaxRecords: 20, Timeout: time.Second}
	c, _ := New(cfg)
	if _, e := c.Jobs(context.Background(), 42, time.Now()); e != nil {
		t.Fatal(e)
	}
	if _, e := c.MachineRuntimes(context.Background()); e != nil {
		t.Fatal(e)
	}
	includeUser = true
	cfg.IncludeRobotUsernames = true
	c, _ = New(cfg)
	if _, e := c.Jobs(context.Background(), 42, time.Now()); e != nil {
		t.Fatal(e)
	}
}
