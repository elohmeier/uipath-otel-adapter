package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/elohmeier/uipath-otel-adapter/internal/adapter"
	"github.com/elohmeier/uipath-otel-adapter/internal/config"
	"github.com/elohmeier/uipath-otel-adapter/internal/state"
	"github.com/elohmeier/uipath-otel-adapter/internal/telemetry"
	"github.com/elohmeier/uipath-otel-adapter/internal/uipath"
)

func main() {
	if err := run(); err != nil {
		slog.Error("adapter stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	once := flag.Bool("once", false, "collect once, attempt delivery, then exit")
	version := flag.Bool("version", false, "print version")
	flag.Parse()
	if *version {
		fmt.Println(telemetry.Version)
		return nil
	}
	c, e := config.Load()
	if e != nil {
		return e
	}
	source, e := uipath.New(c)
	if e != nil {
		return e
	}
	sender, e := telemetry.NewExporter(c)
	if e != nil {
		return e
	}
	store, e := state.Open(c.StatePath, adapter.Binding(c), c.MaxPending)
	if e != nil {
		return e
	}
	defer store.Close()
	a := &adapter.Adapter{Config: c, Source: source, Sender: sender, Store: store, Started: time.Now()}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if *once {
		e := a.Poll(ctx)
		return errors.Join(e, a.Flush(ctx))
	}
	var ready atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "initializing", 503)
			return
		}
		w.WriteHeader(200)
	})
	server := &http.Server{Addr: c.Listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if e := a.Flush(ctx); e != nil && ctx.Err() == nil {
					slog.Warn("OTLP delivery pending", "error", e)
				}
			}
		}
	}()
	defer func() {
		cancel()
		<-done
		stop, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = server.Shutdown(stop)
	}()
	ticker := time.NewTicker(c.PollInterval)
	defer ticker.Stop()
	for {
		if e := a.Poll(ctx); e != nil && ctx.Err() == nil {
			slog.Warn("collection incomplete", "error", e)
		}
		ready.Store(true)
		select {
		case <-ctx.Done():
			return nil
		case e := <-serverErrors:
			return e
		case <-ticker.C:
		}
	}
}
