package state

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestAtomicCheckpointAndOutboxAcrossRestart(t *testing.T) {
	p := filepath.Join(t.TempDir(), "state.db")
	s, e := Open(p, "source-a", 1)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Save("cursor", 10, []Envelope{{Signal: "logs", Payload: []byte("first")}}); e != nil {
		t.Fatal(e)
	}
	if e = s.Save("cursor", 20, []Envelope{{Signal: "logs", Payload: []byte("second")}}); !errors.Is(e, ErrFull) {
		t.Fatalf("want full, got %v", e)
	}
	s.Close()
	s, e = Open(p, "source-a", 1)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	var cursor int
	if e = s.Load("cursor", &cursor); e != nil || cursor != 10 {
		t.Fatalf("checkpoint advanced: %d %v", cursor, e)
	}
	rows, e := s.Pending(time.Now())
	if e != nil || len(rows) != 1 || string(rows[0].Payload) != "first" {
		t.Fatalf("outbox changed %v %v", rows, e)
	}
	if e = s.Fail(rows[0], true); e != nil {
		t.Fatal(e)
	}
	n, q := s.Counts()
	if n != 0 || q != 1 {
		t.Fatal("partial rejection was discarded")
	}
}
func TestRejectDifferentSource(t *testing.T) {
	p := filepath.Join(t.TempDir(), "state.db")
	s, e := Open(p, "source-a", 2)
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	if s, e = Open(p, "source-b", 2); e == nil {
		s.Close()
		t.Fatal("source binding not enforced")
	}
}
func TestMetricsAckCannotDeleteNewSnapshot(t *testing.T) {
	s, e := Open(filepath.Join(t.TempDir(), "state.db"), "a", 2)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	s.Metrics([]byte("old"))
	s.Metrics([]byte("new"))
	s.AckMetrics([]byte("old"))
	b, _ := s.TakeMetrics()
	if string(b) != "new" {
		t.Fatal("concurrent snapshot erased")
	}
}
