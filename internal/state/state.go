// Package state provides single-writer transactional checkpoints and an OTLP outbox.
package state

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

type Envelope struct {
	Signal      string
	Payload     []byte
	Attempts    int
	Next        time.Time
	Quarantined bool
}
type Pending struct {
	ID uint64
	Envelope
}
type Store struct {
	db  *bolt.DB
	max int
}

var ErrFull = errors.New("outbox full; checkpoint not advanced")

func Open(path, binding string, max int) (*Store, error) {
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return nil, e
	}
	db, e := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if e != nil {
		return nil, fmt.Errorf("open state (only one writer allowed): %w", e)
	}
	s := &Store{db: db, max: max}
	e = db.Update(func(tx *bolt.Tx) error {
		for _, n := range []string{"state", "outbox", "meta"} {
			if _, e := tx.CreateBucketIfNotExists([]byte(n)); e != nil {
				return e
			}
		}
		b := tx.Bucket([]byte("meta"))
		old := b.Get([]byte("binding"))
		if old != nil && string(old) != binding {
			return errors.New("state belongs to a different source; choose a new STATE_PATH")
		}
		return b.Put([]byte("binding"), []byte(binding))
	})
	if e != nil {
		db.Close()
		return nil, e
	}
	return s, nil
}
func (s *Store) Close() error { return s.db.Close() }
func (s *Store) Load(key string, v any) error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state")).Get([]byte(key))
		if b == nil {
			return nil
		}
		return json.Unmarshal(b, v)
	})
}
func (s *Store) Save(key string, v any, envelopes []Envelope) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		out := tx.Bucket([]byte("outbox"))
		if out.Stats().KeyN+len(envelopes) > s.max {
			return ErrFull
		}
		b, e := json.Marshal(v)
		if e != nil {
			return e
		}
		if e = tx.Bucket([]byte("state")).Put([]byte(key), b); e != nil {
			return e
		}
		for _, env := range envelopes {
			id, e := out.NextSequence()
			if e != nil {
				return e
			}
			b, e = json.Marshal(env)
			if e != nil {
				return e
			}
			if e = out.Put(idKey(id), b); e != nil {
				return e
			}
		}
		return nil
	})
}

// Metrics are latest-value cumulative snapshots, not an unbounded history spool.
func (s *Store) Metrics(payload []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("meta")).Put([]byte("metrics"), payload) })
}
func (s *Store) TakeMetrics() ([]byte, error) {
	var b []byte
	e := s.db.View(func(tx *bolt.Tx) error {
		b = append([]byte(nil), tx.Bucket([]byte("meta")).Get([]byte("metrics"))...)
		return nil
	})
	return b, e
}
func (s *Store) AckMetrics(payload []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("meta"))
		if string(b.Get([]byte("metrics"))) == string(payload) {
			return b.Delete([]byte("metrics"))
		}
		return nil
	})
}
func (s *Store) Pending(now time.Time) ([]Pending, error) {
	var out []Pending
	e := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("outbox")).ForEach(func(k, b []byte) error {
			var env Envelope
			if e := json.Unmarshal(b, &env); e != nil {
				return e
			}
			if !env.Quarantined && !now.Before(env.Next) {
				out = append(out, Pending{ID: binary.BigEndian.Uint64(k), Envelope: env})
			}
			return nil
		})
	})
	return out, e
}
func (s *Store) Ack(id uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("outbox")).Delete(idKey(id)) })
}
func (s *Store) Fail(p Pending, permanent bool, retryAfter ...time.Duration) error {
	p.Attempts++
	p.Quarantined = permanent
	p.Next = time.Now().Add(time.Duration(min(300, 1<<min(p.Attempts, 8))) * time.Second)
	if len(retryAfter) > 0 && time.Now().Add(retryAfter[0]).After(p.Next) {
		p.Next = time.Now().Add(retryAfter[0])
	}
	b, e := json.Marshal(p.Envelope)
	if e != nil {
		return e
	}
	return s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("outbox")).Put(idKey(p.ID), b) })
}
func (s *Store) Counts() (pending, quarantined int) {
	_ = s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("meta")).Get([]byte("metrics-rejected")) != nil {
			quarantined++
		}
		return tx.Bucket([]byte("outbox")).ForEach(func(_, b []byte) error {
			var env Envelope
			if e := json.Unmarshal(b, &env); e != nil {
				return e
			}
			if env.Quarantined {
				quarantined++
			} else {
				pending++
			}
			return nil
		})
	})
	return
}
func idKey(id uint64) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, id); return b }

// RejectMetrics retains the latest rejected snapshot for inspection without
// replaying a partially accepted request. Subsequent polls are new observations.
func (s *Store) RejectMetrics(payload []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("meta"))
		if err := b.Put([]byte("metrics-rejected"), payload); err != nil {
			return err
		}
		if string(b.Get([]byte("metrics"))) == string(payload) {
			return b.Delete([]byte("metrics"))
		}
		return nil
	})
}
