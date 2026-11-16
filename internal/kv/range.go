package kv

import (
	"context"
	"fmt"
	"strings"

	"k8s-go/internal/wal"
)

// KV is one key and its current value. List returns these.
type KV struct {
	Key   string
	Value string
}

// List returns every current key that starts with prefix.
// A deleted key is absent. Keys outside the prefix are absent.
func (s *Store) List(prefix string) []KV {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var kvs []KV
	for key, value := range s.data {
		if strings.HasPrefix(key, prefix) {
			kvs = append(kvs, KV{Key: key, Value: value})
		}
	}
	return kvs
}

// WatchPrefix replays versions with revision > fromRev under prefix, then
// delivers later live writes the same way Watch does.
func (s *Store) WatchPrefix(ctx context.Context, prefix string, fromRev uint64) <-chan Event {
	ch := make(chan Event, 16)
	s.mu.Lock()
	for key, versions := range s.history {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, v := range versions {
			if v.revision <= fromRev {
				continue
			}
			ch <- Event{Revision: v.revision, Key: key, Value: v.value, Deleted: v.deleted}
		}
	}
	id := s.nextWatch
	s.nextWatch++
	s.watchers[id] = watcher{prefix: prefix, ch: ch}
	s.mu.Unlock()
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		delete(s.watchers, id)
		s.mu.Unlock()
		close(ch)
	}()
	return ch
}

// Update writes value only when the latest revision of key is expectRev.
// expectRev 0 creates the key and fails if it still exists.
// A stale revision returns an error and leaves the stored value unchanged.
// The returned revision is the new store revision.
func (s *Store) Update(key, value string, expectRev uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	versions := s.history[key]
	absent := len(versions) == 0 || versions[len(versions)-1].deleted
	if expectRev == 0 {
		if !absent {
			return 0, fmt.Errorf("kv: key exists")
		}
	} else if absent || versions[len(versions)-1].revision != expectRev {
		return 0, fmt.Errorf("kv: revision conflict")
	}

	if s.log != nil {
		err := s.log.Append(wal.Record{Op: wal.OpPut, Key: key, Value: value})
		if err != nil {
			return 0, err
		}
	}
	s.rev++
	s.history[key] = append(s.history[key], version{
		revision: s.rev,
		value:    value,
	})
	s.data[key] = value
	s.notify(Event{Revision: s.rev, Key: key, Value: value})
	if s.onWrite != nil {
		s.onWrite(key, value, false)
	}
	return s.rev, nil
}

// OnWrite registers fn, called after a successful Update or Delete.
// fn runs while s is locked. It may write to other stores. It must not call back into s.
func (s *Store) OnWrite(fn func(key, value string, deleted bool)) {
	s.mu.Lock()
	s.onWrite = fn
	s.mu.Unlock()
}
