package kv

import (
	"context"
	"strings"
	"sync"

	"k8s-go/internal/wal"
)

// Event is one change delivered to a watcher.
type Event struct {
	Revision uint64
	Key      string
	Value    string
	Deleted  bool
}

// watcher receives events for one key, or for every key under prefix.
// An empty prefix means exact key match. WatchPrefix sets prefix and leaves key empty.
type watcher struct {
	key    string
	prefix string
	ch     chan Event
}

// version is one value a key had, starting at a store-wide revision.
// deleted is a tombstone: the key was removed at that revision, and the
// older values stay in the slice.
type version struct {
	revision uint64
	value    string
	deleted  bool
}

// Store is a key-value map. New keeps it in memory. Open also records
// every Put and Delete in a write-ahead log so a restart can rebuild the map.
// rev is the number of successful writes. history keeps every version.
type Store struct {
	mu         sync.RWMutex
	data       map[string]string
	history    map[string][]version
	watchers   map[int]watcher
	nextWatch  int
	rev        uint64
	compactRev uint64
	log        *wal.Log
	path       string
	onWrite    func(key, value string, deleted bool)
	leases     map[uint64]*lease
	nextLease  uint64
	fence      map[string]uint64
	nextFence  uint64
}

// New returns an empty in-memory store. It has no log, so a crash drops its data.
func New() *Store {
	mu := sync.RWMutex{}
	data := make(map[string]string)
	return &Store{
		mu:       mu,
		data:     data,
		history:  make(map[string][]version),
		watchers: make(map[int]watcher),
		leases:   make(map[uint64]*lease),
		fence:    make(map[string]uint64),
	}
}

// Open opens the log at path, replays it into memory, and returns a durable store.
func Open(path string) (*Store, error) {
	log, err := wal.Open(path)
	if err != nil {
		return nil, err
	}
	records, err := log.Replay()
	if err != nil {
		log.Close()
		return nil, err
	}
	s := &Store{
		data:     make(map[string]string),
		history:  make(map[string][]version),
		watchers: make(map[int]watcher),
		leases:   make(map[uint64]*lease),
		fence:    make(map[string]uint64),
		log:      log,
		path:     path,
	}
	if err := s.applySnapshot(); err != nil {
		log.Close()
		return nil, err
	}
	for _, record := range records {
		// TODO: count this record as the next revision and append it to
		// history, then update data. The first record is revision 1.
		// Do this directly. Put and Delete would append the log again.
		s.rev++
		s.history[record.Key] = append(s.history[record.Key], version{
			revision: s.rev,
			value:    record.Value,
			deleted:  record.Op == wal.OpDelete,
		})
		if record.Op == wal.OpPut {
			s.data[record.Key] = record.Value
		} else if record.Op == wal.OpDelete {
			delete(s.data, record.Key)
		}
	}
	return s, nil
}

// Close closes the log. An in-memory store from New has no log.
func (s *Store) Close() error {
	if s.log != nil {
		return s.log.Close()
	}
	return nil
}

// Put stores value at key, replacing any previous value.
// A durable store appends the record and syncs it before the map changes.
func (s *Store) Put(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log != nil {
		record := wal.Record{Op: wal.OpPut, Key: key, Value: value}
		err := s.log.Append(record)
		if err != nil {
			return err
		}
	}
	// TODO: increment s.rev, append a non-deleted version to s.history[key],
	// then update s.data. Do the increment only after Append succeeds.
	s.rev++
	s.history[key] = append(s.history[key], version{
		revision: s.rev,
		value:    value,
		deleted:  false,
	})
	s.data[key] = value
	s.notify(Event{Revision: s.rev, Key: key, Value: value})
	return nil
}

// Get returns the value and true, or "", false when key is absent.
func (s *Store) Get(key string) (value string, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok = s.data[key]
	return value, ok
}

// Delete removes key. It reports whether the key was present.
func (s *Store) Delete(key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[key]
	if s.log != nil {
		err := s.log.Append(wal.Record{Op: wal.OpDelete, Key: key})
		if err != nil {
			return false, err
		}
	}
	// TODO: increment s.rev and append a deleted version to s.history[key],
	// then delete the key from s.data. A delete of a missing key still counts.
	s.rev++
	s.history[key] = append(s.history[key], version{
		revision: s.rev,
		value:    "",
		deleted:  true,
	})
	delete(s.data, key)
	s.notify(Event{Revision: s.rev, Key: key, Deleted: true})
	if s.onWrite != nil {
		s.onWrite(key, "", true)
	}
	return ok, nil
}

// Revision returns how many Put and Delete operations have succeeded.
func (s *Store) Revision() uint64 {
	// TODO: RLock and return s.rev.
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.rev
}

// GetAt returns the value of key as of rev.
// A missing key, a later delete, and a revision from before the key existed
// all return "", false. Get still returns the current value.
func (s *Store) GetAt(key string, rev uint64) (string, bool) {
	// TODO: RLock. Walk s.history[key] and keep the last version whose
	// revision is <= rev. History is stored in increasing revision order.
	// No such version, or that version is deleted: return "", false.
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.compactRev > 0 && rev < s.compactRev {
		return "", false
	}
	for i := len(s.history[key]) - 1; i >= 0; i-- {
		if s.history[key][i].revision <= rev {
			return s.history[key][i].value, !s.history[key][i].deleted
		}
	}
	return "", false

}

// Watch registers for later changes to key. Cancelling ctx closes the channel.
// The channel is buffered so Put can deliver an event without waiting for the
// watcher to read it.
func (s *Store) Watch(ctx context.Context, key string) <-chan Event {
	// TODO:
	// 1. ch := make(chan Event, 16)
	// 2. Under s.mu, pick s.nextWatch as the id, increment it, and store
	//    watcher{key: key, ch: ch} in s.watchers. Unlock before returning.
	// 3. Start a goroutine. It waits on <-ctx.Done(), then under s.mu deletes
	//    that id, unlocks, and closes ch. Delete the watcher before close so
	//    notify cannot send on a closed channel.
	// 4. Return ch.
	ch := make(chan Event, 16)
	s.mu.Lock()
	id := s.nextWatch
	s.nextWatch++
	s.watchers[id] = watcher{key: key, ch: ch}
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

// notify delivers ev to every watcher of ev.Key.
// The caller holds s.mu.
func (s *Store) notify(ev Event) {
	// TODO: for each watcher, if w.key == ev.Key, send ev on w.ch.
	for _, w := range s.watchers {
		if w.key == ev.Key || (w.prefix != "" && strings.HasPrefix(ev.Key, w.prefix)) {
			w.ch <- ev
		}
	}
}
