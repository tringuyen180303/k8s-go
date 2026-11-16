package kv

import (
	"fmt"
	"time"
)

// lease is a time limit attached to zero or more keys.
// expiry is the instant the lease stops being valid.
type lease struct {
	id     uint64
	ttl    time.Duration
	expiry time.Time
	keys   map[string]struct{}
}

// Grant creates a lease that is valid until now plus ttl.
// The returned id starts at 1 and increases by one each call.
func (s *Store) Grant(ttl time.Duration, now time.Time) (uint64, error) {
	// TODO: lock s.mu. Increment s.nextLease. Store a lease whose expiry is
	// now.Add(ttl) and whose keys map is empty. Return the new id.
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextLease++
	ls := &lease{
		id:     s.nextLease,
		ttl:    ttl,
		expiry: now.Add(ttl),
		keys:   make(map[string]struct{}),
	}
	s.leases[s.nextLease] = ls
	return s.nextLease, nil
}

// KeepAlive moves the lease's expiry to now plus its original ttl.
// An unknown id or a lease that is already expired returns an error.
// now.Before(expiry) means the lease is still valid.
func (s *Store) KeepAlive(id uint64, now time.Time) error {
	// TODO: lock s.mu. Find s.leases[id]. On success set expiry to now.Add(ls.ttl).
	s.mu.Lock()
	defer s.mu.Unlock()
	ls, ok := s.leases[id]
	if !ok {
		return fmt.Errorf("lease is unknown")
	}
	if now.After(ls.expiry) {
		return fmt.Errorf("lease is expired")
	}
	ls.expiry = now.Add(ls.ttl)
	return nil
}

// PutWithLease stores value at key and attaches the key to the lease.
// An unknown or already expired lease returns an error and does not store the key.
// Put takes s.mu itself, so do not hold s.mu across the Put call.
func (s *Store) PutWithLease(key, value string, id uint64, now time.Time) error {
	// TODO:
	// 1. Under s.mu, reject an unknown or expired lease.
	// 2. Unlock, then s.Put(key, value).
	// 3. Lock again and add key to that lease's keys map.
	s.mu.Lock()
	ls, ok := s.leases[id]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("lease is unknown")
	}
	if !now.Before(ls.expiry) {
		s.mu.Unlock()
		return fmt.Errorf("lease is expired")
	}
	s.mu.Unlock()

	if err := s.Put(key, value); err != nil {
		return err
	}

	s.mu.Lock()
	ls.keys[key] = struct{}{}
	s.mu.Unlock()
	return nil
}

// Expire deletes every key attached to a lease whose expiry is not after now.
// Collect the keys and remove the lease while holding s.mu, then unlock
// before calling Delete. Delete locks s.mu again, and a second lock on the
// same goroutine waits forever.
func (s *Store) Expire(now time.Time) error {
	// TODO: for each lease, if !now.Before(ls.expiry), remember its keys and
	// delete the lease from the map. After unlocking, Delete each remembered key.
	s.mu.Lock()
	var keys []string
	for id, ls := range s.leases {
		if now.Before(ls.expiry) {
			continue
		}
		for key := range ls.keys {
			keys = append(keys, key)
		}
		delete(s.leases, id)
	}
	s.mu.Unlock()

	for _, key := range keys {
		if _, err := s.Delete(key); err != nil {
			return err
		}
	}
	return nil
}
