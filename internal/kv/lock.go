package kv

import (
	"fmt"
	"time"
)

func lockKey(name string) string {
	return "lock/" + name
}

// Acquire takes the named lock for owner until the lease expires.
// The returned token is a fencing token. It increases on every successful acquire.
// A second acquire while the lock key still exists returns an error.
func (s *Store) Acquire(name, owner string, ttl time.Duration, now time.Time) (uint64, error) {
	// TODO:
	// 1. If s.Get(lockKey(name)) finds a value, return an error. The lock is held.
	// 2. Grant a lease with ttl at now.
	// 3. Under s.mu, increment s.nextFence and remember that token.
	//    Unlock before PutWithLease. PutWithLease locks s.mu itself.
	// 4. PutWithLease(lockKey(name), owner, leaseID, now).
	// 5. Under s.mu, set s.fence[name] to the token. Return the token.
	if _, err := s.Get(lockKey(name)); err {
		return 0, fmt.Errorf("lock is held")
	}
	leaseID, err := s.Grant(ttl, now)
	if err != nil {
		return 0, err
	}
	s.nextFence++
	s.fence[name] = s.nextFence
	if err := s.PutWithLease(lockKey(name), owner, leaseID, now); err != nil {
		return 0, err
	}
	return s.nextFence, nil
}

// Release drops the lock when token is the current fencing token.
// An older token returns an error and leaves the lock in place.
func (s *Store) Release(name string, token uint64) error {
	// TODO:
	// 1. Under s.mu, read s.fence[name]. If it is missing or not equal to token,
	//    return an error.
	// 2. Unlock, then Delete(lockKey(name)).
	// 3. Lock again. If s.fence[name] is still this token, delete that map entry.
	s.mu.Lock()
	fence, ok := s.fence[name]
	s.mu.Unlock()
	if !ok || fence != token {
		return fmt.Errorf("lock is not held")
	}
	if _, err := s.Delete(lockKey(name)); err != nil {
		return err
	}
	s.mu.Lock()
	if s.fence[name] == token {
		delete(s.fence, name)
	}
	s.mu.Unlock()
	return nil
}

// Mutate writes key only for the current holder of name.
// A stale token, from a client whose lease already expired, returns an error.
func (s *Store) Mutate(name string, token uint64, key, value string) error {
	// TODO: Under s.mu, compare s.fence[name] with token. On a mismatch return
	// an error. Unlock, then s.Put(key, value).
	s.mu.Lock()
	fence, ok := s.fence[name]
	s.mu.Unlock()
	if !ok || fence != token {
		return fmt.Errorf("lock is not held")
	}
	return s.Put(key, value)
}
