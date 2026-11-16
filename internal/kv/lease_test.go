package kv

import (
	"testing"
	"time"
)

func TestLeaseExpiresKey(t *testing.T) {
	s := New()
	now := time.Unix(1_000, 0)
	id := mustGrant(t, s, 30*time.Second, now)
	if err := s.PutWithLease("foo", "bar", id, now); err != nil {
		t.Fatal(err)
	}

	if err := s.Expire(now.Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get("foo")
	if !ok || got != "bar" {
		t.Fatalf("Get(foo) = %q, %v; want %q, true", got, ok, "bar")
	}

	if err := s.Expire(now.Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("foo"); ok {
		t.Fatal("foo present at expiry")
	}
}

func TestKeepAliveExtendsLease(t *testing.T) {
	s := New()
	now := time.Unix(1_000, 0)
	id := mustGrant(t, s, 30*time.Second, now)
	if err := s.PutWithLease("foo", "bar", id, now); err != nil {
		t.Fatal(err)
	}
	renewed := now.Add(20 * time.Second)
	if err := s.KeepAlive(id, renewed); err != nil {
		t.Fatal(err)
	}

	if err := s.Expire(now.Add(40 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("foo"); !ok {
		t.Fatal("foo expired during the renewed interval")
	}
	if err := s.Expire(renewed.Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("foo"); ok {
		t.Fatal("foo present after the renewed expiry")
	}
}

func TestExpireRemovesEveryAttachedKey(t *testing.T) {
	s := New()
	now := time.Unix(1_000, 0)
	id := mustGrant(t, s, 10*time.Second, now)
	if err := s.PutWithLease("foo", "bar", id, now); err != nil {
		t.Fatal(err)
	}
	if err := s.PutWithLease("cat", "dog", id, now); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("bare", "key"); err != nil {
		t.Fatal(err)
	}

	if err := s.Expire(now.Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("foo"); ok {
		t.Fatal("foo still present")
	}
	if _, ok := s.Get("cat"); ok {
		t.Fatal("cat still present")
	}
	got, ok := s.Get("bare")
	if !ok || got != "key" {
		t.Fatalf("Get(bare) = %q, %v; want %q, true", got, ok, "key")
	}
}

func TestPutWithUnknownLease(t *testing.T) {
	s := New()
	err := s.PutWithLease("foo", "bar", 99, time.Unix(1_000, 0))
	if err == nil {
		t.Fatal("PutWithLease err = nil; want an error")
	}
	if _, ok := s.Get("foo"); ok {
		t.Fatal("foo stored without a lease")
	}
}

func mustGrant(t *testing.T, s *Store, ttl time.Duration, now time.Time) uint64 {
	t.Helper()
	id, err := s.Grant(ttl, now)
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("Grant id = 0")
	}
	return id
}
