package kv

import (
	"testing"
	"time"
)

func TestAcquireRejectsWhileHeld(t *testing.T) {
	s := New()
	now := time.Unix(1_000, 0)
	token, err := s.Acquire("migration", "A", 30*time.Second, now)
	if err != nil {
		t.Fatal(err)
	}
	if token == 0 {
		t.Fatal("token = 0")
	}
	if _, err := s.Acquire("migration", "B", 30*time.Second, now); err == nil {
		t.Fatal("B acquired a lock A still holds")
	}
}

func TestExpireLetsNextOwnerFenceTheOldOne(t *testing.T) {
	s := New()
	now := time.Unix(1_000, 0)
	first, err := s.Acquire("migration", "A", 30*time.Second, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Expire(now.Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	second, err := s.Acquire("migration", "B", 30*time.Second, now.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if second <= first {
		t.Fatalf("B token = %d; want a token above A's %d", second, first)
	}
	if err := s.Release("migration", first); err == nil {
		t.Fatal("A released the lock with a stale token")
	}
	if err := s.Mutate("migration", first, "rows", "1"); err == nil {
		t.Fatal("A wrote with a stale token")
	}
	if _, ok := s.Get("rows"); ok {
		t.Fatal("stale write stored rows")
	}
	if err := s.Mutate("migration", second, "rows", "2"); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get("rows")
	if !ok || got != "2" {
		t.Fatalf("Get(rows) = %q, %v; want %q, true", got, ok, "2")
	}
	owner, ok := s.Get(lockKey("migration"))
	if !ok || owner != "B" {
		t.Fatalf("lock owner = %q, %v; want B, true", owner, ok)
	}
}

func TestReleaseLetsNextOwnerAcquire(t *testing.T) {
	s := New()
	now := time.Unix(1_000, 0)
	token, err := s.Acquire("migration", "A", 30*time.Second, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Release("migration", token); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get(lockKey("migration")); ok {
		t.Fatal("lock key present after Release")
	}
	if _, err := s.Acquire("migration", "B", 30*time.Second, now); err != nil {
		t.Fatal(err)
	}
}
