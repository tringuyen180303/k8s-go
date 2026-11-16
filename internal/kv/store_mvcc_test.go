package kv

import (
	"path/filepath"
	"testing"
)

func TestRevisionCountsWrites(t *testing.T) {
	s := New()
	if got := s.Revision(); got != 0 {
		t.Fatalf("Revision = %d; want 0", got)
	}
	mustPut(t, s, "foo", "bar")
	if got := s.Revision(); got != 1 {
		t.Fatalf("Revision = %d; want 1", got)
	}
	mustPut(t, s, "cat", "dog")
	mustPut(t, s, "foo", "baz")
	if got := s.Revision(); got != 3 {
		t.Fatalf("Revision = %d; want 3", got)
	}
	if _, err := s.Delete("foo"); err != nil {
		t.Fatal(err)
	}
	if got := s.Revision(); got != 4 {
		t.Fatalf("Revision = %d; want 4", got)
	}
}

func TestGetAtKeepsOlderValues(t *testing.T) {
	s := New()
	mustPut(t, s, "foo", "bar")
	mustPut(t, s, "cat", "dog")
	mustPut(t, s, "foo", "baz")

	got, ok := s.Get("foo")
	if !ok || got != "baz" {
		t.Fatalf("Get(foo) = %q, %v; want baz, true", got, ok)
	}
	assertAt(t, s, "foo", 1, "bar", true)
	assertAt(t, s, "foo", 2, "bar", true)
	assertAt(t, s, "foo", 3, "baz", true)
	assertAt(t, s, "cat", 1, "", false)
	assertAt(t, s, "cat", 2, "dog", true)

	if _, err := s.Delete("foo"); err != nil {
		t.Fatal(err)
	}
	assertAt(t, s, "foo", 3, "baz", true)
	assertAt(t, s, "foo", 4, "", false)
	if _, ok := s.Get("foo"); ok {
		t.Fatal("Get(foo) after delete = true; want false")
	}
}

func TestDeleteThenPutKeepsBothVersions(t *testing.T) {
	s := New()
	mustPut(t, s, "foo", "bar")
	if _, err := s.Delete("foo"); err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "foo", "baz")

	assertAt(t, s, "foo", 1, "bar", true)
	assertAt(t, s, "foo", 2, "", false)
	assertAt(t, s, "foo", 3, "baz", true)
}

func TestHistorySurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	s := openStore(t, path)
	mustPut(t, s, "foo", "bar")
	mustPut(t, s, "foo", "baz")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openStore(t, path)
	if got := s.Revision(); got != 2 {
		t.Fatalf("Revision = %d; want 2", got)
	}
	assertAt(t, s, "foo", 1, "bar", true)
	assertAt(t, s, "foo", 2, "baz", true)
	got, ok := s.Get("foo")
	if !ok || got != "baz" {
		t.Fatalf("Get(foo) = %q, %v; want baz, true", got, ok)
	}
}

func mustPut(t *testing.T, s *Store, key, value string) {
	t.Helper()
	if err := s.Put(key, value); err != nil {
		t.Fatal(err)
	}
}

func assertAt(t *testing.T, s *Store, key string, rev uint64, want string, wantOK bool) {
	t.Helper()
	got, ok := s.GetAt(key, rev)
	if ok != wantOK || got != want {
		t.Fatalf("GetAt(%s, %d) = %q, %v; want %q, %v", key, rev, got, ok, want, wantOK)
	}
}
