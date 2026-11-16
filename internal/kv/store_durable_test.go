package kv

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPutSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	s := openStore(t, path)
	if err := s.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("cat", "dog"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openStore(t, path)
	assertValue(t, s, "foo", "bar")
	assertValue(t, s, "cat", "dog")
}

func TestDeleteSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	s := openStore(t, path)
	if err := s.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	existed, err := s.Delete("foo")
	if err != nil {
		t.Fatal(err)
	}
	if !existed {
		t.Fatal("Delete = false; want true")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openStore(t, path)
	if _, ok := s.Get("foo"); ok {
		t.Fatal("foo present after reopen")
	}
}

func TestPutReplacesAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	s := openStore(t, path)
	if err := s.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openStore(t, path)
	if err := s.Put("foo", "baz"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openStore(t, path)
	assertValue(t, s, "foo", "baz")
}

func TestReopenDoesNotGrowWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	s := openStore(t, path)
	if err := s.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	before := fileSize(t, path)
	s = openStore(t, path)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	after := fileSize(t, path)
	if after != before {
		t.Fatalf("wal size = %d after reopen; want %d", after, before)
	}
}

func openStore(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func assertValue(t *testing.T, s *Store, key, want string) {
	t.Helper()
	got, ok := s.Get(key)
	if !ok || got != want {
		t.Fatalf("Get(%s) = %q, %v; want %q, true", key, got, ok, want)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}
