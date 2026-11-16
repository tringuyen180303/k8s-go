package kv

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotRestoresState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")

	s := openStore(t, path)
	mustPut(t, s, "foo", "bar")
	mustPut(t, s, "cat", "dog")
	if err := s.SaveSnapshot(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("wal size = %d after snapshot; want 0", info.Size())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openStore(t, path)
	assertValue(t, s, "foo", "bar")
	assertValue(t, s, "cat", "dog")
	if got := s.Revision(); got != 2 {
		t.Fatalf("Revision = %d; want 2", got)
	}

	mustPut(t, s, "foo", "baz")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openStore(t, path)
	assertValue(t, s, "foo", "baz")
	assertValue(t, s, "cat", "dog")
	if got := s.Revision(); got != 3 {
		t.Fatalf("Revision = %d; want 3", got)
	}
}
