package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestReplayRejectsFlippedByte(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	log, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	appendRec(t, log, Record{Op: OpPut, Key: "foo", Value: "bar"})
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	i := bytes.Index(data, []byte(`"bar"`))
	if i < 0 {
		t.Fatal(`"bar" not found in the log`)
	}
	data[i+1] = 'c'
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	log, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { log.Close() })
	if _, err := log.Replay(); err == nil {
		t.Fatal("Replay succeeded after a flipped byte")
	}
}
