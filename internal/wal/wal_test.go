package wal

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestReplayEmpty(t *testing.T) {
	log := openLog(t)
	recs, err := log.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("len = %d; want 0", len(recs))
	}
}

func TestAppendReplaySameProcess(t *testing.T) {
	log := openLog(t)
	appendRec(t, log, Record{Op: OpPut, Key: "foo", Value: "bar"})
	appendRec(t, log, Record{Op: OpDelete, Key: "foo"})

	got, err := log.Replay()
	if err != nil {
		t.Fatal(err)
	}
	want := []Record{
		{Op: OpPut, Key: "foo", Value: "bar"},
		{Op: OpDelete, Key: "foo"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Replay = %#v; want %#v", got, want)
	}
}

func TestReplayThenAppend(t *testing.T) {
	log := openLog(t)
	appendRec(t, log, Record{Op: OpPut, Key: "foo", Value: "bar"})

	if _, err := log.Replay(); err != nil {
		t.Fatal(err)
	}
	appendRec(t, log, Record{Op: OpPut, Key: "cat", Value: "dog"})

	got, err := log.Replay()
	if err != nil {
		t.Fatal(err)
	}
	want := []Record{
		{Op: OpPut, Key: "foo", Value: "bar"},
		{Op: OpPut, Key: "cat", Value: "dog"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Replay = %#v; want %#v", got, want)
	}
}

func TestReplayAfterReopen(t *testing.T) {
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

	log, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { log.Close() })

	got, err := log.Replay()
	if err != nil {
		t.Fatal(err)
	}
	want := []Record{{Op: OpPut, Key: "foo", Value: "bar"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Replay = %#v; want %#v", got, want)
	}
}

func openLog(t *testing.T) *Log {
	t.Helper()
	log, err := Open(filepath.Join(t.TempDir(), "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { log.Close() })
	return log
}

func appendRec(t *testing.T, log *Log, rec Record) {
	t.Helper()
	if err := log.Append(rec); err != nil {
		t.Fatal(err)
	}
}
