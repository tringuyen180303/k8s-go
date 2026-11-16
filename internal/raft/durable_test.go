package raft

import "testing"

func TestSyncWritesFollowerWAL(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(dir, "A", "B", "C")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	c.Node("C").Down()
	if err := c.Put("foo", "baz"); err != nil {
		t.Fatal(err)
	}
	c.Node("C").Up()
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	c, err = Open(dir, "A", "B", "C")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	got, ok := c.Node("C").Get("foo")
	if !ok || got != "baz" {
		t.Fatalf("C after restart Get(foo) = %q, %v; want baz, true", got, ok)
	}
}
