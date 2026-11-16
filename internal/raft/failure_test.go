package raft

import "testing"

func TestLeaderCrashThenNewLeaderWrites(t *testing.T) {
	c := New("A", "B", "C")
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	c.Node("A").Down()
	if err := c.Put("foo", "baz"); err == nil {
		t.Fatal("write committed with the leader down")
	}
	if err := c.Elect("B"); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("foo", "baz"); err != nil {
		t.Fatal(err)
	}
	got, ok := c.Node("B").Get("foo")
	if !ok || got != "baz" {
		t.Fatalf("B Get(foo) = %q, %v; want baz, true", got, ok)
	}
	got, ok = c.Node("C").Get("foo")
	if !ok || got != "baz" {
		t.Fatalf("C Get(foo) = %q, %v; want baz, true", got, ok)
	}
	got, ok = c.Node("A").Get("foo")
	if !ok || got != "bar" {
		t.Fatalf("down A Get(foo) = %q, %v; want bar, true", got, ok)
	}
}

func TestFollowerCrashThenRejoin(t *testing.T) {
	c := New("A", "B", "C")
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	c.Node("C").Down()
	if err := c.Put("foo", "baz"); err != nil {
		t.Fatal(err)
	}
	if got := c.Node("C").LastIndex(); got != 1 {
		t.Fatalf("C LastIndex = %d while down; want 1", got)
	}
	c.Node("C").Up()
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	got, ok := c.Node("C").Get("foo")
	if !ok || got != "baz" || c.Node("C").LastIndex() != 2 {
		t.Fatalf("C after rejoin Get=%q,%v LastIndex=%d", got, ok, c.Node("C").LastIndex())
	}
}

func TestPartitionedLeaderCannotServeRead(t *testing.T) {
	c := New("A", "B", "C")
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	c.Isolate("A")
	if err := c.Elect("B"); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("foo", "baz"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Read("A", "foo"); err == nil {
		t.Fatal("partitioned leader served a read")
	}
	got, ok, err := c.Read("B", "foo")
	if err != nil || !ok || got != "baz" {
		t.Fatalf("Read(B) = %q, %v, %v; want baz, true, nil", got, ok, err)
	}
}
