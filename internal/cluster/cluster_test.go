package cluster

import "testing"

func TestAllNodesSeePut(t *testing.T) {
	c := New("A", "B", "C")
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"A", "B", "C"} {
		got, ok := c.Node(id).Get("foo")
		if !ok || got != "bar" {
			t.Fatalf("node %s Get(foo) = %q, %v; want %q, true", id, got, ok, "bar")
		}
	}
}

func TestOfflineNodeKeepsOldValue(t *testing.T) {
	c := New("A", "B", "C")
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}

	c.Node("C").Down()
	if err := c.Put("foo", "baz"); err != nil {
		t.Fatal(err)
	}

	for _, id := range []string{"A", "B"} {
		got, ok := c.Node(id).Get("foo")
		if !ok || got != "baz" {
			t.Fatalf("node %s Get(foo) = %q, %v; want %q, true", id, got, ok, "baz")
		}
	}
	got, ok := c.Node("C").Get("foo")
	if !ok || got != "bar" {
		t.Fatalf("node C Get(foo) = %q, %v; want %q, true", got, ok, "bar")
	}

	c.Node("C").Up()
	got, ok = c.Node("C").Get("foo")
	if !ok || got != "bar" {
		t.Fatalf("node C after Up = %q, %v; want %q, true", got, ok, "bar")
	}
}
