package kv

import "testing"

func TestCompactDropsOldRevisions(t *testing.T) {
	s := New()
	mustPut(t, s, "foo", "bar")
	mustPut(t, s, "cat", "dog")
	mustPut(t, s, "foo", "baz")

	if err := s.Compact(3); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get("foo")
	if !ok || got != "baz" {
		t.Fatalf("Get(foo) = %q, %v; want baz, true", got, ok)
	}
	got, ok = s.Get("cat")
	if !ok || got != "dog" {
		t.Fatalf("Get(cat) = %q, %v; want dog, true", got, ok)
	}
	assertAt(t, s, "foo", 1, "", false)
	assertAt(t, s, "cat", 2, "", false)
	assertAt(t, s, "foo", 3, "baz", true)
	assertAt(t, s, "cat", 3, "dog", true)
}

func TestCompactRejectsFutureRevision(t *testing.T) {
	s := New()
	mustPut(t, s, "foo", "bar")
	if err := s.Compact(2); err == nil {
		t.Fatal("Compact(2) succeeded at revision 1")
	}
	assertAt(t, s, "foo", 1, "bar", true)
}
