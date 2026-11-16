package kv

import (
	"context"
	"testing"
)

func TestListPrefix(t *testing.T) {
	s := New()
	mustPut(t, s, "/registry/pods/default/a", "one")
	mustPut(t, s, "/registry/pods/default/b", "two")
	mustPut(t, s, "/registry/pods/default/c", "three")
	mustPut(t, s, "/registry/nodes/n", "ready")
	if _, err := s.Delete("/registry/pods/default/b"); err != nil {
		t.Fatal(err)
	}

	got := s.List("/registry/pods/default/")
	want := map[string]string{
		"/registry/pods/default/a": "one",
		"/registry/pods/default/c": "three",
	}
	if len(got) != len(want) {
		t.Fatalf("List = %+v; want the two live pods", got)
	}
	for _, item := range got {
		value, ok := want[item.Key]
		if !ok || value != item.Value {
			t.Fatalf("List item %+v; want %+v", item, want)
		}
	}
}

func TestWatchPrefixFromRevision(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	mustPut(t, s, "/registry/pods/default/a", "one")
	mustPut(t, s, "/registry/pods/default/b", "two")
	mustPut(t, s, "/registry/pods/default/a", "three")
	mustPut(t, s, "/registry/nodes/n", "ready")

	ch := s.WatchPrefix(ctx, "/registry/pods/default/", 2)
	ev := waitEvent(t, ch)
	if ev.Key != "/registry/pods/default/a" || ev.Value != "three" || ev.Revision != 3 || ev.Deleted {
		t.Fatalf("replay = %+v; want a=three at revision 3", ev)
	}

	mustPut(t, s, "/registry/pods/default/a", "four")
	ev = waitEvent(t, ch)
	if ev.Key != "/registry/pods/default/a" || ev.Value != "four" || ev.Revision != 5 || ev.Deleted {
		t.Fatalf("live = %+v; want a=four at revision 5", ev)
	}
}

func TestUpdateRejectsStaleRevision(t *testing.T) {
	s := New()
	mustPut(t, s, "foo", "bar")

	rev, err := s.Update("foo", "baz", 1)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 2 {
		t.Fatalf("Update revision = %d; want 2", rev)
	}
	if got, ok := s.Get("foo"); !ok || got != "baz" {
		t.Fatalf("Get = %q, %v; want baz, true", got, ok)
	}

	if _, err := s.Update("foo", "nope", 1); err == nil {
		t.Fatal("stale Update succeeded")
	}
	if got, _ := s.Get("foo"); got != "baz" {
		t.Fatalf("value after stale Update = %q; want baz", got)
	}
}

func TestUpdateCreateRequiresAbsentKey(t *testing.T) {
	s := New()
	rev, err := s.Update("foo", "bar", 0)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("create revision = %d; want 1", rev)
	}

	if _, err := s.Update("foo", "baz", 0); err == nil {
		t.Fatal("create succeeded for an existing key")
	}
	if got, _ := s.Get("foo"); got != "bar" {
		t.Fatalf("Get = %q; want bar", got)
	}

	if _, err := s.Delete("foo"); err != nil {
		t.Fatal(err)
	}
	rev, err = s.Update("foo", "again", 0)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 3 {
		t.Fatalf("recreate revision = %d; want 3", rev)
	}
	if got, ok := s.Get("foo"); !ok || got != "again" {
		t.Fatalf("Get = %q, %v; want again, true", got, ok)
	}
}
