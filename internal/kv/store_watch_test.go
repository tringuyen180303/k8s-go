package kv

import (
	"context"
	"testing"
	"time"
)

func TestWatchPut(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ch := s.Watch(ctx, "foo")
	mustPut(t, s, "foo", "bar")

	ev := waitEvent(t, ch)
	if ev.Key != "foo" || ev.Value != "bar" || ev.Deleted || ev.Revision != 1 {
		t.Fatalf("event = %+v", ev)
	}
}

func TestWatchIgnoresOtherKeys(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ch := s.Watch(ctx, "foo")
	mustPut(t, s, "cat", "dog")
	mustPut(t, s, "foo", "bar")

	ev := waitEvent(t, ch)
	if ev.Key != "foo" || ev.Value != "bar" || ev.Revision != 2 {
		t.Fatalf("event = %+v", ev)
	}
}

func TestWatchDelete(t *testing.T) {
	s := New()
	mustPut(t, s, "foo", "bar")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ch := s.Watch(ctx, "foo")

	if _, err := s.Delete("foo"); err != nil {
		t.Fatal(err)
	}
	ev := waitEvent(t, ch)
	if ev.Key != "foo" || !ev.Deleted || ev.Revision != 2 {
		t.Fatalf("event = %+v", ev)
	}
}

func TestWatchCancelCloses(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	ch := s.Watch(ctx, "foo")
	cancel()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("received an event after cancel; want a closed channel")
		}
	case <-time.After(time.Second):
		t.Fatal("watch channel was not closed")
	}

	mustPut(t, s, "foo", "bar")
}

func TestTwoWatchers(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	first := s.Watch(ctx, "foo")
	second := s.Watch(ctx, "foo")
	mustPut(t, s, "foo", "bar")

	ev1 := waitEvent(t, first)
	ev2 := waitEvent(t, second)
	if ev1.Value != "bar" || ev2.Value != "bar" || ev1.Revision != 1 || ev2.Revision != 1 {
		t.Fatalf("events = %+v %+v", ev1, ev2)
	}
}

func waitEvent(t *testing.T, ch <-chan Event) Event {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed")
		}
		return ev
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for a watch event")
	}
	return Event{}
}
