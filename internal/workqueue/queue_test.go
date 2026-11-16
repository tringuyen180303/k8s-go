package workqueue

import (
	"testing"
	"time"
)

func TestGetBlocksUntilAdd(t *testing.T) {
	q := New()
	got := make(chan string, 1)
	go func() {
		key, shutdown := q.Get()
		if shutdown {
			got <- "shutdown"
			return
		}
		got <- key
	}()
	select {
	case v := <-got:
		t.Fatalf("Get returned %q before Add", v)
	case <-time.After(50 * time.Millisecond):
	}
	q.Add("default/nginx")
	select {
	case v := <-got:
		if v != "default/nginx" {
			t.Fatalf("Get = %q", v)
		}
	case <-time.After(time.Second):
		t.Fatal("Get did not return after Add")
	}
}

func TestAddIsANoopUntilDone(t *testing.T) {
	q := New()
	q.Add("default/nginx")
	q.Add("default/nginx")
	key, shutdown := q.Get()
	if shutdown || key != "default/nginx" {
		t.Fatalf("Get = %q, shutdown %v", key, shutdown)
	}

	got := make(chan string, 1)
	go func() {
		key, shutdown := q.Get()
		if shutdown {
			got <- "shutdown"
			return
		}
		got <- key
	}()
	select {
	case v := <-got:
		t.Fatalf("second Get returned %q before Done", v)
	case <-time.After(50 * time.Millisecond):
	}
	q.Done("default/nginx")
	q.Add("default/nginx")
	select {
	case v := <-got:
		if v != "default/nginx" {
			t.Fatalf("Get after Done = %q", v)
		}
	case <-time.After(time.Second):
		t.Fatal("Get did not return after Done and Add")
	}
}

func TestShutdownUnblocksGet(t *testing.T) {
	q := New()
	got := make(chan bool, 1)
	go func() {
		_, shutdown := q.Get()
		got <- shutdown
	}()
	select {
	case <-got:
		t.Fatal("Get returned before ShutDown")
	case <-time.After(50 * time.Millisecond):
	}
	q.ShutDown()
	select {
	case shutdown := <-got:
		if !shutdown {
			t.Fatal("Get shutdown = false")
		}
	case <-time.After(time.Second):
		t.Fatal("Get did not return after ShutDown")
	}
}
