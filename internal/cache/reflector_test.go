package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	"k8s-go/internal/api"
	"k8s-go/internal/registry"
)

func TestMetaNamespaceKey(t *testing.T) {
	pod := &api.Pod{Metadata: api.ObjectMeta{Namespace: "default", Name: "nginx"}}
	if got := MetaNamespaceKey(pod); got != "default/nginx" {
		t.Fatalf("pod key = %q", got)
	}
	node := &api.Node{Metadata: api.ObjectMeta{Name: "node-a"}}
	if got := MetaNamespaceKey(node); got != "node-a" {
		t.Fatalf("node key = %q", got)
	}
}

func TestReflectorDeliversListThenWatchAndRelists(t *testing.T) {
	lw := &fakeLW{}
	h := &recordHandler{}
	inf := New(lw, h)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		inf.Run(ctx)
		close(done)
	}()

	deadline := time.After(time.Second)
	var events []string
	var lists int
	for {
		h.mu.Lock()
		events = append([]string(nil), h.events...)
		h.mu.Unlock()
		lw.mu.Lock()
		lists = lw.lists
		lw.mu.Unlock()
		if len(events) >= 2 && lists >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("events = %v, lists = %d", events, lists)
		case <-time.After(10 * time.Millisecond):
		}
	}
	if events[0] != "add nginx" || events[1] != "update nginx" {
		t.Fatalf("events = %v", events)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

type recordHandler struct {
	mu     sync.Mutex
	events []string
}

func (h *recordHandler) OnAdd(obj api.Object) {
	h.mu.Lock()
	h.events = append(h.events, "add "+obj.GetObjectMeta().Name)
	h.mu.Unlock()
}

func (h *recordHandler) OnUpdate(obj api.Object) {
	h.mu.Lock()
	h.events = append(h.events, "update "+obj.GetObjectMeta().Name)
	h.mu.Unlock()
}

func (h *recordHandler) OnDelete(obj api.Object) {
	h.mu.Lock()
	h.events = append(h.events, "delete "+obj.GetObjectMeta().Name)
	h.mu.Unlock()
}

type fakeLW struct {
	mu    sync.Mutex
	lists int
}

func (f *fakeLW) List(ctx context.Context) ([]api.Object, uint64, error) {
	f.mu.Lock()
	f.lists++
	n := f.lists
	f.mu.Unlock()
	pod := &api.Pod{Metadata: api.ObjectMeta{Name: "nginx", Namespace: "default"}}
	if n > 1 {
		pod.Spec.NodeName = "node-a"
	}
	rev := uint64(n)
	return []api.Object{pod}, rev, nil
}

func (f *fakeLW) Watch(ctx context.Context, rev uint64) (<-chan registry.Event, error) {
	ch := make(chan registry.Event, 1)
	go func() {
		defer close(ch)
		if rev != 1 {
			<-ctx.Done()
			return
		}
		ch <- registry.Event{
			Type: registry.Modified,
			Object: &api.Pod{
				Metadata: api.ObjectMeta{Name: "nginx", Namespace: "default", ResourceVersion: 2},
				Spec:     api.PodSpec{NodeName: "node-a"},
			},
		}
	}()
	return ch, nil
}
