package client

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"k8s-go/internal/api"
	"k8s-go/internal/apiserver"
	"k8s-go/internal/kv"
	"k8s-go/internal/registry"
)

func TestPodLifecycle(t *testing.T) {
	srv := httptest.NewServer(apiserver.New(registry.New(kv.New())))
	t.Cleanup(srv.Close)
	c := New(srv.URL)
	ctx := context.Background()

	pod := &api.Pod{
		Metadata: api.ObjectMeta{Name: "nginx", Namespace: "default", Labels: map[string]string{"app": "web"}},
		Spec:     api.PodSpec{Containers: []api.Container{{Name: "nginx", Image: "nginx:1.25"}}},
	}
	if err := c.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if pod.Metadata.UID == "" || pod.Metadata.ResourceVersion == 0 || pod.Metadata.Namespace != "default" {
		t.Fatalf("create = %+v", pod.Metadata)
	}
	first := pod.Metadata.ResourceVersion

	items, rev, err := c.List(ctx, "Pod", "default")
	if err != nil {
		t.Fatal(err)
	}
	if rev < first || len(items) != 1 || items[0].(*api.Pod).Spec.Containers[0].Image != "nginx:1.25" {
		t.Fatalf("list rev %d items %+v", rev, items)
	}

	pod.Spec.NodeName = "node-a"
	if err := c.Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if pod.Metadata.ResourceVersion == first || pod.Spec.NodeName != "node-a" {
		t.Fatalf("update = %+v", pod)
	}

	watchCtx, watchCancel := context.WithCancel(ctx)
	t.Cleanup(watchCancel)
	ch, err := c.Watch(watchCtx, "Pod", "default", first)
	if err != nil {
		t.Fatal(err)
	}
	ev := waitEvent(t, ch)
	watchCancel()
	got, ok := ev.Object.(*api.Pod)
	if ev.Type != registry.Modified || !ok || got.Spec.NodeName != "node-a" {
		t.Fatalf("watch = %+v", ev)
	}

	status := &api.Pod{
		Metadata: api.ObjectMeta{Name: "nginx", Namespace: "default", ResourceVersion: pod.Metadata.ResourceVersion},
		Status:   api.PodStatus{Phase: "Running"},
	}
	if err := c.UpdateStatus(ctx, status); err != nil {
		t.Fatal(err)
	}
	stored, err := c.Get(ctx, "Pod", "default", "nginx")
	if err != nil {
		t.Fatal(err)
	}
	if stored.(*api.Pod).Spec.NodeName != "node-a" || stored.(*api.Pod).Status.Phase != "Running" {
		t.Fatalf("get = %+v", stored)
	}

	stale := &api.Pod{
		Metadata: api.ObjectMeta{Name: "nginx", Namespace: "default", ResourceVersion: first},
		Spec:     api.PodSpec{NodeName: "node-b"},
	}
	if err := c.Update(ctx, stale); !errors.Is(err, registry.ErrConflict) {
		t.Fatalf("stale update err = %v", err)
	}
	if err := c.Delete(ctx, "Pod", "default", "nginx"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, "Pod", "default", "nginx"); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("get after delete err = %v", err)
	}
}

func TestNodeCreate(t *testing.T) {
	srv := httptest.NewServer(apiserver.New(registry.New(kv.New())))
	t.Cleanup(srv.Close)
	c := New(srv.URL)
	node := &api.Node{Metadata: api.ObjectMeta{Name: "node-a"}}
	if err := c.Create(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	if node.Metadata.Name != "node-a" || node.Metadata.Namespace != "" || node.Metadata.UID == "" {
		t.Fatalf("node = %+v", node.Metadata)
	}
}

func waitEvent(t *testing.T, ch <-chan registry.Event) registry.Event {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("watch closed")
		}
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a watch event")
	}
	return registry.Event{}
}
