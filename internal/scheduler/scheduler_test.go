package scheduler

import (
	"context"
	"testing"

	"k8s-go/internal/api"
	"k8s-go/internal/kv"
	"k8s-go/internal/registry"
)

func TestSpreadAcrossReadyNodes(t *testing.T) {
	store := registry.New(kv.New())
	ctx := context.Background()
	for _, name := range []string{"node-a", "node-b", "node-c"} {
		mustNode(t, store, name, true)
	}
	mustNode(t, store, "node-d", false)
	token, err := Campaign(ctx, store, "scheduler-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"one", "two", "three"} {
		mustPod(t, store, name)
		if err := Bind(ctx, store, "default", name, token); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	for _, name := range []string{"one", "two", "three"} {
		pod := getPod(t, store, name)
		if pod.Spec.NodeName == "" || pod.Spec.NodeName == "node-d" || seen[pod.Spec.NodeName] {
			t.Fatalf("%s nodeName = %q, assigned %v", name, pod.Spec.NodeName, seen)
		}
		seen[pod.Spec.NodeName] = true
		if pod.Status.Phase != "Pending" {
			t.Fatalf("%s phase = %q", name, pod.Status.Phase)
		}
	}
}

func TestSecondSchedulerCannotBind(t *testing.T) {
	store := registry.New(kv.New())
	ctx := context.Background()
	mustNode(t, store, "node-a", true)
	mustPod(t, store, "one")
	mustPod(t, store, "two")

	token, err := Campaign(ctx, store, "scheduler-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Campaign(ctx, store, "scheduler-b"); err == nil {
		t.Fatal("second scheduler took the lock")
	}
	if err := Bind(ctx, store, "default", "one", token); err != nil {
		t.Fatal(err)
	}
	if err := Bind(ctx, store, "default", "two", token+1); err == nil {
		t.Fatal("stale token bound a pod")
	}
	if getPod(t, store, "two").Spec.NodeName != "" {
		t.Fatal("pod two was bound without the lock")
	}
	if getPod(t, store, "one").Spec.NodeName == "" {
		t.Fatal("lock holder did not bind pod one")
	}
}

func mustNode(t *testing.T, store registry.Store, name string, ready bool) {
	t.Helper()
	err := store.Create(context.Background(), &api.Node{
		Metadata: api.ObjectMeta{Name: name},
		Status:   api.NodeStatus{Ready: ready},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func mustPod(t *testing.T, store registry.Store, name string) {
	t.Helper()
	err := store.Create(context.Background(), &api.Pod{
		Metadata: api.ObjectMeta{Name: name, Namespace: "default"},
		Status:   api.PodStatus{Phase: "Pending"},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func getPod(t *testing.T, store registry.Store, name string) *api.Pod {
	t.Helper()
	obj, err := store.Get(context.Background(), "Pod", "default", name)
	if err != nil {
		t.Fatal(err)
	}
	return obj.(*api.Pod)
}
