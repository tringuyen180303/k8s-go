package kubelet

import (
	"context"
	"testing"

	"k8s-go/internal/api"
	"k8s-go/internal/kv"
	"k8s-go/internal/registry"
)

func TestReconcileRunsOnlyThisNode(t *testing.T) {
	store := registry.New(kv.New())
	ctx := context.Background()
	mustPod(t, store, "web", "node-a", "Pending")
	mustPod(t, store, "db", "node-b", "Pending")

	if err := Reconcile(ctx, store, "node-a", StatusRuntime{}); err != nil {
		t.Fatal(err)
	}
	web := getPod(t, store, "web")
	if web.Status.Phase != "Running" || web.Spec.NodeName != "node-a" {
		t.Fatalf("web = phase %q node %q", web.Status.Phase, web.Spec.NodeName)
	}
	db := getPod(t, store, "db")
	if db.Status.Phase != "Pending" || db.Spec.NodeName != "node-b" {
		t.Fatalf("db = phase %q node %q", db.Status.Phase, db.Spec.NodeName)
	}
}

func TestSyncIgnoresADeletedPod(t *testing.T) {
	store := registry.New(kv.New())
	if err := Sync(context.Background(), store, "node-a", nil, StatusRuntime{}); err != nil {
		t.Fatal(err)
	}
}

func TestAssignsPodIPs(t *testing.T) {
	store := registry.New(kv.New())
	ctx := context.Background()
	mustPod(t, store, "web", "node-a", "Pending")
	mustPod(t, store, "db", "node-a", "Pending")

	if err := Reconcile(ctx, store, "node-a", StatusRuntime{}); err != nil {
		t.Fatal(err)
	}
	first := map[string]string{
		"web": getPod(t, store, "web").Status.PodIP,
		"db":  getPod(t, store, "db").Status.PodIP,
	}
	if !isPodIP(first["web"]) || !isPodIP(first["db"]) || first["web"] == first["db"] {
		t.Fatalf("ips = %q; want two different 10.0.0.n addresses", first)
	}

	if err := Reconcile(ctx, store, "node-a", StatusRuntime{}); err != nil {
		t.Fatal(err)
	}
	if got := getPod(t, store, "web").Status.PodIP; got != first["web"] {
		t.Fatalf("web ip = %q; want %q", got, first["web"])
	}
	if got := getPod(t, store, "db").Status.PodIP; got != first["db"] {
		t.Fatalf("db ip = %q; want %q", got, first["db"])
	}
}

func isPodIP(ip string) bool {
	if len(ip) < len("10.0.0.1") || ip[:len("10.0.0.")] != "10.0.0." {
		return false
	}
	n := 0
	for _, ch := range ip[len("10.0.0."):] {
		if ch < '0' || ch > '9' {
			return false
		}
		n = n*10 + int(ch-'0')
	}
	return n >= 1
}

func mustPod(t *testing.T, store registry.Store, name, nodeName, phase string) {
	t.Helper()
	err := store.Create(context.Background(), &api.Pod{
		Metadata: api.ObjectMeta{Name: name, Namespace: "default"},
		Spec:     api.PodSpec{NodeName: nodeName},
		Status:   api.PodStatus{Phase: phase},
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
