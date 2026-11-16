package endpoints

import (
	"context"
	"testing"

	"k8s-go/internal/api"
	"k8s-go/internal/kv"
	"k8s-go/internal/registry"
)

func TestEndpointsFollowRunningPods(t *testing.T) {
	store := registry.New(kv.New())
	ctx := context.Background()
	if err := store.Create(ctx, &api.Service{
		Metadata: api.ObjectMeta{Name: "web", Namespace: "default"},
		Spec: api.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Port:     80,
		},
	}); err != nil {
		t.Fatal(err)
	}
	mustPod(t, store, "web-1", "Running", "10.0.0.1", map[string]string{"app": "web"})
	mustPod(t, store, "web-2", "Running", "10.0.0.2", map[string]string{"app": "web"})
	mustPod(t, store, "web-3", "Pending", "10.0.0.3", map[string]string{"app": "web"})
	mustPod(t, store, "db", "Running", "10.0.0.4", map[string]string{"app": "db"})

	if err := Reconcile(ctx, store, "default", "web"); err != nil {
		t.Fatal(err)
	}
	if got := addresses(t, store); !sameAddresses(got, "10.0.0.1:80", "10.0.0.2:80") {
		t.Fatalf("addresses = %v; want 10.0.0.1:80 and 10.0.0.2:80", got)
	}

	if err := Reconcile(ctx, store, "default", "web"); err != nil {
		t.Fatal(err)
	}
	if got := addresses(t, store); !sameAddresses(got, "10.0.0.1:80", "10.0.0.2:80") {
		t.Fatalf("second reconcile addresses = %v", got)
	}

	pod := getPod(t, store, "web-2")
	pod.Status.Phase = "Pending"
	if err := store.UpdateStatus(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if err := Reconcile(ctx, store, "default", "web"); err != nil {
		t.Fatal(err)
	}
	if got := addresses(t, store); !sameAddresses(got, "10.0.0.1:80") {
		t.Fatalf("addresses after Pending = %v; want 10.0.0.1:80", got)
	}
}

func mustPod(t *testing.T, store registry.Store, name, phase, ip string, labels map[string]string) {
	t.Helper()
	err := store.Create(context.Background(), &api.Pod{
		Metadata: api.ObjectMeta{Name: name, Namespace: "default", Labels: labels},
		Status:   api.PodStatus{Phase: phase, PodIP: ip},
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

func addresses(t *testing.T, store registry.Store) []string {
	t.Helper()
	obj, err := store.Get(context.Background(), "Endpoints", "default", "web")
	if err != nil {
		t.Fatal(err)
	}
	ep := obj.(*api.Endpoints)
	out := make([]string, len(ep.Addresses))
	for i, addr := range ep.Addresses {
		out[i] = addr.IP + ":" + itoa(addr.Port)
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

func sameAddresses(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := map[string]int{}
	for _, addr := range got {
		seen[addr]++
	}
	for _, addr := range want {
		seen[addr]--
		if seen[addr] < 0 {
			return false
		}
	}
	return true
}
