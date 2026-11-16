package node_test

import (
	"context"
	"testing"
	"time"

	"k8s-go/internal/api"
	"k8s-go/internal/kv"
	"k8s-go/internal/node"
	"k8s-go/internal/registry"
)

func TestRegisterAndBeat(t *testing.T) {
	store := registry.New(kv.New())
	ctx := context.Background()
	start := time.Unix(1_000, 0).UTC()

	if err := node.Register(ctx, store, "node-a", start); err != nil {
		t.Fatal(err)
	}
	got := getNode(t, store, "node-a")
	if !got.Status.Ready || !got.Status.LastHeartbeat.Equal(start) {
		t.Fatalf("node = %+v", got.Status)
	}
	lease := getLease(t, store, "node-a")
	if !lease.Spec.RenewTime.Equal(start) || lease.Spec.LeaseDurationSeconds != 30 {
		t.Fatalf("lease = %+v", lease.Spec)
	}

	renewed := start.Add(20 * time.Second)
	if err := node.Beat(ctx, store, "node-a", renewed); err != nil {
		t.Fatal(err)
	}
	got = getNode(t, store, "node-a")
	lease = getLease(t, store, "node-a")
	if !got.Status.Ready || !got.Status.LastHeartbeat.Equal(renewed) || !lease.Spec.RenewTime.Equal(renewed) {
		t.Fatalf("after beat node=%+v lease=%+v", got.Status, lease.Spec)
	}
}

func getNode(t *testing.T, store registry.Store, name string) *api.Node {
	t.Helper()
	obj, err := store.Get(context.Background(), "Node", "", name)
	if err != nil {
		t.Fatal(err)
	}
	return obj.(*api.Node)
}

func getLease(t *testing.T, store registry.Store, name string) *api.Lease {
	t.Helper()
	obj, err := store.Get(context.Background(), "Lease", "", name)
	if err != nil {
		t.Fatal(err)
	}
	return obj.(*api.Lease)
}
