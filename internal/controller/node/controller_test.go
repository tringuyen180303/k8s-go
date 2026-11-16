package node_test

import (
	"context"
	"testing"
	"time"

	"k8s-go/internal/api"
	"k8s-go/internal/controller/node"
	"k8s-go/internal/kv"
	heartbeat "k8s-go/internal/node"
	"k8s-go/internal/registry"
)

func TestFreshLeaseStaysReady(t *testing.T) {
	store := registry.New(kv.New())
	ctx := context.Background()
	start := time.Unix(1_000, 0).UTC()

	if err := heartbeat.Register(ctx, store, "node-a", start); err != nil {
		t.Fatal(err)
	}
	if err := heartbeat.Beat(ctx, store, "node-a", start.Add(20*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := node.Reconcile(ctx, store, start.Add(40*time.Second)); err != nil {
		t.Fatal(err)
	}
	if !getNode(t, store, "node-a").Status.Ready {
		t.Fatal("node-a is NotReady; the lease was renewed 20s ago")
	}
}

func TestExpiredLeaseIsNotReady(t *testing.T) {
	store := registry.New(kv.New())
	ctx := context.Background()
	start := time.Unix(1_000, 0).UTC()

	if err := heartbeat.Register(ctx, store, "node-a", start); err != nil {
		t.Fatal(err)
	}
	if err := node.Reconcile(ctx, store, start.Add(31*time.Second)); err != nil {
		t.Fatal(err)
	}
	if getNode(t, store, "node-a").Status.Ready {
		t.Fatal("node-a is Ready; the lease stopped 31s ago")
	}

	if err := heartbeat.Beat(ctx, store, "node-a", start.Add(40*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := node.Reconcile(ctx, store, start.Add(50*time.Second)); err != nil {
		t.Fatal(err)
	}
	if !getNode(t, store, "node-a").Status.Ready {
		t.Fatal("node-a stayed NotReady after a new heartbeat")
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
