package raft

import (
	"context"
	"errors"
	"strings"
	"testing"

	"k8s-go/internal/api"
)

func TestRegistrySurvivesLeaderLoss(t *testing.T) {
	c, err := Open(t.TempDir(), "A", "B", "C")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	store := Registry(c)
	ctx := context.Background()

	err = store.Create(ctx, &api.Node{
		Metadata: api.ObjectMeta{Name: "node-a"},
		Status:   api.NodeStatus{Ready: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	c.Node("A").Down()
	if _, err := store.Get(ctx, "Node", "", "node-a"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Get with leader down = %v; want unavailable", err)
	}
	if err := c.Elect("B"); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, "Node", "", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	node := got.(*api.Node)
	if node.Metadata.Name != "node-a" || !node.Status.Ready {
		t.Fatalf("node = %+v", node)
	}
	raw, ok := c.Node("C").Get("/registry/nodes/node-a")
	if !ok || !strings.Contains(raw, `"name":"node-a"`) {
		t.Fatalf("C Get = %q, %v; want the replicated node", raw, ok)
	}
}
