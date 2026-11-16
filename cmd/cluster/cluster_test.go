package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"k8s-go/internal/api"
	"k8s-go/internal/client"
	"k8s-go/internal/registry"
)

func TestReplicaSetSpreadsThenScalesToZero(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	url, err := Run(ctx, t.TempDir(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c := client.New(url)

	wait(ctx, t, func() bool { return nodesReady(ctx, c) })

	rs := &api.ReplicaSet{
		Metadata: api.ObjectMeta{Name: "web", Namespace: "default"},
		Spec: api.ReplicaSetSpec{
			Replicas: 2,
			Selector: map[string]string{"app": "web"},
		},
	}
	if err := c.Create(ctx, rs); err != nil {
		t.Fatal(err)
	}

	var placed []*api.Pod
	wait(ctx, t, func() bool {
		placed = runningPods(ctx, c)
		return len(placed) == 2 && placed[0].Spec.NodeName != placed[1].Spec.NodeName
	})

	for {
		obj, err := c.Get(ctx, "ReplicaSet", "default", "web")
		if err != nil {
			t.Fatal(err)
		}
		got := obj.(*api.ReplicaSet)
		got.Spec.Replicas = 0
		err = c.Update(ctx, got)
		if !errors.Is(err, registry.ErrConflict) {
			if err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	wait(ctx, t, func() bool {
		items, _, err := c.List(ctx, "Pod", "default")
		return err == nil && len(items) == 0
	})
}

func TestReplicaSetSurvivesLeaderLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	url, cluster, err := RunRaft(ctx, t.TempDir(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cluster.Close() })
	c := client.New(url)

	wait(ctx, t, func() bool { return nodesReady(ctx, c) })

	rs := &api.ReplicaSet{
		Metadata: api.ObjectMeta{Name: "web", Namespace: "default"},
		Spec: api.ReplicaSetSpec{
			Replicas: 2,
			Selector: map[string]string{"app": "web"},
		},
	}
	if err := c.Create(ctx, rs); err != nil {
		t.Fatal(err)
	}
	wait(ctx, t, func() bool {
		placed := runningPods(ctx, c)
		return len(placed) == 2 && placed[0].Spec.NodeName != placed[1].Spec.NodeName
	})

	cluster.Node("A").Down()
	if _, err := c.Get(ctx, "ReplicaSet", "default", "web"); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("Get with leader down = %v; want 503", err)
	}
	if err := cluster.Elect("B"); err != nil {
		t.Fatal(err)
	}

	for {
		obj, err := c.Get(ctx, "ReplicaSet", "default", "web")
		if err != nil {
			t.Fatal(err)
		}
		got := obj.(*api.ReplicaSet)
		got.Spec.Replicas = 0
		err = c.Update(ctx, got)
		if !errors.Is(err, registry.ErrConflict) {
			if err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	wait(ctx, t, func() bool {
		items, _, err := c.List(ctx, "Pod", "default")
		return err == nil && len(items) == 0
	})
}

func nodesReady(ctx context.Context, c *client.Client) bool {
	items, _, err := c.List(ctx, "Node", "")
	if err != nil {
		return false
	}
	ready := map[string]bool{}
	for _, item := range items {
		node := item.(*api.Node)
		ready[node.Metadata.Name] = node.Status.Ready
	}
	return ready["node-a"] && ready["node-b"]
}

func runningPods(ctx context.Context, c *client.Client) []*api.Pod {
	items, _, err := c.List(ctx, "Pod", "default")
	if err != nil {
		return nil
	}
	var pods []*api.Pod
	for _, item := range items {
		pod := item.(*api.Pod)
		if pod.Status.Phase == "Running" && pod.Spec.NodeName != "" {
			pods = append(pods, pod)
		}
	}
	return pods
}

func wait(ctx context.Context, t *testing.T, ready func() bool) {
	t.Helper()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if ready() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
}
