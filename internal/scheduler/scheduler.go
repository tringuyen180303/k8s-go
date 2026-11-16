package scheduler

import (
	"context"
	"errors"
	"fmt"

	"k8s-go/internal/api"
	"k8s-go/internal/registry"
)

// Campaign takes the scheduler lock for holder.
// The lock is a LeaderLease named "scheduler".
// The returned token is the lease's fencing token.
// A second holder gets an error while the lock exists.
func Campaign(ctx context.Context, store registry.Store, holder string) (uint64, error) {
	_, err := store.Get(ctx, "LeaderLease", "", "scheduler")
	if err == nil {
		return 0, fmt.Errorf("scheduler: lock is held")
	}
	if !errors.Is(err, registry.ErrNotFound) {
		return 0, err
	}
	lease := &api.LeaderLease{
		Metadata: api.ObjectMeta{Name: "scheduler"},
		Spec: api.LeaseSpec{
			HolderIdentity: holder,
			FencingToken:   1,
		},
	}
	if err := store.Create(ctx, lease); err != nil {
		return 0, err
	}
	return 1, nil
}

// Bind assigns spec.nodeName on one Pod.
// A token that is not the lock's current fencing token returns an error
// and leaves the Pod unchanged. A Pod that already has a node name returns nil.
// Phase is not written. Update conflicts are returned to the caller.
func Bind(ctx context.Context, store registry.Store, namespace, name string, token uint64) error {
	obj, err := store.Get(ctx, "LeaderLease", "", "scheduler")
	if err != nil {
		return err
	}
	lease := obj.(*api.LeaderLease)
	if lease.Spec.FencingToken != token {
		return fmt.Errorf("scheduler: fencing token mismatch")
	}

	obj, err = store.Get(ctx, "Pod", namespace, name)
	if err != nil {
		return err
	}
	pod := obj.(*api.Pod)
	if pod.Spec.NodeName != "" {
		return nil
	}

	nodes, _, err := store.List(ctx, "Node", "")
	if err != nil {
		return err
	}
	pods, _, err := store.List(ctx, "Pod", namespace)
	if err != nil {
		return err
	}
	nodeName, err := pickNode(nodes, pods)
	if err != nil {
		return err
	}
	pod.Spec.NodeName = nodeName
	return store.Update(ctx, pod)
}

func pickNode(nodes, pods []api.Object) (string, error) {
	counts := map[string]int{}
	var ready []string
	for _, obj := range nodes {
		node := obj.(*api.Node)
		if !node.Status.Ready {
			continue
		}
		ready = append(ready, node.Metadata.Name)
		counts[node.Metadata.Name] = 0
	}
	if len(ready) == 0 {
		return "", fmt.Errorf("scheduler: no ready nodes")
	}
	for _, obj := range pods {
		pod := obj.(*api.Pod)
		if _, ok := counts[pod.Spec.NodeName]; ok {
			counts[pod.Spec.NodeName]++
		}
	}
	for _, name := range ready {
		if counts[name] == 0 {
			return name, nil
		}
	}
	return ready[0], nil
}
