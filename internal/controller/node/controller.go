package node

import (
	"context"
	"errors"
	"time"

	"k8s-go/internal/api"
	"k8s-go/internal/registry"
)

// Reconcile marks every node whose lease is older than 30 seconds NotReady.
// A lease renewed 20 seconds ago is still fresh. A missing lease is NotReady.
// Pass now in from the caller. Do not sleep or read the wall clock.
func Reconcile(ctx context.Context, store registry.Store, now time.Time) error {
	nodes, _, err := store.List(ctx, "Node", "")
	if err != nil {
		return err
	}
	for _, obj := range nodes {
		node := obj.(*api.Node)
		ready := false
		leaseObj, err := store.Get(ctx, "Lease", "", node.Metadata.Name)
		if err != nil && !errors.Is(err, registry.ErrNotFound) {
			return err
		}
		if err == nil {
			lease := leaseObj.(*api.Lease)
			ready = now.Sub(lease.Spec.RenewTime) <= 30*time.Second
		}
		node.Status.Ready = ready
		if err := store.UpdateStatus(ctx, node); err != nil {
			return err
		}
	}
	return nil
}
