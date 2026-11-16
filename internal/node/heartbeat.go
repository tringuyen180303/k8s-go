package node

import (
	"context"
	"time"

	"k8s-go/internal/api"
	"k8s-go/internal/registry"
)

// Register creates the Node and its Lease at now.
// The node is Ready, and the lease renew time is now. The lease lasts 30 seconds.
func Register(ctx context.Context, store registry.Store, name string, now time.Time) error {
	node := &api.Node{
		Metadata: api.ObjectMeta{Name: name},
		Status: api.NodeStatus{
			Ready:         true,
			LastHeartbeat: now,
		},
	}
	if err := store.Create(ctx, node); err != nil {
		return err
	}
	lease := &api.Lease{
		Metadata: api.ObjectMeta{Name: name},
		Spec: api.LeaseSpec{
			HolderIdentity:       name,
			RenewTime:            now,
			LeaseDurationSeconds: 30,
		},
	}
	return store.Create(ctx, lease)
}

// Beat refreshes one node at now. Ready stays true.
// Get the Node, set its status, and UpdateStatus.
// Get the Lease, set RenewTime to now, and Update.
func Beat(ctx context.Context, store registry.Store, name string, now time.Time) error {
	obj, err := store.Get(ctx, "Node", "", name)
	if err != nil {
		return err
	}
	node := obj.(*api.Node)
	node.Status.Ready = true
	node.Status.LastHeartbeat = now
	if err := store.UpdateStatus(ctx, node); err != nil {
		return err
	}

	obj, err = store.Get(ctx, "Lease", "", name)
	if err != nil {
		return err
	}
	lease := obj.(*api.Lease)
	lease.Spec.RenewTime = now
	return store.Update(ctx, lease)
}
