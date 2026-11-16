package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"time"

	"k8s-go/internal/api"
	"k8s-go/internal/apiserver"
	nodectl "k8s-go/internal/controller/node"
	"k8s-go/internal/controller/replicaset"
	"k8s-go/internal/kubelet"
	"k8s-go/internal/kv"
	heartbeat "k8s-go/internal/node"
	"k8s-go/internal/raft"
	"k8s-go/internal/registry"
	"k8s-go/internal/scheduler"
)

func Run(ctx context.Context, dir, addr string) (string, error) {
	kvStore, err := kv.Open(filepath.Join(dir, "registry.wal"))
	if err != nil {
		return "", err
	}
	store := registry.New(kvStore)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	go http.Serve(ln, apiserver.New(store))
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	go func() {
		token, err := scheduler.Campaign(ctx, store, "scheduler")
		if err != nil {
			return
		}
		until(ctx, func() {
			items, _, err := store.List(ctx, "Pod", "default")
			if err != nil {
				return
			}
			for _, item := range items {
				pod := item.(*api.Pod)
				if pod.Spec.NodeName != "" {
					continue
				}
				scheduler.Bind(ctx, store, pod.Metadata.Namespace, pod.Metadata.Name, token)
			}
		})
	}()

	go func() {
		if err := takeControllerLock(ctx, store); err != nil {
			return
		}
		until(ctx, func() {
			nodectl.Reconcile(ctx, store, time.Now())
			items, _, err := store.List(ctx, "ReplicaSet", "default")
			if err != nil {
				return
			}
			for _, item := range items {
				rs := item.(*api.ReplicaSet)
				replicaset.Reconcile(ctx, store, rs.Metadata.Namespace, rs.Metadata.Name)
			}
		})
	}()

	for _, name := range []string{"node-a", "node-b"} {
		go func(name string) {
			if err := heartbeat.Register(ctx, store, name, time.Now()); err != nil {
				return
			}
			until(ctx, func() {
				heartbeat.Beat(ctx, store, name, time.Now())
				kubelet.Reconcile(ctx, store, name, kubelet.StatusRuntime{})
			})
		}(name)
	}

	return "http://" + ln.Addr().String(), nil
}

func until(ctx context.Context, fn func()) {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		fn()
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func takeControllerLock(ctx context.Context, store registry.Store) error {
	_, err := store.Get(ctx, "LeaderLease", "", "controller-manager")
	if err == nil || !errors.Is(err, registry.ErrNotFound) {
		return err
	}
	return store.Create(ctx, &api.LeaderLease{
		Metadata: api.ObjectMeta{Name: "controller-manager"},
		Spec:     api.LeaseSpec{HolderIdentity: "controller-manager", FencingToken: 1},
	})
}

// RunRaft is Run with a three-node raft.Open instead of one kv.Open.
// The API server, scheduler, controller manager, and both kubelets share
// raft.Registry(cluster). Return the server URL and the cluster.
// In writeError, raft.ErrUnavailable is HTTP 503.
func RunRaft(ctx context.Context, dir, addr string) (string, *raft.Cluster, error) {
	cluster, err := raft.Open(dir, "A", "B", "C")
	if err != nil {
		return "", nil, err
	}
	store := raft.Registry(cluster)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		cluster.Close()
		return "", nil, err
	}
	go http.Serve(ln, apiserver.New(store))
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	go func() {
		token, err := scheduler.Campaign(ctx, store, "scheduler")
		if err != nil {
			return
		}
		until(ctx, func() {
			items, _, err := store.List(ctx, "Pod", "default")
			if err != nil {
				return
			}
			for _, item := range items {
				pod := item.(*api.Pod)
				if pod.Spec.NodeName != "" {
					continue
				}
				scheduler.Bind(ctx, store, pod.Metadata.Namespace, pod.Metadata.Name, token)
			}
		})
	}()

	go func() {
		if err := takeControllerLock(ctx, store); err != nil {
			return
		}
		until(ctx, func() {
			nodectl.Reconcile(ctx, store, time.Now())
			items, _, err := store.List(ctx, "ReplicaSet", "default")
			if err != nil {
				return
			}
			for _, item := range items {
				rs := item.(*api.ReplicaSet)
				replicaset.Reconcile(ctx, store, rs.Metadata.Namespace, rs.Metadata.Name)
			}
		})
	}()

	for _, name := range []string{"node-a", "node-b"} {
		go func(name string) {
			if err := heartbeat.Register(ctx, store, name, time.Now()); err != nil {
				return
			}
			until(ctx, func() {
				heartbeat.Beat(ctx, store, name, time.Now())
				kubelet.Reconcile(ctx, store, name, kubelet.StatusRuntime{})
			})
		}(name)
	}

	return "http://" + ln.Addr().String(), cluster, nil
}
