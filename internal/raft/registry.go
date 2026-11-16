package raft

import (
	"context"
	"errors"

	"k8s-go/internal/api"
	"k8s-go/internal/registry"
)

// ErrUnavailable means this call cannot be served by the current leader.
var ErrUnavailable = errors.New("raft: unavailable")

// Registry serves registry objects through the current leader's store.
// Get, List, and Watch read that store. Create, Update, UpdateStatus, and
// Delete write it. There is no leader, or the leader cannot reach a majority:
// return ErrUnavailable.
//
// A write has to reach the other reachable nodes too. Sync copies the Raft
// log, and these objects are not in that log. Install store.OnWrite on every
// node. When the written store belongs to the current leader, Put the same
// value on each reachable follower, or Delete the key when deleted is true.
// The hook runs while that store is locked, so it must not call back into it.
func Registry(c *Cluster) registry.Store {
	s := leaderStore{c: c, backends: map[string]registry.Store{}}
	for _, n := range c.nodes {
		n := n
		s.backends[n.id] = registry.New(n.store)
		n.store.OnWrite(func(key, value string, deleted bool) {
			if c.leader() != n {
				return
			}
			for _, other := range c.nodes {
				if other == n || !n.canReach(other) {
					continue
				}
				if deleted {
					other.store.Delete(key)
				} else {
					other.store.Put(key, value)
				}
			}
		})
	}
	return s
}

type leaderStore struct {
	c        *Cluster
	backends map[string]registry.Store
}

func (s leaderStore) backend() (registry.Store, error) {
	leader := s.c.leader()
	if leader == nil {
		return nil, ErrUnavailable
	}
	majority := len(s.c.nodes)/2 + 1
	reachable := 0
	for _, n := range s.c.nodes {
		if leader.canReach(n) {
			reachable++
		}
	}
	if reachable < majority {
		return nil, ErrUnavailable
	}
	return s.backends[leader.id], nil
}

func (s leaderStore) Create(ctx context.Context, obj api.Object) error {
	store, err := s.backend()
	if err != nil {
		return err
	}
	return store.Create(ctx, obj)
}

func (s leaderStore) Get(ctx context.Context, kind, namespace, name string) (api.Object, error) {
	store, err := s.backend()
	if err != nil {
		return nil, err
	}
	return store.Get(ctx, kind, namespace, name)
}

func (s leaderStore) Update(ctx context.Context, obj api.Object) error {
	store, err := s.backend()
	if err != nil {
		return err
	}
	return store.Update(ctx, obj)
}

func (s leaderStore) UpdateStatus(ctx context.Context, obj api.Object) error {
	store, err := s.backend()
	if err != nil {
		return err
	}
	return store.UpdateStatus(ctx, obj)
}

func (s leaderStore) Delete(ctx context.Context, kind, namespace, name string) error {
	store, err := s.backend()
	if err != nil {
		return err
	}
	return store.Delete(ctx, kind, namespace, name)
}

func (s leaderStore) List(ctx context.Context, kind, namespace string) ([]api.Object, uint64, error) {
	store, err := s.backend()
	if err != nil {
		return nil, 0, err
	}
	return store.List(ctx, kind, namespace)
}

func (s leaderStore) Watch(ctx context.Context, kind, namespace string, rev uint64) (<-chan registry.Event, error) {
	store, err := s.backend()
	if err != nil {
		return nil, err
	}
	return store.Watch(ctx, kind, namespace, rev)
}
