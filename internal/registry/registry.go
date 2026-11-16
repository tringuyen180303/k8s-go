package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"k8s-go/internal/api"
	"k8s-go/internal/kv"
)

var (
	ErrConflict = errors.New("registry: conflict")
	ErrNotFound = errors.New("registry: not found")
)

type EventType string

const (
	Added    EventType = "ADDED"
	Modified EventType = "MODIFIED"
	Deleted  EventType = "DELETED"
)

// Event is one watch notification. Object is the decoded Pod, Node, or ReplicaSet.
type Event struct {
	Type   EventType  `json:"type"`
	Object api.Object `json:"object"`
}

// Store is the only API that knows etcd key paths.
type Store interface {
	Create(ctx context.Context, obj api.Object) error
	Get(ctx context.Context, kind, namespace, name string) (api.Object, error)
	Update(ctx context.Context, obj api.Object) error
	UpdateStatus(ctx context.Context, obj api.Object) error
	Delete(ctx context.Context, kind, namespace, name string) error
	List(ctx context.Context, kind, namespace string) ([]api.Object, uint64, error)
	Watch(ctx context.Context, kind, namespace string, fromRev uint64) (<-chan Event, error)
}

// Memory stores objects in a kv.Store.
type Memory struct {
	mu      sync.Mutex
	kv      *kv.Store
	nextUID uint64
}

func New(s *kv.Store) Store {
	return &Memory{kv: s}
}

// objectKey returns the etcd key for one object.
// Pod default/nginx is /registry/pods/default/nginx.
// Node node-a is /registry/nodes/node-a.
// ReplicaSet default/web is /registry/replicasets/default/web.
func objectKey(kind, namespace, name string) (string, error) {
	// TODO: switch on kind. A namespaced kind needs namespace and name.
	// A Node needs only name. Anything else returns an error.
	switch kind {
	case "Pod":
		return fmt.Sprintf("/registry/pods/%s/%s", namespace, name), nil
	case "Node":
		return fmt.Sprintf("/registry/nodes/%s", name), nil
	case "Lease":
		return fmt.Sprintf("/registry/leases/nodes/%s", name), nil
	case "LeaderLease":
		return fmt.Sprintf("/registry/leases/leaders/%s", name), nil
	case "ReplicaSet":
		return fmt.Sprintf("/registry/replicasets/%s/%s", namespace, name), nil
	case "Service":
		return fmt.Sprintf("/registry/services/%s/%s", namespace, name), nil
	case "Endpoints":
		return fmt.Sprintf("/registry/endpoints/%s/%s", namespace, name), nil
	default:
		return "", fmt.Errorf("registry: unknown kind: %s", kind)
	}

}

// prefixKey returns the List and Watch prefix, with a trailing slash.
func prefixKey(kind, namespace string) (string, error) {
	// TODO: /registry/pods/{namespace}/, /registry/nodes/,
	// /registry/replicasets/{namespace}/.
	switch kind {
	case "Pod":
		return fmt.Sprintf("/registry/pods/%s/", namespace), nil
	case "Node":
		return "/registry/nodes/", nil
	case "Lease":
		return "/registry/leases/nodes/", nil
	case "LeaderLease":
		return "/registry/leases/leaders/", nil
	case "ReplicaSet":
		return fmt.Sprintf("/registry/replicasets/%s/", namespace), nil
	case "Service":
		return fmt.Sprintf("/registry/services/%s/", namespace), nil
	case "Endpoints":
		return fmt.Sprintf("/registry/endpoints/%s/", namespace), nil
	default:
		return "", fmt.Errorf("registry: unknown kind: %s", kind)
	}
}

func (m *Memory) Create(ctx context.Context, obj api.Object) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	meta := obj.GetObjectMeta()
	meta.DefaultNamespace(obj.Kind() != "Node" && obj.Kind() != "Lease" && obj.Kind() != "LeaderLease")
	if meta.UID == "" {
		m.nextUID++
		meta.UID = fmt.Sprintf("%d", m.nextUID)
	}
	meta.ResourceVersion = m.kv.Revision() + 1
	body, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	key, err := objectKey(obj.Kind(), meta.Namespace, meta.Name)
	if err != nil {
		return err
	}
	rev, err := m.kv.Update(key, string(body), 0)
	if err != nil {
		return ErrConflict
	}
	if rev != meta.ResourceVersion {
		return fmt.Errorf("registry: revision %d, stored %d", rev, meta.ResourceVersion)
	}
	return nil
}

func (m *Memory) Get(ctx context.Context, kind, namespace, name string) (api.Object, error) {
	key, err := objectKey(kind, namespace, name)
	if err != nil {
		return nil, err
	}
	value, ok := m.kv.Get(key)
	if !ok {
		return nil, ErrNotFound
	}
	return decode(kind, value)
}

func (m *Memory) Update(ctx context.Context, obj api.Object) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.update(ctx, obj)
}

func (m *Memory) update(ctx context.Context, obj api.Object) error {
	meta := obj.GetObjectMeta()
	expect := meta.ResourceVersion
	meta.ResourceVersion = m.kv.Revision() + 1
	body, err := json.Marshal(obj)
	if err != nil {
		meta.ResourceVersion = expect
		return err
	}
	key, err := objectKey(obj.Kind(), meta.Namespace, meta.Name)
	if err != nil {
		meta.ResourceVersion = expect
		return err
	}
	rev, err := m.kv.Update(key, string(body), expect)
	if err != nil {
		meta.ResourceVersion = expect
		return ErrConflict
	}
	meta.ResourceVersion = rev
	return nil
}

func (m *Memory) UpdateStatus(ctx context.Context, obj api.Object) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	meta := obj.GetObjectMeta()
	current, err := m.Get(ctx, obj.Kind(), meta.Namespace, meta.Name)
	if err != nil {
		return err
	}
	switch next := obj.(type) {
	case *api.Pod:
		current.(*api.Pod).Status = next.Status
	case *api.Node:
		current.(*api.Node).Status = next.Status
	case *api.ReplicaSet:
		current.(*api.ReplicaSet).Status = next.Status
	default:
		return fmt.Errorf("registry: unknown kind %s", obj.Kind())
	}
	current.GetObjectMeta().ResourceVersion = meta.ResourceVersion
	return m.update(ctx, current)
}

func (m *Memory) Delete(ctx context.Context, kind, namespace, name string) error {
	key, err := objectKey(kind, namespace, name)
	if err != nil {
		return err
	}
	ok, err := m.kv.Delete(key)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotFound
	}
	return nil
}

func (m *Memory) List(ctx context.Context, kind, namespace string) ([]api.Object, uint64, error) {
	prefix, err := prefixKey(kind, namespace)
	if err != nil {
		return nil, 0, err
	}
	items := m.kv.List(prefix)
	objects := make([]api.Object, 0, len(items))
	for _, item := range items {
		obj, err := decode(kind, item.Value)
		if err != nil {
			return nil, 0, err
		}
		objects = append(objects, obj)
	}
	return objects, m.kv.Revision(), nil
}

func (m *Memory) Watch(ctx context.Context, kind, namespace string, fromRev uint64) (<-chan Event, error) {
	prefix, err := prefixKey(kind, namespace)
	if err != nil {
		return nil, err
	}
	in := m.kv.WatchPrefix(ctx, prefix, fromRev)
	out := make(chan Event)
	go func() {
		defer close(out)
		for ev := range in {
			if ev.Deleted {
				obj, err := decode(kind, "{}")
				if err != nil {
					continue
				}
				obj.GetObjectMeta().Name = strings.TrimPrefix(ev.Key, prefix)
				obj.GetObjectMeta().Namespace = namespace
				out <- Event{Type: Deleted, Object: obj}
				continue
			}
			obj, err := decode(kind, ev.Value)
			if err != nil {
				continue
			}
			out <- Event{Type: Modified, Object: obj}
		}
	}()
	return out, nil
}

func decode(kind, value string) (api.Object, error) {
	switch kind {
	case "Pod":
		obj := &api.Pod{}
		err := json.Unmarshal([]byte(value), obj)
		return obj, err
	case "Node":
		obj := &api.Node{}
		err := json.Unmarshal([]byte(value), obj)
		return obj, err
	case "Lease":
		obj := &api.Lease{}
		err := json.Unmarshal([]byte(value), obj)
		return obj, err
	case "LeaderLease":
		obj := &api.LeaderLease{}
		err := json.Unmarshal([]byte(value), obj)
		return obj, err
	case "ReplicaSet":
		obj := &api.ReplicaSet{}
		err := json.Unmarshal([]byte(value), obj)
		return obj, err
	case "Service":
		obj := &api.Service{}
		err := json.Unmarshal([]byte(value), obj)
		return obj, err
	case "Endpoints":
		obj := &api.Endpoints{}
		err := json.Unmarshal([]byte(value), obj)
		return obj, err
	default:
		return nil, fmt.Errorf("registry: unknown kind: %s", kind)
	}
}
