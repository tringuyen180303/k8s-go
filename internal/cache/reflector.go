package cache

import (
	"context"

	"k8s-go/internal/api"
	"k8s-go/internal/registry"
)

// ListerWatcher is the list-then-watch source a reflector reads.
// The HTTP client is one implementation. Tests pass a fake.
type ListerWatcher interface {
	List(ctx context.Context) ([]api.Object, uint64, error)
	Watch(ctx context.Context, rev uint64) (<-chan registry.Event, error)
}

// Handler receives objects in order. A controller handler only enqueues
// MetaNamespaceKey(obj) and returns. Reconcile does the read.
type Handler interface {
	OnAdd(obj api.Object)
	OnUpdate(obj api.Object)
	OnDelete(obj api.Object)
}

// MetaNamespaceKey is the work-queue key. A Pod is "default/nginx".
// A Node has an empty namespace, so the key is "node-a".
func MetaNamespaceKey(obj api.Object) string {
	meta := obj.GetObjectMeta()
	if meta.Namespace == "" {
		return meta.Name
	}
	return meta.Namespace + "/" + meta.Name
}

type DeltaType string

const (
	Added   DeltaType = "Added"
	Updated DeltaType = "Updated"
	Deleted DeltaType = "Deleted"
)

// Delta is one list or watch change waiting for the handler.
type Delta struct {
	Type   DeltaType
	Object api.Object
}

// FIFO keeps deltas in the order they were added. Pop blocks until one
// is waiting or ctx is done.
type FIFO struct {
	ch chan Delta
}

func (f *FIFO) Add(d Delta) {
	f.ch <- d
}

func (f *FIFO) Pop(ctx context.Context) (Delta, bool) {
	select {
	case <-ctx.Done():
		return Delta{}, false
	case d, ok := <-f.ch:
		return d, ok
	}
}

// Informer lists, watches, and delivers each change to the handler.
type Informer struct {
	lw      ListerWatcher
	handler Handler
	fifo    *FIFO
}

func New(lw ListerWatcher, h Handler) *Informer {
	return &Informer{lw: lw, handler: h, fifo: &FIFO{ch: make(chan Delta, 16)}}
}

func (i *Informer) Run(ctx context.Context) {
	go i.reflect(ctx)
	for {
		d, ok := i.fifo.Pop(ctx)
		if !ok {
			return
		}
		switch d.Type {
		case Added:
			i.handler.OnAdd(d.Object)
		case Updated:
			i.handler.OnUpdate(d.Object)
		case Deleted:
			i.handler.OnDelete(d.Object)
		}
	}
}

func (i *Informer) reflect(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		objs, rev, err := i.lw.List(ctx)
		if err != nil {
			return
		}
		for _, obj := range objs {
			select {
			case <-ctx.Done():
				return
			case i.fifo.ch <- Delta{Type: Added, Object: obj}:
			}
		}
		ch, err := i.lw.Watch(ctx, rev)
		if err != nil {
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-ch:
				if !ok {
					goto relist
				}
				kind := Updated
				switch ev.Type {
				case registry.Added:
					kind = Added
				case registry.Deleted:
					kind = Deleted
				}
				select {
				case <-ctx.Done():
					return
				case i.fifo.ch <- Delta{Type: kind, Object: ev.Object}:
				}
			}
		}
	relist:
	}
}
