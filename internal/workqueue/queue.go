package workqueue

import "sync"

// Queue is the work queue a controller handler fills.
// Get blocks until a key is waiting or the queue is shut down.
// Add of a key that is already queued or being processed does nothing.
// Done marks that key finished, so a later Add can queue it again.
type Queue interface {
	Add(key string)
	Get() (key string, shutdown bool)
	Done(key string)
	ShutDown()
}

// Type is an in-memory queue.
// dirty means the key needs processing. processing means Get took it
// and Done has not been called.
type Type struct {
	mu         sync.Mutex
	cond       *sync.Cond
	dirty      map[string]bool
	processing map[string]bool
	queue      []string
	shutDown   bool
}

func New() *Type {
	q := &Type{
		dirty:      make(map[string]bool),
		processing: make(map[string]bool),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *Type) Add(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.shutDown || q.dirty[key] {
		return
	}
	q.dirty[key] = true
	if !q.processing[key] {
		q.queue = append(q.queue, key)
		q.cond.Signal()
	}
}

func (q *Type) Get() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.queue) == 0 && !q.shutDown {
		q.cond.Wait()
	}
	if len(q.queue) == 0 {
		return "", true
	}
	key := q.queue[0]
	q.queue = q.queue[1:]
	delete(q.dirty, key)
	q.processing[key] = true
	return key, false
}

func (q *Type) Done(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.processing, key)
	if q.dirty[key] {
		q.queue = append(q.queue, key)
		q.cond.Signal()
	}
}

func (q *Type) ShutDown() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.shutDown = true
	q.cond.Broadcast()
}
