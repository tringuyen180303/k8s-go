package cluster

import (
	"k8s-go/internal/kv"
)

// Node is one copy of the key-value store.
// online is false when this process cannot be reached.
type Node struct {
	id     string
	store  *kv.Store
	online bool
}

// Cluster is three or more nodes that a client writes to.
type Cluster struct {
	nodes []*Node
}

// New builds one in-memory store per id. Every node starts online.
func New(ids ...string) *Cluster {
	c := &Cluster{}
	for _, id := range ids {
		c.nodes = append(c.nodes, &Node{
			id:     id,
			store:  kv.New(),
			online: true,
		})
	}
	return c
}

// Node returns the member with this id, or nil when id is unknown.
func (c *Cluster) Node(id string) *Node {
	for _, n := range c.nodes {
		if n.id == id {
			return n
		}
	}
	return nil
}

// Down marks the node unreachable.
func (n *Node) Down() { n.online = false }

// Up marks the node reachable again. It does not copy any missed writes.
func (n *Node) Up() { n.online = true }

// Get reads this node's own store.
func (n *Node) Get(key string) (string, bool) {
	return n.store.Get(key)
}

// Put writes key to every node that is currently online.
// An offline node is skipped, and Put still returns nil.
// The skipped node keeps its old value. That disagreement is the bug.
func (c *Cluster) Put(key, value string) error {
	// TODO: range c.nodes. Skip a node when n.online is false.
	// On each online node, call n.store.Put(key, value) and return its error
	// if that call fails. Return nil after the loop, even if a node was skipped.
	for _, n := range c.nodes {
		if !n.online {
			continue
		}
		err := n.store.Put(key, value)
		if err != nil {
			return err
		}
	}
	return nil
}
