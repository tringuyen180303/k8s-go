package raft

import (
	"fmt"
	"path/filepath"
	"k8s-go/internal/kv"
)

// Role is follower, candidate, or leader.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// Entry is one operation in the replicated log.
// Every node that accepts the operation stores the same Index.
type Entry struct {
	Index uint64
	Key   string
	Value string
}

// Node is one Raft server. The log is the source of committed operations.
// The store is updated only after an entry is committed.
type Node struct {
	id       string
	log      []Entry
	store    *kv.Store
	online   bool
	term     uint64
	votedFor string
	role     Role
	group    int
}

// Cluster is a fixed group. The leader is whoever won the latest election.
type Cluster struct {
	nodes     []*Node
	nextGroup int
}

// New starts one in-memory server per id. The first id is the leader at term 1.
func New(ids ...string) *Cluster {
	c := &Cluster{}
	for i, id := range ids {
		role := Follower
		if i == 0 {
			role = Leader
		}
		c.nodes = append(c.nodes, &Node{
			id:     id,
			store:  kv.New(),
			online: true,
			term:   1,
			role:   role,
		})
	}
	return c
}

// Open starts one durable server per id. Each node uses
// kv.Open(filepath.Join(dir, id+".wal")). The first id is the leader at term 1.
// Put and Sync already call store.Put. A durable store records that Put in the WAL.
func Open(dir string, ids ...string) (*Cluster, error) {
	c := &Cluster{}
	for i, id := range ids {
		role := Follower
		if i == 0 {
			role = Leader
		}
		store, err := kv.Open(filepath.Join(dir, id+".wal"))
		if err != nil {
			c.Close()
			return nil, err
		}
		c.nodes = append(c.nodes, &Node{
			id:     id,
			store:  store,
			online: true,
			term:   1,
			role:   role,
		})
	}
	return c, nil
}

// Close closes every node's store.
func (c *Cluster) Close() error {
	for _, n := range c.nodes {
		if err := n.store.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Node returns the server with this id, or nil when id is unknown.
func (c *Cluster) Node(id string) *Node {
	for _, n := range c.nodes {
		if n.id == id {
			return n
		}
	}
	return nil
}

// Down marks the server unreachable.
func (n *Node) Down() { n.online = false }

// Up marks the server reachable. It does not copy log entries the server missed.
func (n *Node) Up() { n.online = true }

// Isolate moves id into a group by itself.
// Nodes in different groups cannot exchange votes or log entries.
// The isolated node stays online and keeps its current role.
func (c *Cluster) Isolate(id string) {
	n := c.Node(id)
	if n == nil {
		return
	}
	c.nextGroup++
	n.group = c.nextGroup
}

// canReach reports whether a message from n can arrive at other.
// A node can reach itself. A down node can reach nobody.
func (n *Node) canReach(other *Node) bool {
	if n == nil || other == nil || !n.online || !other.online {
		return false
	}
	return n.group == other.group
}

// Get reads the store. A committed write is visible here.
func (n *Node) Get(key string) (string, bool) {
	return n.store.Get(key)
}

// Role returns follower, candidate, or leader.
func (n *Node) Role() Role { return n.role }

// Term returns the node's current election term.
func (n *Node) Term() uint64 { return n.term }

// leader is the online leader with the highest term.
func (c *Cluster) leader() *Node {
	var best *Node
	for _, n := range c.nodes {
		if !n.online || n.role != Leader {
			continue
		}
		if best == nil || n.term > best.term {
			best = n
		}
	}
	return best
}

// LastIndex is the index of the last log entry, or 0 when the log is empty.
func (n *Node) LastIndex() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

// Put appends one entry on the leader and commits it only with a majority.
func (c *Cluster) Put(key, value string) error {
	// TODO:
	// 1. The leader is c.nodes[0]. If leader.online is false, return an error.
	// 2. Count nodes with online == true. majority is len(c.nodes)/2 + 1.
	//    When the count is below majority, return an error.
	//    Do not append and do not call store.Put in that case.
	// 3. Build one Entry. Index is leader.LastIndex() + 1.
	//    For every online node, append that same entry to n.log,
	//    then n.store.Put(key, value). Leave offline nodes unchanged.
	leader := c.leader()
	if leader == nil {
		return fmt.Errorf("leader is offline")
	}
	majority := len(c.nodes)/2 + 1
	reachable := 0
	for _, n := range c.nodes {
		if leader.canReach(n) {
			reachable++
		}
	}
	if reachable < majority {
		return fmt.Errorf("not enough nodes reachable")
	}
	entry := Entry{Index: leader.LastIndex() + 1, Key: key, Value: value}
	for _, n := range c.nodes {
		if !leader.canReach(n) {
			continue
		}
		n.log = append(n.log, entry)
		n.store.Put(key, value)
	}
	return nil
}

// Sync copies log entries a follower missed while it was down.
// The leader's log is the source. Each missing entry is appended, then applied
// to that follower's store, in index order.
func (c *Cluster) Sync() error {
	// TODO:
	// 1. The leader is c.nodes[0]. If it is offline, return an error.
	// 2. For every other node that is online, walk leader.log.
	//    Skip an entry whose Index is <= n.LastIndex().
	//    Append each later entry to n.log, then n.store.Put(entry.Key, entry.Value).
	// 3. Leave offline nodes unchanged. Return nil when the online followers
	//    have caught up.
	leader := c.leader()
	if leader == nil {
		return fmt.Errorf("leader is offline")
	}
	for _, n := range c.nodes {
		if !leader.canReach(n) {
			continue
		}
		if n.LastIndex() >= leader.log[len(leader.log)-1].Index {
			continue
		}
		for _, entry := range leader.log {
			if entry.Index <= n.LastIndex() {
				continue
			}
			n.log = append(n.log, entry)
			n.store.Put(entry.Key, entry.Value)
		}
	}
	return nil
}

// Elect asks the online nodes to vote for id.
// A candidate becomes leader only with a majority of votes in a new term.
func (c *Cluster) Elect(id string) error {
	// TODO:
	// 1. Find the node. Unknown or offline: return an error.
	// 2. Increment its term. Set role to Candidate and votedFor to its own id.
	//    Start the vote count at 1.
	// 3. Every other online node with a lower term grants the vote.
	//    Set that node's term to the candidate term, votedFor to the candidate id,
	//    and role to Follower. Add one to the vote count.
	//    Skip offline nodes. They keep their old role and term.
	// 4. majority is len(c.nodes)/2 + 1. With fewer votes, set the candidate
	//    back to Follower and return an error.
	// 5. With a majority, set every other online Leader to Follower, then set
	//    the candidate's role to Leader.
	node := c.Node(id)
	if node == nil {
		return fmt.Errorf("node is unknown")
	}
	node.term++
	node.role = Candidate
	node.votedFor = id
	count := 1
	for _, n := range c.nodes {
		if !node.canReach(n) {
			continue
		}
		if n.term < node.term {
			n.term = node.term
			n.votedFor = id
			n.role = Follower
			count++
		}
	}
	majority := len(c.nodes)/2 + 1
	if count < majority {
		node.role = Follower
		return fmt.Errorf("not enough votes")
	}
	for _, n := range c.nodes {
		if !node.canReach(n) {
			continue
		}
		if n.role == Leader {
			n.role = Follower
		}
	}
	node.role = Leader
	return nil
}

// Read returns the value of key from node id only when that read is linearizable.
// A follower, or a leader that cannot reach a majority, cannot serve the read.
// Node.Get remains the local store and can be stale.
func (c *Cluster) Read(id, key string) (string, bool, error) {
	// TODO:
	// 1. Find the node. Unknown or offline: return an error.
	// 2. If its role is not Leader, return an error. Do not read its store.
	// 3. Count nodes it canReach, including itself. majority is len(c.nodes)/2 + 1.
	//    Below majority: return an error. Do not read its store.
	// 4. Return n.Get(key). A missing key is "", false, and a nil error.
	node := c.Node(id)
	if node == nil {
		return "", false, fmt.Errorf("node is unknown")
	}
	if node.role != Leader {
		return "", false, fmt.Errorf("node is not leader")
	}
	majority := len(c.nodes)/2 + 1
	reachable := 0
	for _, n := range c.nodes {
		if node.canReach(n) {
			reachable++
		}
	}
	if reachable < majority {
		return "", false, fmt.Errorf("not enough nodes reachable")
	}
	value, ok := node.Get(key)
	return value, ok, nil
}
