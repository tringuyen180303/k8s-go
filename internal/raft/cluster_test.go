package raft

import "testing"

func TestPutCommitsOnEveryNode(t *testing.T) {
	c := New("A", "B", "C")
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"A", "B", "C"} {
		n := c.Node(id)
		got, ok := n.Get("foo")
		if !ok || got != "bar" {
			t.Fatalf("node %s Get(foo) = %q, %v; want %q, true", id, got, ok, "bar")
		}
		if n.LastIndex() != 1 {
			t.Fatalf("node %s LastIndex = %d; want 1", id, n.LastIndex())
		}
	}
}

func TestOneFollowerDownStillCommits(t *testing.T) {
	c := New("A", "B", "C")
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	c.Node("C").Down()
	if err := c.Put("foo", "baz"); err != nil {
		t.Fatal(err)
	}

	for _, id := range []string{"A", "B"} {
		n := c.Node(id)
		got, ok := n.Get("foo")
		if !ok || got != "baz" {
			t.Fatalf("node %s Get(foo) = %q, %v; want %q, true", id, got, ok, "baz")
		}
		if n.LastIndex() != 2 {
			t.Fatalf("node %s LastIndex = %d; want 2", id, n.LastIndex())
		}
	}
	cNode := c.Node("C")
	got, ok := cNode.Get("foo")
	if !ok || got != "bar" {
		t.Fatalf("node C Get(foo) = %q, %v; want %q, true", got, ok, "bar")
	}
	if cNode.LastIndex() != 1 {
		t.Fatalf("node C LastIndex = %d; want 1", cNode.LastIndex())
	}
}

func TestNoQuorumDoesNotApply(t *testing.T) {
	c := New("A", "B", "C")
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	c.Node("B").Down()
	c.Node("C").Down()
	if err := c.Put("foo", "baz"); err == nil {
		t.Fatal("Put returned nil with 1 of 3 nodes online")
	}

	for _, id := range []string{"A", "B", "C"} {
		n := c.Node(id)
		got, ok := n.Get("foo")
		if !ok || got != "bar" {
			t.Fatalf("node %s Get(foo) = %q, %v; want %q, true", id, got, ok, "bar")
		}
		if n.LastIndex() != 1 {
			t.Fatalf("node %s LastIndex = %d; want 1", id, n.LastIndex())
		}
	}
}

func TestFollowerCatchesUp(t *testing.T) {
	c := New("A", "B", "C")
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	c.Node("C").Down()
	if err := c.Put("foo", "baz"); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("cat", "dog"); err != nil {
		t.Fatal(err)
	}
	c.Node("C").Up()

	behind, ok := c.Node("C").Get("foo")
	if !ok || behind != "bar" || c.Node("C").LastIndex() != 1 {
		t.Fatalf("C before Sync: Get=%q,%v LastIndex=%d", behind, ok, c.Node("C").LastIndex())
	}

	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	got, ok := c.Node("C").Get("foo")
	if !ok || got != "baz" {
		t.Fatalf("C Get(foo) = %q, %v; want %q, true", got, ok, "baz")
	}
	got, ok = c.Node("C").Get("cat")
	if !ok || got != "dog" {
		t.Fatalf("C Get(cat) = %q, %v; want %q, true", got, ok, "dog")
	}
	if c.Node("C").LastIndex() != 3 {
		t.Fatalf("C LastIndex = %d; want 3", c.Node("C").LastIndex())
	}
	if _, ok := c.Node("A").Get("cat"); !ok || c.Node("A").LastIndex() != 3 {
		t.Fatal("Sync changed the leader")
	}
}

func TestSyncSkipsOfflineFollower(t *testing.T) {
	c := New("A", "B", "C")
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	c.Node("C").Down()
	if err := c.Put("foo", "baz"); err != nil {
		t.Fatal(err)
	}
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	got, ok := c.Node("C").Get("foo")
	if !ok || got != "bar" || c.Node("C").LastIndex() != 1 {
		t.Fatalf("offline C Get=%q,%v LastIndex=%d", got, ok, c.Node("C").LastIndex())
	}
}

func TestLeaderDownDoesNotCommit(t *testing.T) {
	c := New("A", "B", "C")
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	c.Node("A").Down()
	if err := c.Put("foo", "baz"); err == nil {
		t.Fatal("Put returned nil while the leader is down")
	}
	for _, id := range []string{"B", "C"} {
		got, ok := c.Node(id).Get("foo")
		if !ok || got != "bar" {
			t.Fatalf("node %s Get(foo) = %q, %v; want %q, true", id, got, ok, "bar")
		}
	}
}

func TestElectionMovesLeadership(t *testing.T) {
	c := New("A", "B", "C")
	if c.Node("A").Role() != Leader || c.Node("A").Term() != 1 {
		t.Fatalf("A starts as leader term 1, got %v term %d", c.Node("A").Role(), c.Node("A").Term())
	}
	if err := c.Elect("B"); err != nil {
		t.Fatal(err)
	}
	if c.Node("B").Role() != Leader {
		t.Fatalf("B role = %v; want leader", c.Node("B").Role())
	}
	if c.Node("A").Role() != Follower {
		t.Fatalf("A role = %v; want follower", c.Node("A").Role())
	}
	if c.Node("B").Term() != 2 || c.Node("C").Term() != 2 {
		t.Fatalf("terms B=%d C=%d; want 2 and 2", c.Node("B").Term(), c.Node("C").Term())
	}
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	if c.Node("B").LastIndex() != 1 || c.Node("A").LastIndex() != 1 {
		t.Fatalf("indexes A=%d B=%d; want 1 and 1", c.Node("A").LastIndex(), c.Node("B").LastIndex())
	}
}

func TestElectionNeedsMajority(t *testing.T) {
	c := New("A", "B", "C")
	c.Node("A").Down()
	c.Node("C").Down()
	if err := c.Elect("B"); err == nil {
		t.Fatal("B won an election with one node online")
	}
	if c.Node("B").Role() == Leader {
		t.Fatal("B role = leader; want a non-leader")
	}
	if c.Node("B").Term() < 2 {
		t.Fatalf("B term = %d; want a new term even though the election failed", c.Node("B").Term())
	}
}

func TestDownLeaderKeepsStaleTerm(t *testing.T) {
	c := New("A", "B", "C")
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	c.Node("A").Down()
	if err := c.Elect("B"); err != nil {
		t.Fatal(err)
	}
	if c.Node("A").Role() != Leader || c.Node("A").Term() != 1 {
		t.Fatalf("down A role=%v term=%d; want leader term 1", c.Node("A").Role(), c.Node("A").Term())
	}
	if c.Node("B").Term() <= c.Node("A").Term() {
		t.Fatalf("B term %d; want a term above A's %d", c.Node("B").Term(), c.Node("A").Term())
	}
	if err := c.Put("foo", "baz"); err != nil {
		t.Fatal(err)
	}
	got, ok := c.Node("A").Get("foo")
	if !ok || got != "bar" {
		t.Fatalf("A Get(foo) = %q, %v; want %q, true", got, ok, "bar")
	}
	for _, id := range []string{"B", "C"} {
		got, ok = c.Node(id).Get("foo")
		if !ok || got != "baz" {
			t.Fatalf("%s Get(foo) = %q, %v; want %q, true", id, got, ok, "baz")
		}
	}
}

func TestIsolatedLeaderCannotCommit(t *testing.T) {
	c := New("A", "B", "C")
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	c.Isolate("A")
	if c.Node("A").Role() != Leader {
		t.Fatalf("A role = %v; want leader", c.Node("A").Role())
	}
	if err := c.Put("foo", "baz"); err == nil {
		t.Fatal("isolated leader committed a write")
	}
	for _, id := range []string{"A", "B", "C"} {
		got, ok := c.Node(id).Get("foo")
		if !ok || got != "bar" {
			t.Fatalf("%s Get(foo) = %q, %v; want %q, true", id, got, ok, "bar")
		}
	}
}

func TestPartitionElectsNewLeader(t *testing.T) {
	c := New("A", "B", "C")
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	c.Isolate("A")
	if err := c.Elect("B"); err != nil {
		t.Fatal(err)
	}
	if c.Node("A").Role() != Leader || c.Node("A").Term() != 1 {
		t.Fatalf("A role=%v term=%d; want leader term 1", c.Node("A").Role(), c.Node("A").Term())
	}
	if c.Node("B").Role() != Leader || c.Node("B").Term() <= 1 {
		t.Fatalf("B role=%v term=%d; want leader above term 1", c.Node("B").Role(), c.Node("B").Term())
	}
	if err := c.Put("foo", "baz"); err != nil {
		t.Fatal(err)
	}
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	got, ok := c.Node("A").Get("foo")
	if !ok || got != "bar" {
		t.Fatalf("A Get(foo) = %q, %v; want %q, true", got, ok, "bar")
	}
	for _, id := range []string{"B", "C"} {
		got, ok = c.Node(id).Get("foo")
		if !ok || got != "baz" {
			t.Fatalf("%s Get(foo) = %q, %v; want %q, true", id, got, ok, "baz")
		}
	}
}

func TestReadFromLeader(t *testing.T) {
	c := New("A", "B", "C")
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	got, ok, err := c.Read("A", "foo")
	if err != nil || !ok || got != "bar" {
		t.Fatalf("Read = %q, %v, %v; want %q, true, nil", got, ok, err, "bar")
	}
	_, ok, err = c.Read("A", "missing")
	if err != nil || ok {
		t.Fatalf("missing key ok=%v err=%v; want false, nil", ok, err)
	}
}

func TestReadFromStaleFollowerFails(t *testing.T) {
	c := New("A", "B", "C")
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	c.Node("C").Down()
	if err := c.Put("foo", "baz"); err != nil {
		t.Fatal(err)
	}
	c.Node("C").Up()

	local, ok := c.Node("C").Get("foo")
	if !ok || local != "bar" {
		t.Fatalf("C local Get = %q, %v; want %q, true", local, ok, "bar")
	}
	if _, _, err := c.Read("C", "foo"); err == nil {
		t.Fatal("Read on a follower succeeded")
	}
	got, ok, err := c.Read("A", "foo")
	if err != nil || !ok || got != "baz" {
		t.Fatalf("Read(A) = %q, %v, %v; want %q, true, nil", got, ok, err, "baz")
	}
}

func TestReadFromIsolatedLeaderFails(t *testing.T) {
	c := New("A", "B", "C")
	if err := c.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	c.Isolate("A")
	if err := c.Elect("B"); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("foo", "baz"); err != nil {
		t.Fatal(err)
	}

	local, ok := c.Node("A").Get("foo")
	if !ok || local != "bar" {
		t.Fatalf("A local Get = %q, %v; want %q, true", local, ok, "bar")
	}
	if _, _, err := c.Read("A", "foo"); err == nil {
		t.Fatal("isolated leader served a read")
	}
	got, ok, err := c.Read("B", "foo")
	if err != nil || !ok || got != "baz" {
		t.Fatalf("Read(B) = %q, %v, %v; want %q, true, nil", got, ok, err, "baz")
	}
}
