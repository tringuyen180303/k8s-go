# From this store to Kubernetes

The code in this repo is the etcd half of a cluster. Kubernetes does not replace it. The control plane stores every object in it, watches it, and writes the next object back.

Build in the order below. Each step is one package, one test file, and one behavior you can run before the next step. Do not start the scheduler before a Pod round-trips through the API server. Do not start a ReplicaSet before one Pod is bound and reported Running.

## What you already have

| Piece | Where | What Kubernetes calls it |
| --- | --- | --- |
| Put, Get, Delete, revision | `internal/kv/store.go` | etcd key, mod revision |
| History, `GetAt` | `internal/kv/store.go` | MVCC, resourceVersion |
| `Watch` | `internal/kv/store.go` | watch, the input to every controller |
| WAL replay | `internal/wal/wal.go` | etcd write-ahead log |
| Snapshot | `internal/kv/snapshot.go` | etcd snapshot and log compaction. `SaveSnapshot` is still a stub. |
| Majority commit, election, linearizable read | `internal/raft` | the etcd cluster the API server writes to |
| `Grant`, `KeepAlive`, `Expire` | `internal/kv/lease.go` | node heartbeats and leader election TTL |
| `Acquire`, fencing token, `Mutate` | `internal/kv/lock.go` | one scheduler, one controller manager |

`internal/cluster` is the split-brain version. Leave it as the counterexample. The API server writes through `internal/raft`.

## The loop you are building

```
kubectl
  |  POST /api/v1/namespaces/default/pods
  v
apiserver ---- Put JSON at /registry/pods/default/nginx ----> raft leader
  ^                                                              |
  |                         watch                                v
scheduler -------- pods with empty spec.nodeName -------- kv.Store
  |  PUT spec.nodeName
  v
kubelet ---------- pods bound to this node -------------- store
  |  PUT status.phase = Running
  v
replicaset controller -- watch own pods, create or delete to match .spec.replicas
```

A ReplicaSet of 2, with two kubelets, is the end-to-end demo:

1. The client creates the ReplicaSet.
2. The controller creates two Pods.
3. The scheduler binds each Pod to a different Node.
4. Each kubelet sets the Pod phase to Running.
5. Killing one kubelet expires its node lease. The node controller marks that Node NotReady. The pod controller does not count those pods. The ReplicaSet creates a replacement on the live node.

## What stays out of the first demo

Authentication, RBAC, admission webhooks, CRDs, CNI, containerd, iptables kube-proxy, Deployments, volumes, and ingress. Pods "run" by a status transition in the kubelet, not by starting a container. Add a process runtime only after the status loop is green.

## Key layout

One etcd key per object. The value is JSON.

```
/registry/pods/{namespace}/{name}
/registry/nodes/{name}
/registry/replicasets/{namespace}/{name}
/registry/services/{namespace}/{name}
/registry/endpoints/{namespace}/{name}
/registry/leases/nodes/{name}
/registry/leases/leaders/scheduler
/registry/leases/leaders/controller-manager
```

`resourceVersion` on the object is `Store.Revision()` after the write that stored it. A watcher resumes from that number. An update sends the version it read. The registry rejects the write when the stored version is newer. That conflict is HTTP 409.

## Step 0. Finish the store the API server needs

Files: `internal/kv/snapshot.go`, `internal/kv/store.go`.

Three holes block every later step.

**Snapshot.** Implement `SaveSnapshot` and `applySnapshot`. `TestSnapshotRestoresState` is already written. A long-running API server compacts the WAL. Without this, replay time grows with every Pod update.

**Prefix list.** Add `List(prefix string) []KV`. `List("/registry/pods/default/")` returns every Pod in that namespace. Keys are the full path. This is etcd Range.

**Prefix watch from a revision.** `Watch` today is one key and only future writes. Add:

```go
func (s *Store) WatchPrefix(ctx context.Context, prefix string, fromRev uint64) <-chan Event
```

On register, walk `history` and send every version with `revision > fromRev` whose key has that prefix. Then send live events the same way `notify` does. A controller that restarts lists, takes the list's revision, and watches from there. It must not miss the write that happened between the list and the watch.

**Compare and swap.** Add `Update(key, value string, expectRev uint64) (uint64, error)`. Read the latest history entry for `key`. If its revision is not `expectRev`, return an error and do not append the WAL. On match, Put and return the new revision. Create uses expectRev 0 and fails when the key already exists.

Leases and fencing tokens live in memory only. That is fine until step 5. Node heartbeats in the demo are keys under `/registry/leases/...`, written with `Put`, and expired by a controller. You do not have to make `Grant` durable.

Done when: snapshot tests pass, a list returns three keys under one prefix and ignores another prefix, a watch from revision 2 replays revision 3 and then a later live put, and `Update` with a stale revision leaves the value unchanged.

## Step 1. API objects

Package: `internal/api`.

```go
type ObjectMeta struct {
    Name            string
    Namespace       string
    UID             string
    ResourceVersion uint64
    Labels          map[string]string
    OwnerReferences []OwnerReference
}

type OwnerReference struct {
    Kind string
    Name string
    UID  string
}

type Pod struct {
    Metadata ObjectMeta
    Spec     PodSpec
    Status   PodStatus
}

type PodSpec struct {
    NodeName   string
    Containers []Container // Name, Image. Image is recorded, not pulled.
}

type PodStatus struct {
    Phase string // Pending, Running, Succeeded, Failed
}

type Node struct {
    Metadata ObjectMeta
    Status   NodeStatus // Ready bool, plus a last-heartbeat time
}

type ReplicaSet struct {
    Metadata ObjectMeta
    Spec     ReplicaSetSpec // Replicas int, Selector map[string]string
    Status   ReplicaSetStatus // Replicas, ReadyReplicas
}
```

`Namespace` defaults to `default` for namespaced objects. Nodes have an empty namespace.

Add `MatchLabels(labels, selector map[string]string) bool`. An empty selector matches nothing. The ReplicaSet controller and the scheduler both use this.

Done when: a test encodes a Pod to JSON and decodes the same NodeName, labels, and resourceVersion.

## Step 2. Registry

Package: `internal/registry`.

The registry is the only package that knows etcd key paths. Controllers never call `kv.Store` directly.

```go
type Store interface {
    Create(ctx context.Context, obj api.Object) error
    Get(ctx context.Context, kind, namespace, name string) (api.Object, error)
    Update(ctx context.Context, obj api.Object) error
    Delete(ctx context.Context, kind, namespace, name string) error
    List(ctx context.Context, kind, namespace string) ([]api.Object, uint64, error)
    Watch(ctx context.Context, kind, namespace string, fromRev uint64) (<-chan Event, error)
}
```

`Event` is `Added`, `Modified`, or `Deleted`, plus the decoded object.

Behavior:

- `Create` allocates a UID, sets resourceVersion from `Update(..., 0)`, and fails if the key exists.
- `Update` sends `obj.Metadata.ResourceVersion` as `expectRev`. On conflict return `ErrConflict`.
- `List` uses the prefix for that kind and namespace and returns the store revision along with the objects. The revision is what the watcher passes as `fromRev`.
- `Watch` calls `WatchPrefix`. Decode each `kv.Event` into an `api` object. A deleted history entry is `Deleted`.

Status writes go through `UpdateStatus`. It loads the current object, replaces only `Status`, and CAS-writes the result. The kubelet updates phase without wiping `spec.nodeName`. The scheduler sets `spec.nodeName` without wiping a phase the kubelet already wrote. Both still lose on a stale resourceVersion and retry.

Done when: two updates of the same Pod, the second started from the original resourceVersion, and only one succeeds. A watch started after create receives the next update and the delete.

## Step 3. API server

Package: `internal/apiserver`. Command: `cmd/apiserver`.

One process. On start it opens the durable store (`kv.Open`) and serves HTTP. Routes:

```
POST   /api/v1/namespaces/{ns}/pods
GET    /api/v1/namespaces/{ns}/pods
GET    /api/v1/namespaces/{ns}/pods/{name}
PUT    /api/v1/namespaces/{ns}/pods/{name}
PUT    /api/v1/namespaces/{ns}/pods/{name}/status
DELETE /api/v1/namespaces/{ns}/pods/{name}
GET    /api/v1/namespaces/{ns}/pods?watch=1&resourceVersion={rev}
```

Same shape for `replicasets` and `services`. Nodes are cluster-scoped:

```
POST /api/v1/nodes
GET  /api/v1/nodes
GET  /api/v1/nodes/{name}
PUT  /api/v1/nodes/{name}/status
```

Watch is a long-lived response. Write one JSON event per line and flush. The client reads until the response body closes.

Errors: missing object is 404, CAS conflict is 409, bad JSON is 400.

Keep `cmd/kv` as the raw key server. The API server is a second binary.

Done when: `httptest` creates a Pod, lists it, watches an update, and a second PUT with the old resourceVersion returns 409.

## Step 4. Client, reflector, work queue

Packages: `internal/client`, `internal/cache`, `internal/workqueue`.

The client is the HTTP calls from step 3: `Create`, `Get`, `Update`, `UpdateStatus`, `Delete`, `List`, `Watch`.

The reflector is the loop every controller shares:

1. `List`. Remember the returned resourceVersion.
2. `Watch` from that version.
3. Push each event onto a `DeltaFIFO` keyed by namespace and name.
4. On a watch error, list again from scratch.

The informer pops the FIFO and calls `Add`, `Update`, or `Delete` on a handler. The handler only enqueues a key. Reconcile does the read.

```go
type Queue interface {
    Add(key string)
    Get() (key string, shutdown bool)
    Done(key string)
}
```

`Get` blocks. `Add` of a key already queued is a no-op. `Done` lets that key be queued again.

Done when: a reflector fed a list of one Pod and a watch update delivers both to the handler, in order, and a disconnected watch lists again instead of stalling.

## Step 5. Nodes and heartbeats

Packages: `internal/node`, command later in `cmd/kubelet`.

A kubelet registers itself:

1. `Create` a Node named after the process flag (`node-a`).
2. Every 10 seconds, `UpdateStatus` with `Ready: true` and the current time.
3. Write a Lease object at `/registry/leases/nodes/node-a` with the same timestamp, through the registry. TTL is 30 seconds. This is a stored object, not `kv.Grant`, so it survives an API server restart.

The node controller (`internal/controller/node`):

1. Watch Nodes and Leases.
2. On each pass, if the lease timestamp is older than 30 seconds, `UpdateStatus` with `Ready: false`.

Use a fake clock in tests, the same way `lease_test.go` passes `now time.Time`. Do not sleep.

Done when: a node whose lease is refreshed at t+20s stays Ready at t+40s, and a node whose lease stops at t+0 is NotReady at t+31s.

## Step 6. Scheduler

Package: `internal/scheduler`. Command: `cmd/scheduler`.

Leader election first, using the lock you have. Only the process that holds `/registry/leases/leaders/scheduler` runs the loop. A second scheduler watches and waits. Model the lock as a Lease object plus a fencing token stored in its value, with the same rule as `Mutate`: a stale token cannot bind.

One reconcile of a Pod key:

1. Get the Pod. If it has `spec.nodeName`, return.
2. List Nodes. Keep the ones with `Ready: true`.
3. Skip a Node that already has a Pod from this pass's list when another Ready node is free. This is the only scoring rule. It spreads Pods.
4. Set `spec.nodeName` and `Update`. On 409, requeue. The retry reads the fresh object.
5. Leave `status.phase` as Pending. The kubelet is the only writer of phase.

The scheduler watches Pods. A new Pod with an empty nodeName is the only event that enqueues work.

Done when: three Ready nodes and three Pods end with three different node names. One NotReady node never receives a Pod. Two schedulers and one lock produce a single bind.

## Step 7. Kubelet

Package: `internal/kubelet`. Command: `cmd/kubelet`.

Flags: `--name=node-a`, `--apiserver=http://127.0.0.1:8080`.

Loop:

1. Register the Node and start the heartbeat from step 5.
2. Watch Pods. Handle a Pod only when `spec.nodeName` equals this kubelet's name.
3. Desired phase is Running. `UpdateStatus` to Running. On 409, requeue and read again.
4. A deleted Pod is acknowledged by returning. There is no local process to stop yet.

The runtime interface is one method so a later process runtime can replace the status-only one:

```go
type Runtime interface {
    Ensure(ctx context.Context, pod *api.Pod) (phase string, err error)
    Stop(ctx context.Context, pod *api.Pod) error
}
```

`StatusRuntime.Ensure` returns `Running`.

Done when: a Pod bound to `node-a` becomes Running after the kubelet reconcile, and a Pod bound to `node-b` is unchanged.

## Step 8. ReplicaSet controller

Package: `internal/controller/replicaset`. Command: `cmd/controller-manager`.

The controller manager holds `/registry/leases/leaders/controller-manager` the same way the scheduler holds its key. It runs the node controller and the ReplicaSet controller.

Reconcile one ReplicaSet:

1. Get the ReplicaSet. List Pods in its namespace.
2. Keep Pods whose labels match `spec.selector` and whose `ownerReferences` contain this ReplicaSet UID.
3. Count Pods whose node is Ready or whose `spec.nodeName` is still empty. A Pod on a NotReady node is not counted. Delete is not required for the first version. Leaving it avoids killing a Pod during a short lease gap. The replacement is an extra Pod.
4. If the count is below `spec.replicas`, `Create` Pods named `{rs}-{n}` with the selector copied onto labels and an ownerReference. `n` increases until the name is free.
5. If the count is above `spec.replicas`, `Delete` extras. Delete Pods that are not Running first.
6. `UpdateStatus` with the counts.

Pod names and the owner UID make a re-run safe. Creating the same name twice returns a conflict, and the next reconcile lists again.

Done when: a ReplicaSet with replicas 2 creates two Pods. A second reconcile creates nothing. Raising replicas to 3 creates one more. Dropping replicas to 1 deletes down to one. Marking a bound Pod's node NotReady makes the next reconcile create a replacement.

## Step 9. Wire the demo

Command: `cmd/cluster`.

One process starts, in order:

1. A `raft.Cluster` of three nodes is the wrong shape for this step. It is an in-memory simulation, and the API server needs `*kv.Store`. Start one `kv.Open` directory. Point the API server at it. Move the API server onto Raft only in step 10.
2. API server on `:8080`.
3. Scheduler.
4. Controller manager.
5. Kubelets `node-a` and `node-b`.

A test in `cmd/cluster` drives the client:

1. Wait until both Nodes are Ready.
2. Create a ReplicaSet, replicas 2, selector `app=web`.
3. Poll until two Pods are Running and their node names differ.
4. Set replicas to 0. Poll until the list is empty.

Use a context timeout of a few seconds. The test fails on the timeout. It does not sleep a fixed duration and hope.

Add `cmd/kubectl` with `apply -f pod.json` and `get pods` so the same flow runs by hand. `apply` is Create, and on 409 it is Get plus Update of spec.

## Step 10. Run the API server on the Raft cluster

`internal/raft` commits into an in-memory `kv.Store` and forgets the log on restart. The API server needs the durable store and a single writer.

Changes, in this order:

1. Give each `raft.Node` a `kv.Open` path. Commit calls `store.Put` on that durable store, which you already do. Restart calls the equivalent of `Sync`: replay the leader log into a follower that was down, then the follower's WAL has the records.
2. Serve the registry on the leader only. `Cluster.Read` is the linearizable read. A follower returns an error. The API server uses that error as HTTP 503.
3. `Elect` after the leader is `Down`. The new leader's store must contain every committed key before it serves. That is `Sync` first, then accept HTTP.
4. Replace the single `kv.Open` in `cmd/cluster` with this cluster. The step 9 test still passes with one node stopped.

Keep the Raft implementation in-process. Real TCP between nodes is a later change. The lesson here is that the API server, scheduler, and kubelets keep working when the store fails over, because they talk to the registry and retry 503 and 409.

## Step 11. Service and endpoints

After the demo is green.

`Service` has a selector and a port. The endpoints controller lists Pods that match the selector, keeps those with phase Running, and writes an `Endpoints` object of `podIP:port`. Give each Running Pod an address of `10.0.0.{n}` from the kubelet. No packets move yet.

A userspace proxy is the step after that: watch Endpoints, listen on the Service port, and forward to one address. That is the first data-plane piece. It is not required for the control-plane demo.

## Later, each as its own step

| Step | Adds |
| --- | --- |
| Process runtime | `Runtime.Ensure` starts the container command as a child process. `Stop` kills it. Status becomes Failed when the process exits non-zero. |
| Deployment | A controller that owns ReplicaSets, one per template hash. Changing the template creates a new ReplicaSet and scales the old one down. |
| Garbage collection | A controller that deletes objects whose ownerReference UID no longer exists. ReplicaSet delete then takes its Pods with it. |
| Auth | One bearer token on the request. The API server rejects a missing token before it touches the registry. |
| Network Raft | Each `raft.Node` is a process. AppendEntries and RequestVote are HTTP. The store code stays. |

## Suggested order of work

| Order | Package | Proves |
| --- | --- | --- |
| 1 | `internal/kv` snapshot, `List`, `WatchPrefix`, `Update` | etcd range, watch resume, resourceVersion |
| 2 | `internal/api` | Pod, Node, ReplicaSet JSON |
| 3 | `internal/registry` | key paths and 409 |
| 4 | `internal/apiserver`, `cmd/apiserver` | HTTP create, list, watch, status |
| 5 | `internal/client`, `internal/cache`, `internal/workqueue` | list plus watch restart |
| 6 | `internal/controller/node` | NotReady from an expired lease |
| 7 | `internal/scheduler`, `cmd/scheduler` | one bind, spread, single leader |
| 8 | `internal/kubelet`, `cmd/kubelet` | bound Pod becomes Running |
| 9 | `internal/controller/replicaset`, `cmd/controller-manager` | replicas converge |
| 10 | `cmd/cluster`, `cmd/kubectl` | the demo above |
| 11 | `internal/raft` durable leader failover | API server survives a leader loss |
| 12 | endpoints, then the proxy | a Service has backends |
