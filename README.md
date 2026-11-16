# k8s-go

A Kubernetes control plane written in Go, backed by a small etcd. The store, scheduler, kubelet, and controllers run in this repo. Pods become `Running` by a status update. No container is started, and no packet is forwarded.

`internal/kv` is the etcd. The API server does not dial an external etcd process. Every object is JSON in that store, and `resourceVersion` is the store revision. A ReplicaSet named `web` lives at `/registry/replicasets/default/web`. A Pod lives at `/registry/pods/default/web-1`.

## The loop

`cmd/cluster` starts one process:

1. The API server stores objects in the key-value store.
2. The scheduler binds a Pod with an empty `spec.nodeName` to a Ready node.
3. The controller manager keeps a ReplicaSet at `spec.replicas` and marks a node NotReady 30 seconds after its lease stops.
4. Kubelets `node-a` and `node-b` heartbeat and set `status.phase` to `Running` for Pods bound to them. A Running Pod gets an address `10.0.0.n`.

`RunRaft` in the same package uses three stores instead of one. Writes go to the Raft leader, and the leader copies each key to the followers. A leader that cannot reach a majority returns unavailable.

## Layout

| Path | Role |
| --- | --- |
| `internal/kv`, `internal/wal` | etcd: WAL, revisions, prefix watch, snapshots, compare-and-swap |
| `internal/raft` | three-node cluster and the registry that writes through the leader |
| `internal/registry`, `internal/apiserver` | object keys and the HTTP API |
| `internal/scheduler` | leader lock and Pod binding |
| `internal/kubelet` | node-local Pod status and Pod IPs |
| `internal/controller/node` | NotReady when a node lease expires |
| `internal/controller/replicaset` | create and delete Pods to match `spec.replicas` |
| `internal/controller/endpoints` | Service backends from Running Pods. The cluster process does not call it yet |
| `cmd/kubectl` | `apply -f` a Pod and `get pods` |
| `docs/kubernetes.md` | the build order |

## Run

```bash
go test ./...
go run ./cmd/cluster
```

The API server listens on `:8080`. In another shell:

```bash
go run ./cmd/kubectl apply -f pod.json
go run ./cmd/kubectl get pods
```

`pod.json` is a Pod:

```json
{
  "metadata": {"name": "nginx", "namespace": "default"},
  "spec": {"containers": [{"name": "nginx", "image": "nginx:1.25"}]}
}
```

`go test ./cmd/cluster` creates a ReplicaSet of 2, waits until the Pods are Running on different nodes, then scales it to 0. `TestReplicaSetSurvivesLeaderLoss` runs that path on the Raft cluster.

## Still outside this loop

The endpoints controller is not wired into `cmd/cluster`, and there is no proxy, so a Service address does not accept a connection. Also still out: a process runtime, Deployments, garbage collection, authentication, and Raft nodes as separate processes. CNI, containerd, iptables, volumes, and Ingress are not part of this project.
