package api

import "time"

// ObjectMeta is the identity stored on every API object.
// JSON names are the Kubernetes ones: name, namespace, uid,
// resourceVersion, labels, ownerReferences. The object field is metadata.
type ObjectMeta struct {
	Name            string            `json:"name"`
	Namespace       string            `json:"namespace"`
	UID             string            `json:"uid"`
	ResourceVersion uint64            `json:"resourceVersion"`
	Labels          map[string]string `json:"labels"`
	OwnerReferences []OwnerReference  `json:"ownerReferences"`
}

// DefaultNamespace sets Namespace for an object before it is stored.
// A namespaced object with an empty namespace becomes "default".
// A cluster-scoped object, such as a Node, keeps an empty namespace.
func (m *ObjectMeta) DefaultNamespace(namespaced bool) {
	// TODO: if !namespaced, set m.Namespace to "".
	// If namespaced and m.Namespace is empty, set m.Namespace to "default".
	// Leave a namespace that was already set unchanged.
	if !namespaced {
		m.Namespace = ""
	} else if m.Namespace == "" {
		m.Namespace = "default"
	}
}

type OwnerReference struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
	UID  string `json:"uid"`
}

type Container struct {
	Name  string `json:"name"`
	Image string `json:"image"`
}

type Pod struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     PodSpec    `json:"spec"`
	Status   PodStatus  `json:"status"`
}

type PodSpec struct {
	NodeName   string      `json:"nodeName"`
	Containers []Container `json:"containers"`
}

type PodStatus struct {
	Phase string `json:"phase"`
	PodIP string `json:"podIP"`
}

type Node struct {
	Metadata ObjectMeta `json:"metadata"`
	Status   NodeStatus `json:"status"`
}

type NodeStatus struct {
	Ready         bool      `json:"ready"`
	LastHeartbeat time.Time `json:"lastHeartbeatTime"`
}

// Lease is the heartbeat record for one node. It is cluster-scoped.
type Lease struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     LeaseSpec  `json:"spec"`
}

// LeaderLease is the scheduler lock. It is stored at
// /registry/leases/leaders/{name}. FencingToken is the same rule as a lock:
// a stale token cannot change a Pod.
type LeaderLease struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     LeaseSpec  `json:"spec"`
}

type LeaseSpec struct {
	HolderIdentity       string    `json:"holderIdentity"`
	RenewTime            time.Time `json:"renewTime"`
	LeaseDurationSeconds int       `json:"leaseDurationSeconds"`
	FencingToken         uint64    `json:"fencingToken"`
}

type ReplicaSet struct {
	Metadata ObjectMeta       `json:"metadata"`
	Spec     ReplicaSetSpec   `json:"spec"`
	Status   ReplicaSetStatus `json:"status"`
}

type ReplicaSetSpec struct {
	Replicas int               `json:"replicas"`
	Selector map[string]string `json:"selector"`
}

type ReplicaSetStatus struct {
	Replicas      int `json:"replicas"`
	ReadyReplicas int `json:"readyReplicas"`
}

// MatchLabels reports whether labels contain every key and value in selector.
// An empty selector matches nothing.
func MatchLabels(labels, selector map[string]string) bool {
	// TODO: return false when len(selector) == 0.
	// Otherwise every selector key must be present in labels with the same value.
	// Extra keys in labels are fine.
	if len(selector) == 0 {
		return false
	}
	for key, value := range selector {
		if labels[key] != value {
			return false
		}
	}
	return true
}

// Service selects Pods and names the port they serve.
type Service struct {
	Metadata ObjectMeta  `json:"metadata"`
	Spec     ServiceSpec `json:"spec"`
}

type ServiceSpec struct {
	Selector map[string]string `json:"selector"`
	Port     int               `json:"port"`
}

// EndpointAddress is one backend, podIP:port.
type EndpointAddress struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

// Endpoints is the set of Running Pods for one Service.
// It has the Service's name and namespace.
type Endpoints struct {
	Metadata  ObjectMeta        `json:"metadata"`
	Addresses []EndpointAddress `json:"addresses"`
}
