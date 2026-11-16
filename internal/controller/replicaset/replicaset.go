package replicaset

import (
	"context"
	"errors"
	"fmt"

	"k8s-go/internal/api"
	"k8s-go/internal/registry"
)

// Reconcile makes the Pods for one ReplicaSet match spec.replicas.
// The caller holds the LeaderLease named "controller-manager".
func Reconcile(ctx context.Context, store registry.Store, namespace, name string) error {
	obj, err := store.Get(ctx, "ReplicaSet", namespace, name)
	if err != nil {
		return err
	}
	rs := obj.(*api.ReplicaSet)

	podObjs, _, err := store.List(ctx, "Pod", namespace)
	if err != nil {
		return err
	}
	nodeObjs, _, err := store.List(ctx, "Node", "")
	if err != nil {
		return err
	}
	ready := map[string]bool{}
	for _, item := range nodeObjs {
		node := item.(*api.Node)
		ready[node.Metadata.Name] = node.Status.Ready
	}

	var owned []*api.Pod
	for _, item := range podObjs {
		pod := item.(*api.Pod)
		if api.MatchLabels(pod.Metadata.Labels, rs.Spec.Selector) && podOwnedBy(pod, rs.Metadata.UID) {
			owned = append(owned, pod)
		}
	}

	counted := make([]*api.Pod, 0, len(owned))
	for _, pod := range owned {
		if pod.Spec.NodeName == "" || ready[pod.Spec.NodeName] {
			counted = append(counted, pod)
		}
	}

	n := 1
	for len(counted) < rs.Spec.Replicas {
		pod := &api.Pod{
			Metadata: api.ObjectMeta{
				Name:      fmt.Sprintf("%s-%d", rs.Metadata.Name, n),
				Namespace: rs.Metadata.Namespace,
				Labels:    copyLabels(rs.Spec.Selector),
				OwnerReferences: []api.OwnerReference{{
					Kind: "ReplicaSet",
					Name: rs.Metadata.Name,
					UID:  rs.Metadata.UID,
				}},
			},
		}
		err := store.Create(ctx, pod)
		if errors.Is(err, registry.ErrConflict) {
			n++
			continue
		}
		if err != nil {
			return err
		}
		counted = append(counted, pod)
		owned = append(owned, pod)
		n++
	}

	for len(counted) > rs.Spec.Replicas {
		idx := victim(counted)
		pod := counted[idx]
		if err := store.Delete(ctx, "Pod", pod.Metadata.Namespace, pod.Metadata.Name); err != nil {
			return err
		}
		counted = append(counted[:idx], counted[idx+1:]...)
		owned = withoutPod(owned, pod.Metadata.Name)
	}

	readyReplicas := 0
	for _, pod := range owned {
		if pod.Status.Phase == "Running" {
			readyReplicas++
		}
	}
	rs.Status.Replicas = len(owned)
	rs.Status.ReadyReplicas = readyReplicas
	return store.UpdateStatus(ctx, rs)
}

func podOwnedBy(pod *api.Pod, uid string) bool {
	for _, ref := range pod.Metadata.OwnerReferences {
		if ref.Kind == "ReplicaSet" && ref.UID == uid {
			return true
		}
	}
	return false
}

func copyLabels(selector map[string]string) map[string]string {
	labels := make(map[string]string, len(selector))
	for key, value := range selector {
		labels[key] = value
	}
	return labels
}

func victim(pods []*api.Pod) int {
	for i, pod := range pods {
		if pod.Status.Phase != "Running" {
			return i
		}
	}
	return 0
}

func withoutPod(pods []*api.Pod, name string) []*api.Pod {
	out := make([]*api.Pod, 0, len(pods))
	for _, pod := range pods {
		if pod.Metadata.Name != name {
			out = append(out, pod)
		}
	}
	return out
}
