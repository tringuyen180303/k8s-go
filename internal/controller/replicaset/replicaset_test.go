package replicaset

import (
	"context"
	"testing"

	"k8s-go/internal/api"
	"k8s-go/internal/kv"
	"k8s-go/internal/registry"
)

func TestReplicaSetConverges(t *testing.T) {
	store := registry.New(kv.New())
	ctx := context.Background()
	rs := mustReplicaSet(t, store, 2)

	if err := Reconcile(ctx, store, "default", "web"); err != nil {
		t.Fatal(err)
	}
	first := ownedPods(t, store, rs)
	if len(first) != 2 {
		t.Fatalf("pods = %d; want 2", len(first))
	}
	for _, pod := range first {
		if pod.Metadata.Labels["app"] != "web" || !ownedBy(pod, rs.Metadata.UID) {
			t.Fatalf("pod = %+v", pod.Metadata)
		}
	}
	if got := getReplicaSet(t, store).Status.Replicas; got != 2 {
		t.Fatalf("status.replicas = %d; want 2", got)
	}

	if err := Reconcile(ctx, store, "default", "web"); err != nil {
		t.Fatal(err)
	}
	second := ownedPods(t, store, rs)
	if !sameNames(second, first) {
		t.Fatalf("second reconcile pods = %v; want %v", podNames(second), podNames(first))
	}

	scale(t, store, 3)
	if err := Reconcile(ctx, store, "default", "web"); err != nil {
		t.Fatal(err)
	}
	if got := len(ownedPods(t, store, rs)); got != 3 {
		t.Fatalf("pods after scale up = %d; want 3", got)
	}

	current := ownedPods(t, store, rs)
	current[0].Status.Phase = "Running"
	if err := store.UpdateStatus(ctx, current[0]); err != nil {
		t.Fatal(err)
	}
	running := current[0].Metadata.Name
	scale(t, store, 1)
	if err := Reconcile(ctx, store, "default", "web"); err != nil {
		t.Fatal(err)
	}
	left := ownedPods(t, store, rs)
	if len(left) != 1 || left[0].Metadata.Name != running {
		t.Fatalf("pods after scale down = %v; want only %s", podNames(left), running)
	}
	if got := getReplicaSet(t, store).Status.ReadyReplicas; got != 1 {
		t.Fatalf("readyReplicas = %d; want 1", got)
	}
}

func TestNotReadyPodIsReplaced(t *testing.T) {
	store := registry.New(kv.New())
	ctx := context.Background()
	if err := store.Create(ctx, &api.Node{
		Metadata: api.ObjectMeta{Name: "node-b"},
		Status:   api.NodeStatus{Ready: false},
	}); err != nil {
		t.Fatal(err)
	}
	rs := mustReplicaSet(t, store, 1)
	if err := Reconcile(ctx, store, "default", "web"); err != nil {
		t.Fatal(err)
	}
	pods := ownedPods(t, store, rs)
	if len(pods) != 1 {
		t.Fatalf("pods = %d; want 1", len(pods))
	}
	pods[0].Spec.NodeName = "node-b"
	if err := store.Update(ctx, pods[0]); err != nil {
		t.Fatal(err)
	}
	if err := Reconcile(ctx, store, "default", "web"); err != nil {
		t.Fatal(err)
	}
	if got := len(ownedPods(t, store, rs)); got != 2 {
		t.Fatalf("pods after NotReady = %d; want the original plus a replacement", got)
	}
}

func mustReplicaSet(t *testing.T, store registry.Store, replicas int) *api.ReplicaSet {
	t.Helper()
	rs := &api.ReplicaSet{
		Metadata: api.ObjectMeta{Name: "web", Namespace: "default"},
		Spec: api.ReplicaSetSpec{
			Replicas: replicas,
			Selector: map[string]string{"app": "web"},
		},
	}
	if err := store.Create(context.Background(), rs); err != nil {
		t.Fatal(err)
	}
	return rs
}

func scale(t *testing.T, store registry.Store, replicas int) {
	t.Helper()
	rs := getReplicaSet(t, store)
	rs.Spec.Replicas = replicas
	if err := store.Update(context.Background(), rs); err != nil {
		t.Fatal(err)
	}
}

func getReplicaSet(t *testing.T, store registry.Store) *api.ReplicaSet {
	t.Helper()
	obj, err := store.Get(context.Background(), "ReplicaSet", "default", "web")
	if err != nil {
		t.Fatal(err)
	}
	return obj.(*api.ReplicaSet)
}

func ownedPods(t *testing.T, store registry.Store, rs *api.ReplicaSet) []*api.Pod {
	t.Helper()
	items, _, err := store.List(context.Background(), "Pod", "default")
	if err != nil {
		t.Fatal(err)
	}
	var pods []*api.Pod
	for _, item := range items {
		pod := item.(*api.Pod)
		if ownedBy(pod, rs.Metadata.UID) {
			pods = append(pods, pod)
		}
	}
	return pods
}

func ownedBy(pod *api.Pod, uid string) bool {
	for _, ref := range pod.Metadata.OwnerReferences {
		if ref.Kind == "ReplicaSet" && ref.UID == uid {
			return true
		}
	}
	return false
}

func podNames(pods []*api.Pod) []string {
	out := make([]string, len(pods))
	for i, pod := range pods {
		out[i] = pod.Metadata.Name
	}
	return out
}

func sameNames(a, b []*api.Pod) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[string]int{}
	for _, pod := range a {
		seen[pod.Metadata.Name]++
	}
	for _, pod := range b {
		seen[pod.Metadata.Name]--
		if seen[pod.Metadata.Name] < 0 {
			return false
		}
	}
	return true
}
