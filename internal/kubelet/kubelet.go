package kubelet

import (
	"context"
	"fmt"

	"k8s-go/internal/api"
	"k8s-go/internal/registry"
)

// Runtime turns a desired Pod into a phase. StatusRuntime does not start a process.
type Runtime interface {
	Ensure(ctx context.Context, pod *api.Pod) (phase string, err error)
	Stop(ctx context.Context, pod *api.Pod) error
}

// StatusRuntime reports Running. A later process runtime can replace it.
type StatusRuntime struct{}

func (StatusRuntime) Ensure(ctx context.Context, pod *api.Pod) (string, error) {
	return "Running", nil
}

func (StatusRuntime) Stop(ctx context.Context, pod *api.Pod) error {
	return nil
}

// Reconcile lists Pods in the default namespace and syncs each one.
func Reconcile(ctx context.Context, store registry.Store, nodeName string, rt Runtime) error {
	// TODO: store.List(ctx, "Pod", "default"). Call Sync for each *api.Pod.
	pods, _, err := store.List(ctx, "Pod", "default")
	if err != nil {
		return err
	}
	for _, pod := range pods {
		if err := Sync(ctx, store, nodeName, pod.(*api.Pod), rt); err != nil {
			return err
		}
	}
	return nil
}

// Sync updates one Pod bound to nodeName. A nil Pod is a delete and returns nil.
// Another node's Pod returns nil without a write.
// Ensure supplies the phase. UpdateStatus writes it. On ErrConflict, Get the
// Pod again and retry once. Do not change spec.nodeName.
func Sync(ctx context.Context, store registry.Store, nodeName string, pod *api.Pod, rt Runtime) error {
	// TODO:
	// 1. pod == nil returns nil.
	// 2. pod.Spec.NodeName != nodeName returns nil.
	// 3. phase, err := rt.Ensure(ctx, pod). Set pod.Status.Phase and UpdateStatus.
	// 4. registry.ErrConflict: Get the Pod by name, set the phase on that
	//    fresh object, and UpdateStatus once more.
	// 5. When phase is Running and PodIP is empty, set 10.0.0.{n}.
	//    n starts at 1 and skips an address already stored on another Pod.
	//    A second Sync leaves an existing PodIP unchanged.

	if pod == nil {
		return nil
	}
	if pod.Spec.NodeName != nodeName {
		return nil
	}
	phase, err := rt.Ensure(ctx, pod)
	if err != nil {
		return err
	}
	pod.Status.Phase = phase
	if phase == "Running" && pod.Status.PodIP == "" {
		ip, err := nextPodIP(ctx, store)
		if err != nil {
			return err
		}
		pod.Status.PodIP = ip
	}
	return store.UpdateStatus(ctx, pod)
}

func nextPodIP(ctx context.Context, store registry.Store) (string, error) {
	pods, _, err := store.List(ctx, "Pod", "default")
	if err != nil {
		return "", err
	}
	used := map[string]bool{}
	for _, item := range pods {
		used[item.(*api.Pod).Status.PodIP] = true
	}
	for n := 1; ; n++ {
		ip := fmt.Sprintf("10.0.0.%d", n)
		if !used[ip] {
			return ip, nil
		}
	}
}
