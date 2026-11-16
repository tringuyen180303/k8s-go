package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	"k8s-go/internal/api"
	"k8s-go/internal/client"
	"k8s-go/internal/registry"
)

// Apply reads a Pod JSON file. Create it.
// On registry.ErrConflict, Get the Pod and Update its spec from the file.
// Keep the resourceVersion returned by Get.
func Apply(ctx context.Context, c *client.Client, filename string) error {
	body, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	pod := &api.Pod{}
	if err := json.Unmarshal(body, pod); err != nil {
		return err
	}
	err = c.Create(ctx, pod)
	if !errors.Is(err, registry.ErrConflict) {
		return err
	}
	current, err := c.Get(ctx, "Pod", pod.Metadata.Namespace, pod.Metadata.Name)
	if err != nil {
		return err
	}
	got := current.(*api.Pod)
	got.Spec = pod.Spec
	return c.Update(ctx, got)
}

// GetPods lists Pods in namespace.
func GetPods(ctx context.Context, c *client.Client, namespace string) ([]*api.Pod, error) {
	items, _, err := c.List(ctx, "Pod", namespace)
	if err != nil {
		return nil, err
	}
	pods := make([]*api.Pod, len(items))
	for i, item := range items {
		pods[i] = item.(*api.Pod)
	}
	return pods, nil
}
