package endpoints

import (
	"context"
	"errors"

	"k8s-go/internal/api"
	"k8s-go/internal/registry"
)

// Reconcile writes the Endpoints for one Service.
// The Endpoints object uses the Service name and namespace.
// An address is included when the Pod labels match spec.selector, the phase
// is Running, and status.podIP is set. The port is spec.port.
// An empty selector matches nothing. A second call with the same Pods
// leaves the addresses unchanged.
func Reconcile(ctx context.Context, store registry.Store, namespace, name string) error {
	obj, err := store.Get(ctx, "Service", namespace, name)
	if err != nil {
		return err
	}
	svc := obj.(*api.Service)

	pods, _, err := store.List(ctx, "Pod", namespace)
	if err != nil {
		return err
	}
	var addresses []api.EndpointAddress
	for _, item := range pods {
		pod := item.(*api.Pod)
		if pod.Status.Phase != "Running" || pod.Status.PodIP == "" {
			continue
		}
		if !api.MatchLabels(pod.Metadata.Labels, svc.Spec.Selector) {
			continue
		}
		addresses = append(addresses, api.EndpointAddress{
			IP:   pod.Status.PodIP,
			Port: svc.Spec.Port,
		})
	}

	current, err := store.Get(ctx, "Endpoints", namespace, name)
	if errors.Is(err, registry.ErrNotFound) {
		return store.Create(ctx, &api.Endpoints{
			Metadata:  api.ObjectMeta{Name: name, Namespace: namespace},
			Addresses: addresses,
		})
	}
	if err != nil {
		return err
	}
	ep := current.(*api.Endpoints)
	ep.Addresses = addresses
	return store.Update(ctx, ep)
}
