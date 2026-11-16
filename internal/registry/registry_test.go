package registry

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s-go/internal/api"
	"k8s-go/internal/kv"
)

func TestCreateGetAndConflict(t *testing.T) {
	r := New(kv.New())
	ctx := context.Background()
	pod := &api.Pod{
		Metadata: api.ObjectMeta{Name: "nginx", Labels: map[string]string{"app": "web"}},
		Spec:     api.PodSpec{Containers: []api.Container{{Name: "nginx", Image: "nginx:1.25"}}},
	}
	if err := r.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if pod.Metadata.UID == "" || pod.Metadata.Namespace != "default" || pod.Metadata.ResourceVersion == 0 {
		t.Fatalf("metadata after create = %+v", pod.Metadata)
	}
	first := pod.Metadata.ResourceVersion

	got, err := r.Get(ctx, "Pod", "default", "nginx")
	if err != nil {
		t.Fatal(err)
	}
	stored, ok := got.(*api.Pod)
	if !ok || stored.Spec.Containers[0].Image != "nginx:1.25" || stored.Metadata.ResourceVersion != first {
		t.Fatalf("Get = %+v", got)
	}

	if err := r.Create(ctx, &api.Pod{Metadata: api.ObjectMeta{Name: "nginx"}}); err == nil {
		t.Fatal("second create succeeded")
	}

	pod.Spec.NodeName = "node-a"
	if err := r.Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if pod.Metadata.ResourceVersion == first {
		t.Fatal("resourceVersion did not advance")
	}

	stale := &api.Pod{Metadata: api.ObjectMeta{Name: "nginx", Namespace: "default", ResourceVersion: first}}
	stale.Spec.NodeName = "node-b"
	if err := r.Update(ctx, stale); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale Update err = %v; want ErrConflict", err)
	}
	got, err = r.Get(ctx, "Pod", "default", "nginx")
	if err != nil {
		t.Fatal(err)
	}
	if got.(*api.Pod).Spec.NodeName != "node-a" {
		t.Fatalf("nodeName = %q; want node-a", got.(*api.Pod).Spec.NodeName)
	}
}

func TestUpdateStatusKeepsSpec(t *testing.T) {
	r := New(kv.New())
	ctx := context.Background()
	pod := &api.Pod{
		Metadata: api.ObjectMeta{Name: "nginx"},
		Spec:     api.PodSpec{NodeName: "node-a"},
		Status:   api.PodStatus{Phase: "Pending"},
	}
	if err := r.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	first := pod.Metadata.ResourceVersion

	status := &api.Pod{
		Metadata: api.ObjectMeta{Name: "nginx", Namespace: "default", ResourceVersion: first},
		Status:   api.PodStatus{Phase: "Running"},
	}
	if err := r.UpdateStatus(ctx, status); err != nil {
		t.Fatal(err)
	}
	got, err := r.Get(ctx, "Pod", "default", "nginx")
	if err != nil {
		t.Fatal(err)
	}
	stored := got.(*api.Pod)
	if stored.Spec.NodeName != "node-a" || stored.Status.Phase != "Running" {
		t.Fatalf("after status = spec %q phase %q", stored.Spec.NodeName, stored.Status.Phase)
	}

	stale := &api.Pod{
		Metadata: api.ObjectMeta{Name: "nginx", Namespace: "default", ResourceVersion: first},
		Status:   api.PodStatus{Phase: "Failed"},
	}
	if err := r.UpdateStatus(ctx, stale); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale status err = %v; want ErrConflict", err)
	}
	got, err = r.Get(ctx, "Pod", "default", "nginx")
	if err != nil {
		t.Fatal(err)
	}
	if got.(*api.Pod).Status.Phase != "Running" {
		t.Fatalf("phase = %q; want Running", got.(*api.Pod).Status.Phase)
	}
}

func TestListPodsInNamespace(t *testing.T) {
	r := New(kv.New())
	ctx := context.Background()
	for _, pod := range []*api.Pod{
		{Metadata: api.ObjectMeta{Name: "a"}},
		{Metadata: api.ObjectMeta{Name: "b"}},
		{Metadata: api.ObjectMeta{Name: "c", Namespace: "kube-system"}},
	} {
		if err := r.Create(ctx, pod); err != nil {
			t.Fatal(err)
		}
	}
	items, rev, err := r.List(ctx, "Pod", "default")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || rev == 0 {
		t.Fatalf("List = %d items, rev %d; want 2 and a revision", len(items), rev)
	}
}

func TestWatchUpdateAndDelete(t *testing.T) {
	r := New(kv.New())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	pod := &api.Pod{Metadata: api.ObjectMeta{Name: "nginx"}}
	if err := r.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	ch, err := r.Watch(ctx, "Pod", "default", pod.Metadata.ResourceVersion)
	if err != nil {
		t.Fatal(err)
	}

	pod.Spec.NodeName = "node-a"
	if err := r.Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	ev := waitEvent(t, ch)
	if ev.Type != Modified || ev.Object.(*api.Pod).Spec.NodeName != "node-a" {
		t.Fatalf("watch update = %+v", ev)
	}

	if err := r.Delete(ctx, "Pod", "default", "nginx"); err != nil {
		t.Fatal(err)
	}
	ev = waitEvent(t, ch)
	if ev.Type != Deleted || ev.Object.GetObjectMeta().Name != "nginx" {
		t.Fatalf("watch delete = %+v name %q", ev.Type, ev.Object.GetObjectMeta().Name)
	}
	if _, err := r.Get(ctx, "Pod", "default", "nginx"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete err = %v; want ErrNotFound", err)
	}
}

func waitEvent(t *testing.T, ch <-chan Event) Event {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed")
		}
		return ev
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for a watch event")
	}
	return Event{}
}
