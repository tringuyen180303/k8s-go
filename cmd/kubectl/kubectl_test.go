package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"k8s-go/internal/api"
	"k8s-go/internal/apiserver"
	"k8s-go/internal/client"
	"k8s-go/internal/kv"
	"k8s-go/internal/registry"
)

func TestApplyCreatesThenUpdatesSpec(t *testing.T) {
	srv := httptest.NewServer(apiserver.New(registry.New(kv.New())))
	t.Cleanup(srv.Close)
	c := client.New(srv.URL)
	ctx := context.Background()
	dir := t.TempDir()

	path := writePod(t, dir, "nginx")
	if err := Apply(ctx, c, path); err != nil {
		t.Fatal(err)
	}
	pods, err := GetPods(ctx, c, "default")
	if err != nil {
		t.Fatal(err)
	}
	if len(pods) != 1 || pods[0].Spec.Containers[0].Image != "nginx" {
		t.Fatalf("pods = %+v; want one nginx pod", pods)
	}

	writePod(t, dir, "nginx:2")
	if err := Apply(ctx, c, path); err != nil {
		t.Fatal(err)
	}
	pods, err = GetPods(ctx, c, "default")
	if err != nil {
		t.Fatal(err)
	}
	if len(pods) != 1 || pods[0].Metadata.Name != "web" || pods[0].Spec.Containers[0].Image != "nginx:2" {
		t.Fatalf("pods = %+v; want web updated to nginx:2", pods)
	}
}

func writePod(t *testing.T, dir, image string) string {
	t.Helper()
	pod := &api.Pod{
		Metadata: api.ObjectMeta{Name: "web", Namespace: "default"},
		Spec:     api.PodSpec{Containers: []api.Container{{Name: "web", Image: image}}},
	}
	body, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "pod.json")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
