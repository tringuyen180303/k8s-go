package apiserver

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"k8s-go/internal/api"
	"k8s-go/internal/kv"
	"k8s-go/internal/registry"
)

func TestPodCreateListWatchAndConflict(t *testing.T) {
	srv := testServer(t)

	res := do(t, srv, http.MethodPost, "/api/v1/namespaces/default/pods", []byte(`{
		"metadata": {"name": "nginx", "labels": {"app": "web"}},
		"spec": {"containers": [{"name": "nginx", "image": "nginx:1.25"}]}
	}`))
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d; want 201", res.StatusCode)
	}
	var created api.Pod
	decode(t, res, &created)
	if created.Metadata.Name != "nginx" || created.Metadata.Namespace != "default" || created.Metadata.UID == "" || created.Metadata.ResourceVersion == 0 {
		t.Fatalf("created = %+v", created.Metadata)
	}
	first := created.Metadata.ResourceVersion

	res = do(t, srv, http.MethodGet, "/api/v1/namespaces/default/pods", nil)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d", res.StatusCode)
	}
	var list struct {
		ResourceVersion uint64    `json:"resourceVersion"`
		Items           []api.Pod `json:"items"`
	}
	decode(t, res, &list)
	if list.ResourceVersion < first || len(list.Items) != 1 || list.Items[0].Spec.Containers[0].Image != "nginx:1.25" {
		t.Fatalf("list = %+v", list)
	}

	created.Spec.NodeName = "node-a"
	res = do(t, srv, http.MethodPut, "/api/v1/namespaces/default/pods/nginx", mustJSON(t, created))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("update status = %d; want 200", res.StatusCode)
	}
	var updated api.Pod
	decode(t, res, &updated)
	if updated.Spec.NodeName != "node-a" || updated.Metadata.ResourceVersion == first {
		t.Fatalf("updated = %+v", updated)
	}

	watchURL := "/api/v1/namespaces/default/pods?watch=1&resourceVersion=" + strconv.FormatUint(first, 10)
	body := readWatchLine(t, srv, watchURL)
	var ev struct {
		Type   string  `json:"type"`
		Object api.Pod `json:"object"`
	}
	if err := json.Unmarshal(body, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Type != "MODIFIED" || ev.Object.Spec.NodeName != "node-a" {
		t.Fatalf("watch event = %s", body)
	}

	stale := created
	stale.Metadata.ResourceVersion = first
	stale.Spec.NodeName = "node-b"
	res = do(t, srv, http.MethodPut, "/api/v1/namespaces/default/pods/nginx", mustJSON(t, stale))
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("stale update status = %d; want 409", res.StatusCode)
	}

	status := api.Pod{
		Metadata: api.ObjectMeta{Name: "nginx", ResourceVersion: updated.Metadata.ResourceVersion},
		Status:   api.PodStatus{Phase: "Running"},
	}
	res = do(t, srv, http.MethodPut, "/api/v1/namespaces/default/pods/nginx/status", mustJSON(t, status))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status update = %d; want 200", res.StatusCode)
	}
	res = do(t, srv, http.MethodGet, "/api/v1/namespaces/default/pods/nginx", nil)
	var got api.Pod
	decode(t, res, &got)
	if got.Spec.NodeName != "node-a" || got.Status.Phase != "Running" {
		t.Fatalf("after status = node %q phase %q", got.Spec.NodeName, got.Status.Phase)
	}
}

func TestCreateUsesPathNamespace(t *testing.T) {
	srv := testServer(t)
	res := do(t, srv, http.MethodPost, "/api/v1/namespaces/kube-system/pods", []byte(`{"metadata":{"name":"dns"}}`))
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d; want 201", res.StatusCode)
	}
	var pod api.Pod
	decode(t, res, &pod)
	if pod.Metadata.Namespace != "kube-system" {
		t.Fatalf("namespace = %q; want kube-system", pod.Metadata.Namespace)
	}
}

func TestNodeCreateLeavesNamespaceEmpty(t *testing.T) {
	srv := testServer(t)
	res := do(t, srv, http.MethodPost, "/api/v1/nodes", []byte(`{"metadata":{"name":"node-a"}}`))
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d; want 201", res.StatusCode)
	}
	var node api.Node
	decode(t, res, &node)
	if node.Metadata.Name != "node-a" || node.Metadata.Namespace != "" {
		t.Fatalf("node = %+v", node.Metadata)
	}
}

func TestBadJSONAndMissingPod(t *testing.T) {
	srv := testServer(t)
	res := do(t, srv, http.MethodPost, "/api/v1/namespaces/default/pods", []byte(`{`))
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad json status = %d; want 400", res.StatusCode)
	}
	res = do(t, srv, http.MethodGet, "/api/v1/namespaces/default/pods/missing", nil)
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("missing status = %d; want 404", res.StatusCode)
	}
}

func testServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(New(registry.New(kv.New())))
	t.Cleanup(srv.Close)
	srv.Client().Timeout = 2 * time.Second
	return srv
}

func do(t *testing.T, srv *httptest.Server, method, path string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { res.Body.Close() })
	return res
}

func readWatchLine(t *testing.T, srv *httptest.Server, path string) []byte {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { res.Body.Close() })
	if res.StatusCode != http.StatusOK {
		t.Fatalf("watch status = %d; want 200", res.StatusCode)
	}
	line, err := bufio.NewReader(res.Body).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	return line
}

func decode(t *testing.T, res *http.Response, dest any) {
	t.Helper()
	if err := json.NewDecoder(res.Body).Decode(dest); err != nil {
		t.Fatal(err)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	body, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return body
}
