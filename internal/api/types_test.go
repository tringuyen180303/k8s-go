package api

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestPodJSONRoundTrip(t *testing.T) {
	const raw = `{
		"metadata": {
			"name": "nginx",
			"namespace": "default",
			"uid": "pod-1",
			"resourceVersion": 7,
			"labels": {"app": "web"},
			"ownerReferences": [{"kind": "ReplicaSet", "name": "web", "uid": "rs-1"}]
		},
		"spec": {
			"nodeName": "node-a",
			"containers": [{"name": "nginx", "image": "nginx:1.25"}]
		},
		"status": {"phase": "Pending"}
	}`

	var pod Pod
	if err := json.Unmarshal([]byte(raw), &pod); err != nil {
		t.Fatal(err)
	}
	if pod.Metadata.Name != "nginx" || pod.Metadata.Namespace != "default" || pod.Metadata.UID != "pod-1" {
		t.Fatalf("metadata = %+v", pod.Metadata)
	}
	if pod.Metadata.ResourceVersion != 7 || pod.Metadata.Labels["app"] != "web" {
		t.Fatalf("metadata = %+v", pod.Metadata)
	}
	if len(pod.Metadata.OwnerReferences) != 1 || pod.Metadata.OwnerReferences[0].UID != "rs-1" {
		t.Fatalf("owners = %+v", pod.Metadata.OwnerReferences)
	}
	if pod.Spec.NodeName != "node-a" || len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].Image != "nginx:1.25" {
		t.Fatalf("spec = %+v", pod.Spec)
	}
	if pod.Status.Phase != "Pending" {
		t.Fatalf("phase = %q", pod.Status.Phase)
	}

	out, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(out)
	for _, field := range []string{`"metadata"`, `"nodeName"`, `"resourceVersion"`, `"labels"`, `"ownerReferences"`} {
		if !strings.Contains(encoded, field) {
			t.Fatalf("encoded pod = %s; missing %s", encoded, field)
		}
	}
	var again Pod
	if err := json.Unmarshal(out, &again); err != nil {
		t.Fatal(err)
	}
	if again.Spec.NodeName != "node-a" || again.Metadata.Labels["app"] != "web" || again.Metadata.ResourceVersion != 7 {
		t.Fatalf("round trip = %+v", again)
	}
}

func TestNodeAndReplicaSetJSON(t *testing.T) {
	beat := time.Date(2026, 11, 12, 20, 0, 0, 0, time.UTC)
	nodeRaw := `{
		"metadata": {"name": "node-a"},
		"status": {"ready": true, "lastHeartbeatTime": "2026-11-12T20:00:00Z"}
	}`
	var node Node
	if err := json.Unmarshal([]byte(nodeRaw), &node); err != nil {
		t.Fatal(err)
	}
	if node.Metadata.Name != "node-a" || !node.Status.Ready || !node.Status.LastHeartbeat.Equal(beat) {
		t.Fatalf("node = %+v", node)
	}

	rsRaw := `{
		"metadata": {"name": "web", "namespace": "default"},
		"spec": {"replicas": 2, "selector": {"app": "web"}},
		"status": {"replicas": 2, "readyReplicas": 1}
	}`
	var rs ReplicaSet
	if err := json.Unmarshal([]byte(rsRaw), &rs); err != nil {
		t.Fatal(err)
	}
	if rs.Spec.Replicas != 2 || rs.Spec.Selector["app"] != "web" || rs.Status.ReadyReplicas != 1 {
		t.Fatalf("replicaset = %+v", rs)
	}
}

func TestDefaultNamespace(t *testing.T) {
	pod := ObjectMeta{Name: "nginx"}
	pod.DefaultNamespace(true)
	if pod.Namespace != "default" {
		t.Fatalf("pod namespace = %q; want default", pod.Namespace)
	}

	named := ObjectMeta{Name: "nginx", Namespace: "kube-system"}
	named.DefaultNamespace(true)
	if named.Namespace != "kube-system" {
		t.Fatalf("namespace = %q; want kube-system", named.Namespace)
	}

	node := ObjectMeta{Name: "node-a", Namespace: "default"}
	node.DefaultNamespace(false)
	if node.Namespace != "" {
		t.Fatalf("node namespace = %q; want empty", node.Namespace)
	}
}

func TestMatchLabels(t *testing.T) {
	labels := map[string]string{"app": "web", "tier": "fe"}
	if MatchLabels(labels, nil) || MatchLabels(labels, map[string]string{}) {
		t.Fatal("empty selector matched")
	}
	if !MatchLabels(labels, map[string]string{"app": "web"}) {
		t.Fatal("app=web did not match")
	}
	if MatchLabels(labels, map[string]string{"app": "web", "track": "canary"}) {
		t.Fatal("missing selector key matched")
	}
	if MatchLabels(map[string]string{"app": "db"}, map[string]string{"app": "web"}) {
		t.Fatal("different value matched")
	}
}
