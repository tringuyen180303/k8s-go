package apiserver

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"k8s-go/internal/api"
	"k8s-go/internal/raft"
	"k8s-go/internal/registry"
)

// New routes kubectl-style HTTP onto the registry.
func New(store registry.Store) http.Handler {
	mux := http.NewServeMux()
	register(mux, store, "pods", "Pod", func() api.Object { return &api.Pod{} })
	register(mux, store, "replicasets", "ReplicaSet", func() api.Object { return &api.ReplicaSet{} })
	registerNodes(mux, store)
	return mux
}

// register mounts one namespaced resource.
// pods live at /api/v1/namespaces/{ns}/pods.
func register(mux *http.ServeMux, store registry.Store, resource, kind string, newObj func() api.Object) {
	prefix := "/api/v1/namespaces/{ns}/" + resource
	mux.HandleFunc("POST "+prefix, func(w http.ResponseWriter, r *http.Request) {
		obj := newObj()
		if err := json.NewDecoder(r.Body).Decode(obj); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		obj.GetObjectMeta().Namespace = r.PathValue("ns")
		if err := store.Create(r.Context(), obj); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(obj)
	})
	mux.HandleFunc("GET "+prefix, func(w http.ResponseWriter, r *http.Request) {
		writeListOrWatch(w, r, store, kind, r.PathValue("ns"))
	})
	mux.HandleFunc("GET "+prefix+"/{name}", func(w http.ResponseWriter, r *http.Request) {
		obj, err := store.Get(r.Context(), kind, r.PathValue("ns"), r.PathValue("name"))
		if err != nil {
			writeError(w, err)
			return
		}
		json.NewEncoder(w).Encode(obj)
	})
	mux.HandleFunc("PUT "+prefix+"/{name}/status", func(w http.ResponseWriter, r *http.Request) {
		obj := newObj()
		if err := json.NewDecoder(r.Body).Decode(obj); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		obj.GetObjectMeta().Namespace = r.PathValue("ns")
		obj.GetObjectMeta().Name = r.PathValue("name")
		if err := store.UpdateStatus(r.Context(), obj); err != nil {
			writeError(w, err)
			return
		}
		json.NewEncoder(w).Encode(obj)
	})
	mux.HandleFunc("PUT "+prefix+"/{name}", func(w http.ResponseWriter, r *http.Request) {
		obj := newObj()
		if err := json.NewDecoder(r.Body).Decode(obj); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		obj.GetObjectMeta().Namespace = r.PathValue("ns")
		obj.GetObjectMeta().Name = r.PathValue("name")
		if err := store.Update(r.Context(), obj); err != nil {
			writeError(w, err)
			return
		}
		json.NewEncoder(w).Encode(obj)
	})
	mux.HandleFunc("DELETE "+prefix+"/{name}", func(w http.ResponseWriter, r *http.Request) {
		err := store.Delete(r.Context(), kind, r.PathValue("ns"), r.PathValue("name"))
		if err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func registerNodes(mux *http.ServeMux, store registry.Store) {
	mux.HandleFunc("POST /api/v1/nodes", func(w http.ResponseWriter, r *http.Request) {
		var node api.Node
		if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		node.Metadata.Namespace = ""
		if err := store.Create(r.Context(), &node); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(node)
	})
	mux.HandleFunc("GET /api/v1/nodes", func(w http.ResponseWriter, r *http.Request) {
		writeListOrWatch(w, r, store, "Node", "")
	})
	mux.HandleFunc("GET /api/v1/nodes/{name}", func(w http.ResponseWriter, r *http.Request) {
		node, err := store.Get(r.Context(), "Node", "", r.PathValue("name"))
		if err != nil {
			writeError(w, err)
			return
		}
		json.NewEncoder(w).Encode(node)
	})
	mux.HandleFunc("PUT /api/v1/nodes/{name}/status", func(w http.ResponseWriter, r *http.Request) {
		var node api.Node
		if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		node.Metadata.Namespace = ""
		node.Metadata.Name = r.PathValue("name")
		if err := store.UpdateStatus(r.Context(), &node); err != nil {
			writeError(w, err)
			return
		}
		json.NewEncoder(w).Encode(node)
	})
}

func writeListOrWatch(w http.ResponseWriter, r *http.Request, store registry.Store, kind, namespace string) {
	if r.URL.Query().Get("watch") == "1" {
		rev, err := strconv.ParseUint(r.URL.Query().Get("resourceVersion"), 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ch, err := store.Watch(r.Context(), kind, namespace, rev)
		if err != nil {
			writeError(w, err)
			return
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming is not supported", http.StatusInternalServerError)
			return
		}
		enc := json.NewEncoder(w)
		for ev := range ch {
			if err := enc.Encode(ev); err != nil {
				return
			}
			flusher.Flush()
		}
		return
	}
	items, rev, err := store.List(r.Context(), kind, namespace)
	if err != nil {
		writeError(w, err)
		return
	}
	json.NewEncoder(w).Encode(struct {
		ResourceVersion uint64       `json:"resourceVersion"`
		Items           []api.Object `json:"items"`
	}{ResourceVersion: rev, Items: items})
}

func writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, registry.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, registry.ErrConflict):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, raft.ErrUnavailable):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
