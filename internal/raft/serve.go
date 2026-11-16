package raft

import (
	"io"
	"net/http"
	"strings"
)

// Handler serves GET and PUT /kv/{key} for one node.
// PUT commits with Cluster.Put only when id is the leader. A follower
// answers 503 and does not write.
// GET uses Cluster.Read. A follower, a down node, or a leader that cannot
// reach a majority answers 503. A missing key on the leader is 404.
// A successful PUT answers 204. A successful GET writes the value.
func Handler(c *Cluster, id string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		switch r.Method {
		case http.MethodGet:
			value, ok, err := c.Read(id, key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			if !ok {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.Write([]byte(value))
		case http.MethodPut:
			if _, _, err := c.Read(id, key); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := c.Put(key, string(body)); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}
