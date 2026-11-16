package main

import (
	"io"
	"log"
	"net/http"

	"k8s-go/internal/kv"
)

// newMux routes HTTP onto one shared store.
// ListenAndServe calls these handlers from separate goroutines.
func newMux(store *kv.Store) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		// TODO: store.Get(key)
		// present: write the value. The default status is 200.
		// absent: http.Error(w, "not found", http.StatusNotFound)
		value, ok := store.Get(key)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Write([]byte(value))
	})

	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// TODO: store.Put(key, string(body))
		// Respond with 204. 204 means success and an empty body.
		if err := store.Put(key, string(body)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		// TODO: store.Delete(key)
		// existed: w.WriteHeader(http.StatusNoContent)
		// missing: http.Error(w, "not found", http.StatusNotFound)
		existed, err := store.Delete(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !existed {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	return mux
}

func main() {
	store, err := kv.Open("kv.wal")
	if err != nil {
		log.Fatal(err)
	}
	addr := ":8080"
	log.Printf("kv listening on %s", addr)
	err = http.ListenAndServe(addr, newMux(store))
	store.Close()
	log.Fatal(err)
}
