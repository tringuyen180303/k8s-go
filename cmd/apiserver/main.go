package main

import (
	"log"
	"net/http"

	"k8s-go/internal/apiserver"
	"k8s-go/internal/kv"
	"k8s-go/internal/registry"
)

func main() {
	store, err := kv.Open("registry.wal")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	addr := ":8080"
	log.Printf("apiserver listening on %s", addr)
	err = http.ListenAndServe(addr, apiserver.New(registry.New(store)))
	log.Fatal(err)
}
