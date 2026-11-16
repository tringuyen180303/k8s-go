package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"k8s-go/internal/kv"
)

func TestHTTPPutThenGet(t *testing.T) {
	srv := httptest.NewServer(newMux(kv.New()))
	t.Cleanup(srv.Close)

	put(t, srv.URL+"/kv/foo", "bar")

	resp, err := http.Get(srv.URL + "/kv/foo")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d; want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "bar" {
		t.Fatalf("GET body = %q; want %q", body, "bar")
	}
}

func TestHTTPGetMissing(t *testing.T) {
	srv := httptest.NewServer(newMux(kv.New()))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/kv/missing")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET status = %d; want 404", resp.StatusCode)
	}
}

func TestHTTPPutReplaces(t *testing.T) {
	srv := httptest.NewServer(newMux(kv.New()))
	t.Cleanup(srv.Close)

	put(t, srv.URL+"/kv/foo", "bar")
	put(t, srv.URL+"/kv/foo", "baz")

	resp, err := http.Get(srv.URL + "/kv/foo")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "baz" {
		t.Fatalf("GET body = %q; want %q", body, "baz")
	}
}

func TestHTTPDelete(t *testing.T) {
	srv := httptest.NewServer(newMux(kv.New()))
	t.Cleanup(srv.Close)

	put(t, srv.URL+"/kv/foo", "bar")

	req, err := http.NewRequest(http.MethodDelete, srv.URL+"/kv/foo", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d; want 204", resp.StatusCode)
	}

	resp, err = http.Get(srv.URL + "/kv/foo")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after DELETE status = %d; want 404", resp.StatusCode)
	}
}

func TestHTTPDeleteMissing(t *testing.T) {
	srv := httptest.NewServer(newMux(kv.New()))
	t.Cleanup(srv.Close)

	req, err := http.NewRequest(http.MethodDelete, srv.URL+"/kv/missing", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("DELETE status = %d; want 404", resp.StatusCode)
	}
}

func put(t *testing.T, url, value string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(value))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT %s status = %d; want 204", url, resp.StatusCode)
	}
}
