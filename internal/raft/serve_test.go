package raft

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOnlyLeaderServes(t *testing.T) {
	c, err := Open(t.TempDir(), "A", "B", "C")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })

	a := httptest.NewServer(Handler(c, "A"))
	b := httptest.NewServer(Handler(c, "B"))
	t.Cleanup(a.Close)
	t.Cleanup(b.Close)

	put(t, a.URL+"/kv/foo", "bar")
	if got := get(t, a.URL+"/kv/foo"); got != "bar" {
		t.Fatalf("A Get = %q; want bar", got)
	}
	if status := statusOf(t, http.MethodGet, b.URL+"/kv/foo", ""); status != http.StatusServiceUnavailable {
		t.Fatalf("B Get status = %d; want 503", status)
	}
	if status := statusOf(t, http.MethodPut, b.URL+"/kv/foo", "nope"); status != http.StatusServiceUnavailable {
		t.Fatalf("B Put status = %d; want 503", status)
	}
	if got, ok := c.Node("A").Get("foo"); !ok || got != "bar" {
		t.Fatalf("store after rejected Put = %q, %v; want bar, true", got, ok)
	}

	c.Node("A").Down()
	if err := c.Elect("B"); err != nil {
		t.Fatal(err)
	}
	if err := c.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := get(t, b.URL+"/kv/foo"); got != "bar" {
		t.Fatalf("B after election = %q; want bar", got)
	}
	if status := statusOf(t, http.MethodGet, a.URL+"/kv/foo", ""); status != http.StatusServiceUnavailable {
		t.Fatalf("A after election status = %d; want 503", status)
	}

	put(t, b.URL+"/kv/foo", "baz")
	if got := get(t, b.URL+"/kv/foo"); got != "baz" {
		t.Fatalf("B Get = %q; want baz", got)
	}
	if got, ok := c.Node("C").Get("foo"); !ok || got != "baz" {
		t.Fatalf("C Get = %q, %v; want baz, true", got, ok)
	}
}

func put(t *testing.T, url, value string) {
	t.Helper()
	if status := statusOf(t, http.MethodPut, url, value); status != http.StatusNoContent {
		t.Fatalf("PUT %s = %d; want 204", url, status)
	}
}

func get(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d (%s); want 200", url, resp.StatusCode, body)
	}
	return string(body)
}

func statusOf(t *testing.T, method, url, value string) int {
	t.Helper()
	var body io.Reader
	if method == http.MethodPut {
		body = strings.NewReader(value)
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}
