package kv

import (
	"sync"
	"testing"
)

func TestPutGet(t *testing.T) {
	s := New()
	s.Put("foo", "bar")

	got, ok := s.Get("foo")
	if !ok || got != "bar" {
		t.Fatalf("Get(foo) = %q, %v; want %q, true", got, ok, "bar")
	}
}

func TestPutReplaces(t *testing.T) {
	s := New()
	s.Put("foo", "bar")
	s.Put("foo", "baz")

	got, ok := s.Get("foo")
	if !ok || got != "baz" {
		t.Fatalf("Get(foo) = %q, %v; want %q, true", got, ok, "baz")
	}
}

func TestGetMissing(t *testing.T) {
	s := New()
	got, ok := s.Get("missing")
	if ok || got != "" {
		t.Fatalf("Get(missing) = %q, %v; want %q, false", got, ok, "")
	}
}

func TestDelete(t *testing.T) {
	s := New()
	s.Put("foo", "bar")

	existed, err := s.Delete("foo")
	if err != nil {
		t.Fatal(err)
	}
	if !existed {
		t.Fatal("Delete(foo) = false; want true")
	}
	if _, ok := s.Get("foo"); ok {
		t.Fatal("foo still present after Delete")
	}
	existed, err = s.Delete("foo")
	if err != nil {
		t.Fatal(err)
	}
	if existed {
		t.Fatal("second Delete(foo) = true; want false")
	}
}

func TestConcurrentPutGetDelete(t *testing.T) {
	s := New()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			s.Put("k", "v")
		}()
		go func() {
			defer wg.Done()
			s.Get("k")
		}()
		go func() {
			defer wg.Done()
			s.Delete("k")
		}()
	}
	wg.Wait()
}
