// decode returns a *Pod, the same shape as registry.decode.
// Run: go run ./examples/pointers/decode
package main

import (
	"encoding/json"
	"fmt"
)

type Pod struct {
	Name  string `json:"name"`
	Phase string `json:"phase"`
}

func decode(raw string) *Pod {
	obj := &Pod{}
	if err := json.Unmarshal([]byte(raw), obj); err != nil {
		panic(err)
	}
	return obj
}

func main() {
	pod := decode(`{"name":"nginx","phase":"Pending"}`)
	fmt.Printf("decoded: %+v\n", pod)

	// pod is a *Pod, so this writes into the struct decode created.
	pod.Phase = "Running"
	fmt.Printf("edited:  %+v\n", pod)

	// A nil *Pod has no struct behind it.
	var missing *Pod
	missing = &Pod{}
	missing.Name = "nginx"
	fmt.Printf("missing == nil: %v\n", missing == nil)
	if missing != nil {
		fmt.Println(missing.Name)
	}
}
