// & takes the address of a value. *Pod is the type of that address.
// Run: go run ./examples/pointers/share
package main

import "fmt"

type Pod struct {
	Name  string
	Phase string
}

func main() {
	pod := Pod{Name: "nginx", Phase: "Pending"}

	// p holds the address of pod. Its type is *Pod.
	p := &pod
	fmt.Printf("pod is a Pod:   %+v\n", pod)
	fmt.Printf("p is a *Pod:    %p\n", p)

	// p.Phase is short for (*p).Phase. Both follow the pointer.
	p.Phase = "Running"
	p.Name = "nginx-2"

	fmt.Printf("pod after p.Phase and (*p).Name: %+v\n", pod)

	// A second pointer to the same pod sees the same change.
	also := &pod
	also.Phase = "Succeeded"
	also.Name = "nginx-3"
	fmt.Printf("pod through also: %+v\n", pod)


}
