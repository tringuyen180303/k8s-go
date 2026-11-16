// A value is the struct itself. Assigning it copies every field.
// Run: go run ./examples/pointers/value
package main

import "fmt"

type Pod struct {
	Name  string
	Phase string
}

func main() {
	pod := Pod{Name: "nginx", Phase: "Pending"}
	fmt.Printf("pod:  %+v\n", pod)

	clone := pod
	clone.Phase = "Running"

	fmt.Printf("clone: %+v\n", clone)
	fmt.Printf("pod:  %+v\n", pod)
}
