// A Pod parameter receives a copy. A *Pod parameter receives an address.
// Run: go run ./examples/pointers/funcs
package main

import "fmt"

type Pod struct {
	Name  string
	Phase string
}

func setPhaseCopy(pod Pod, phase string) {
	pod.Phase = phase
}

func setPhase(pod *Pod, phase string) {
	pod.Phase = phase
}

// The receiver (p *Pod) makes Set the same kind of function as setPhase.
func (p *Pod) Set(phase string) {
	p.Phase = phase
}

func main() {
	pod := Pod{Name: "nginx", Phase: "Pending"}

	setPhaseCopy(pod, "Running")
	fmt.Printf("after setPhaseCopy: %+v\n", pod)

	setPhase(&pod, "Running")
	fmt.Printf("after setPhase:     %+v\n", pod)

	pod.Set("Succeeded")
	fmt.Printf("after pod.Set:      %+v\n", pod)

	// The same method can be called on the pointer.
	p := &pod
	p.Set("Failed")
	fmt.Printf("after p.Set:        %+v\n", pod)
}
