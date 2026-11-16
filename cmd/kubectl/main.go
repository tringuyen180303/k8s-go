package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"k8s-go/internal/client"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: kubectl apply -f pod.json\n       kubectl get pods")
	}
	c := client.New("http://127.0.0.1:8080")
	ctx := context.Background()
	switch os.Args[1] {
	case "apply":
		if len(os.Args) != 4 || os.Args[2] != "-f" {
			log.Fatal("usage: kubectl apply -f pod.json")
		}
		if err := Apply(ctx, c, os.Args[3]); err != nil {
			log.Fatal(err)
		}
	case "get":
		if len(os.Args) != 3 || os.Args[2] != "pods" {
			log.Fatal("usage: kubectl get pods")
		}
		pods, err := GetPods(ctx, c, "default")
		if err != nil {
			log.Fatal(err)
		}
		for _, pod := range pods {
			image := ""
			if len(pod.Spec.Containers) > 0 {
				image = pod.Spec.Containers[0].Image
			}
			fmt.Printf("%s\t%s\t%s\n", pod.Metadata.Name, pod.Status.Phase, image)
		}
	default:
		log.Fatalf("kubectl: unknown command %q", os.Args[1])
	}
}
