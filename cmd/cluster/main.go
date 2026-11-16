package main

import (
	"context"
	"log"
	"os"
	"os/signal"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	url, err := Run(ctx, ".", ":8080")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("apiserver listening on %s", url)
	<-ctx.Done()
}
