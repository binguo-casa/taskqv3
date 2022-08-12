package main

import (
	"context"
	"flag"
	"log"

	"github.com/binguo-casa/taskqv3/example/api_worker"
)

func main() {
	flag.Parse()

	c := context.Background()

	err := api_worker.QueueFactory.StartConsumers(c)
	if err != nil {
		log.Fatal(err)
	}

	go api_worker.LogStats()

	sig := api_worker.WaitSignal()
	log.Println(sig.String())

	err = api_worker.QueueFactory.Close()
	if err != nil {
		log.Fatal(err)
	}
}
