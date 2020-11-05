package main

import (
	"context"
	"log"
	"time"

	"github.com/flowerinthenight/longsub/gcppubsub"
)

func callback(ctx interface{}, data []byte) error {
	log.Println("recv:", string(data))
	log.Println("start long task (>1min)...")
	time.Sleep(time.Second * 90) // more than the ack deadline
	log.Println("callback done")
	return nil
}

func main() {
	// Update with your project id.
	project := "<project-id-here>"

	// These will be created, if permission allows.
	// You need to delete these manually after.
	topic := "longsub-testtopic"
	subscription := "longsub-testtopic"

	// Get topic, create if needed.
	p, t, err := gcppubsub.GetPublisher(project, topic)
	if err != nil {
		log.Fatal(err)
	}

	// Then subscribe. Ack deadline should be 1min by default.
	_, err = gcppubsub.GetSubscription(project, subscription, t)
	if err != nil {
		log.Fatal(err)
	}

	quit, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		ls := gcppubsub.NewLengthySubscriber(quit, project, subscription, callback)
		err := ls.Start(quit, done)
		if err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(time.Second * 5) // subscriber should be ready by now
	p.PublishRaw(context.Background(), "", []byte("hello world"))

	time.Sleep(time.Minute * 2)
	cancel()
	<-done
}
