package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/flowerinthenight/longsub/gcppubsub"
)

var (
	project  = flag.String("project", "", "GCP project")
	noextend = flag.Bool("noextend", false, "if true, disable message extender")
)

func longCallback(ctx interface{}, data []byte) error {
	log.Println("recv:", string(data))
	log.Println("start long task (>1min)...")
	time.Sleep(time.Second * 90) // more than the ack deadline
	log.Println("long callback done")
	return nil
}

func callback(ctx interface{}, data []byte) error {
	log.Println("recv:", string(data))
	log.Println("callback done")
	return nil
}

func main() {
	flag.Parse()

	// These will be created, if permission allows.
	// You need to delete these manually after.
	topic := "longsub-testtopic"
	subscription := "longsub-testtopic"

	// Get topic, create if needed.
	t, err := gcppubsub.GetTopic(*project, topic)
	if err != nil {
		log.Fatal(err)
	}

	// Then subscribe. Ack deadline should be 1min by default.
	_, err = gcppubsub.GetSubscription(*project, subscription, t)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		if *noextend {
			ls := gcppubsub.NewLengthySubscriber(ctx,
				*project,
				subscription,
				callback,
				gcppubsub.WithNoExtend(true),
			)

			err := ls.Start(ctx, done)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			ls := gcppubsub.NewLengthySubscriber(ctx, *project, subscription, longCallback)
			err := ls.Start(ctx, done)
			if err != nil {
				log.Fatal(err)
			}
		}
	}()

	time.Sleep(time.Second * 5) // subscriber should be ready by now
	gcppubsub.PublishRaw(ctx, t, []byte("hello world"))

	if !*noextend {
		time.Sleep(time.Minute * 2) // wait for longCallback()
	} else {
		time.Sleep(time.Second * 2) // wait for callback()
	}

	cancel()
	<-done
}
