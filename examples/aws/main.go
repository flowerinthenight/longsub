package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	v3 "github.com/flowerinthenight/longsub/v3/awssqs"
)

var (
	queue    = flag.String("queue", "longsub-testqueue", "SQS queue to create and subscribe to")
	noextend = flag.Bool("noextend", false, "if true, disable message extender")
)

func longCallback(ctx any, data []byte) error {
	log.Println("recv:", string(data))
	log.Println("start long task (>1min)...")
	time.Sleep(time.Second * 90) // more than the queue's visibility timeout
	log.Println("long callback done")
	return nil
}

func callback(ctx any, data []byte) error {
	log.Println("recv:", string(data))
	log.Println("callback done")
	return nil
}

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())

	// Credentials come from the same env vars the subscriber defaults to:
	// AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and the optional ROLE_ARN.
	// Leaving key/secret empty falls back to the SDK's default credential chain.
	helper := v3.NewHelper(
		os.Getenv("AWS_REGION"),
		os.Getenv("AWS_ACCESS_KEY_ID"),
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		os.Getenv("ROLE_ARN"),
	)

	// This will be created, if permission allows. You need to delete it manually after.
	// A new queue gets the default visibility timeout of 30s, which is what makes the
	// extender observable below: longCallback() runs for 90s, so without the extender
	// the message would be redelivered twice before the callback returns.
	queueUrl, _, err := helper.GetSqs(ctx, *queue)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("queue:", *queueUrl)

	done := make(chan error, 1)
	go func() {
		var ls *v3.LengthySubscriber
		if *noextend {
			ls = v3.NewLengthySubscriber(ctx, *queue, callback, v3.WithNoExtend(true))
		} else {
			ls = v3.NewLengthySubscriber(ctx, *queue, longCallback)
		}

		err := ls.Start(ctx, done)
		if err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(time.Second * 5) // subscriber should be ready by now

	// Publishing isn't part of longsub's API, so we use the SDK directly here. Note that
	// this path uses the default credential chain only; it does not assume ROLE_ARN.
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	_, err = sqs.NewFromConfig(cfg).SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    queueUrl,
		MessageBody: aws.String("hello world"),
	})

	if err != nil {
		log.Fatal(err)
	}

	if !*noextend {
		time.Sleep(time.Minute * 2) // wait for longCallback()
	} else {
		time.Sleep(time.Second * 2) // wait for callback()
	}

	// Note that Start() can take up to WithTimeout() seconds (20s by default) to return,
	// since the in-flight long poll is allowed to finish before the loop breaks.
	cancel()
	<-done
}
