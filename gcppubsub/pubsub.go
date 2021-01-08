package gcppubsub

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	pubsubv1 "cloud.google.com/go/pubsub/apiv1"
	"github.com/dchest/uniuri"
	"github.com/flowerinthenight/longsub"
	pubsubpb "google.golang.org/genproto/googleapis/pubsub/v1"
)

type Callback func(ctx interface{}, data []byte) error

type Option interface {
	Apply(*LengthySubscriber)
}

type withDeadline int

func (w withDeadline) Apply(o *LengthySubscriber) { o.deadline = int(w) }

// WithDeadline sets the deadline option.
func WithDeadline(v int) Option { return withDeadline(v) }

type withLogger struct{ l *log.Logger }

func (w withLogger) Apply(o *LengthySubscriber) { o.logger = w.l }

// WithSecretAccessKey sets the logger option.
func WithLogger(v *log.Logger) Option { return withLogger{v} }

type LengthySubscriber struct {
	ctx          interface{} // any arbitrary data passed to callback
	project      string
	subscription string
	deadline     int // seconds
	maxMessages  int
	callback     Callback

	logger *log.Logger

	// Count for our exponential backoff for pull errors.
	backoffMax int
}

func (l *LengthySubscriber) Start(quit context.Context, done chan error) error {
	localId := uniuri.NewLen(10)
	l.logger.Printf("pubsub lengthy subscriber started, id=%v, time=%v", localId, time.Now())

	defer func(begin time.Time) {
		l.logger.Printf("duration=%v, id=%v", time.Since(begin), localId)
	}(time.Now())

	var term int32
	go func() {
		<-quit.Done()
		l.logger.Printf("requested to terminate, id=%v", localId)
		atomic.StoreInt32(&term, 1)
	}()

	ctx := context.TODO()
	client, err := pubsubv1.NewSubscriberClient(ctx)
	if err != nil {
		return err
	}

	defer client.Close()

	subname := fmt.Sprintf("projects/%v/subscriptions/%v", l.project, l.subscription)
	req := pubsubpb.PullRequest{Subscription: subname, MaxMessages: int32(l.maxMessages)}
	backoff := time.Second * 1
	backoffn, backoffmax := 0, l.backoffMax

	l.logger.Printf("start subscription listen on %v", subname)

	for {
		if atomic.LoadInt32(&term) > 0 {
			l.logger.Printf("id=%v terminated", localId)
			done <- nil
			return nil
		}

		res, err := client.Pull(ctx, &req)
		if err != nil {
			l.logger.Printf("client pull failed, backoff=%v: %v", backoff, err)
			time.Sleep(backoff)
			backoff = backoff * 2
			backoffn += 1
			if backoffn > l.backoffMax {
				l.logger.Printf("backoff exceeds %v", backoffmax)
				return fmt.Errorf("backoff exceeds %v", backoffmax)
			}

			continue
		}

		// Pull() returns an empty list if there are no messages available in the
		// backlog. We should skip processing steps when that happens.
		if len(res.ReceivedMessages) == 0 {
			continue
		}

		var ids []string
		for _, m := range res.ReceivedMessages {
			ids = append(ids, m.AckId)
		}

		var finishc = make(chan error)
		var delay = 0 * time.Second // tick immediately upon reception
		var ackDeadline = time.Second * time.Duration(l.deadline)

		// Continuously notify the server that processing is still happening on this batch.
		go func() {
			defer l.logger.Printf("ack extender done for %v", ids)

			for {
				select {
				case <-ctx.Done():
					return
				case <-finishc:
					return
				case <-time.After(delay):
					l.logger.Printf("modify ack deadline for %vs, ids=%v", ackDeadline.Seconds(), ids)

					err := client.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
						Subscription:       subname,
						AckIds:             ids,
						AckDeadlineSeconds: int32(ackDeadline.Seconds()),
					})

					if err != nil {
						l.logger.Printf("ModifyAckDeadline failed: %v", err)
					}

					delay = ackDeadline - 10*time.Second // 10 seconds grace period
				}
			}
		}()

		eachmsgc := make(chan error)
		doneallc := make(chan error)

		// Wait for all messages in this batch (of 1 message, actually) to finish.
		if len(res.ReceivedMessages) > 0 {
			go func(n int) {
				count := 0
				for {
					<-eachmsgc
					count += 1
					if count >= n {
						doneallc <- nil
						return
					}
				}
			}(len(res.ReceivedMessages))
		}

		// Process each message concurrently.
		for _, msg := range res.ReceivedMessages {
			go func(rm *pubsubpb.ReceivedMessage) {
				defer func(begin time.Time) {
					l.logger.Printf("duration=%v, ids=%v", time.Since(begin), ids)
				}(time.Now())

				l.logger.Printf("payload=%v, ids=%v", string(rm.Message.Data), ids)

				ack := true
				err := l.callback(l.ctx, rm.Message.Data) // process message via callback
				if err != nil {
					if rq, ok := err.(longsub.Requeuer); ok {
						if rq.ShouldRequeue() {
							ack = false
						}
					} else {
						l.logger.Printf("callback failed: %v", err)
					}
				}

				if ack {
					err = client.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
						Subscription: subname,
						AckIds:       []string{rm.AckId},
					})

					if err != nil {
						l.logger.Printf("Acknowledge failed: %v", err)
					}
				}

				eachmsgc <- err
			}(msg)
		}

		// Wait for all messages (just 1 actually) to finish.
		<-doneallc

		// This will terminate our ack extender goroutine.
		close(finishc)
	}

	return nil
}

func NewLengthySubscriber(ctx interface{}, project, subscription string, callback Callback, o ...Option) *LengthySubscriber {
	s := &LengthySubscriber{
		ctx:          ctx,
		project:      project,
		subscription: subscription,
		deadline:     60,
		maxMessages:  1,
		callback:     callback,
		backoffMax:   5,
	}

	for _, opt := range o {
		opt.Apply(s)
	}

	if s.logger == nil {
		s.logger = log.New(os.Stdout, "[pubsub] ", 0)
	}

	return s
}
