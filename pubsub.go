package longsub

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	pubsubv1 "cloud.google.com/go/pubsub/apiv1"
	"github.com/dchest/uniuri"
	"github.com/pkg/errors"
	pubsubpb "google.golang.org/genproto/googleapis/pubsub/v1"
)

type Callback func(ctx interface{}, data []byte) error

type LengthySubscriberOption interface {
	Apply(*LengthySubscriber)
}

type withDeadline int

func (w withDeadline) Apply(o *LengthySubscriber) { o.deadline = int(w) }

// WithDeadline sets the deadline option.
func WithDeadline(v int) LengthySubscriberOption { return withDeadline(v) }

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

	var term int32
	go func() {
		<-quit.Done()
		l.logger.Printf("requested to terminate, id=%v", localId)
		atomic.StoreInt32(&term, 1)
	}()

	ctx := context.TODO()
	client, err := pubsubv1.NewSubscriberClient(ctx)
	if err != nil {
		return errors.Wrap(err, "new subscriber failed")
	}

	defer client.Close()

	subname := fmt.Sprintf("projects/%v/subscriptions/%v", l.project, l.subscription)

	req := pubsubpb.PullRequest{
		Subscription: subname,
		MaxMessages:  int32(l.maxMessages),
	}

	l.logger.Printf("start subscription listen on %v", subname)

	backoff := time.Second * 1
	backoffn, backoffmax := 0, l.backoffMax

	for {
		if atomic.LoadInt32(&term) > 0 {
			l.logger.Printf("id=%v terminated", localId)
			done <- nil
			return nil
		}

		res, err := client.Pull(ctx, &req)
		if err != nil {
			l.logger.Printf("client pull failed, backoff=%v, err=%v", backoff, err)

			time.Sleep(backoff)
			backoff = backoff * 2
			backoffn += 1
			if backoffn > l.backoffMax {
				l.logger.Printf("backoff exceeds %v", backoffmax)
				return errors.Errorf("backoff exceeds %v", backoffmax)
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
						l.logger.Printf("failed in ack deadline extend, err=%v", err)
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

				// Process message.
				err := l.callback(l.ctx, rm.Message.Data)
				ack := true
				if rq, ok := err.(Requeuer); ok {
					if rq.ShouldRequeue() {
						ack = false
					}
				}

				if ack {
					err = client.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
						Subscription: subname,
						AckIds:       []string{rm.AckId},
					})

					if err != nil {
						l.logger.Printf("ack failed, err=%v", err)
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

func NewLengthySubscriber(ctx interface{}, project, subscription string, callback Callback, o ...LengthySubscriberOption) *LengthySubscriber {
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
		s.logger = log.New(os.Stdout, "[pubsub]", 0)
	}

	return s
}
