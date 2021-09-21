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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Callback func(ctx interface{}, data []byte) error

type Option interface {
	Apply(*LengthySubscriber)
}

type withDeadline int

func (w withDeadline) Apply(o *LengthySubscriber) { o.deadline = int(w) }

// WithDeadline sets the deadline option.
func WithDeadline(v int) Option { return withDeadline(v) }

type withNoExtend bool

func (w withNoExtend) Apply(o *LengthySubscriber) { o.noExtend = bool(w) }

// WithNoExtend sets the flag to not extend the visibility timeout.
func WithNoExtend(v bool) Option { return withNoExtend(v) }

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

	backoffMax int  // count for our exponential backoff for pull errors
	noExtend   bool // if true, no attempt to extend visibility per message
}

// Start starts the main goroutine handler. Terminates via 'ctx'. If 'done' is provided, will
// send a message there to signal caller that it's done with processing.
func (l *LengthySubscriber) Start(ctx context.Context, done ...chan error) error {
	localId := uniuri.NewLen(10)
	l.logger.Printf("lengthy subscriber started, id=%v, time=%v",
		localId, time.Now().Format(time.RFC3339))

	defer func(begin time.Time) {
		l.logger.Printf("duration=%v, id=%v", time.Since(begin), localId)
		if len(done) > 0 {
			done[0] <- nil
		}
	}(time.Now())

	var term int32
	go func() {
		<-ctx.Done() // for when we're past client.Pull()
		atomic.StoreInt32(&term, 1)
	}()

	subctx := context.WithValue(ctx, struct{}{}, nil)
	client, err := pubsubv1.NewSubscriberClient(subctx)
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
			return nil
		}

		res, err := client.Pull(subctx, &req)
		if err != nil {
			st, ok := status.FromError(err)
			if ok {
				if st.Code() == codes.Canceled {
					l.logger.Printf("[%v] cancelled, done", localId)
					return nil
				}
			}

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

		var xfinish = make(chan error)
		var xdone = make(chan struct{}, 1)
		var delay = 0 * time.Second // tick immediately upon reception
		var ackDeadline = time.Second * time.Duration(l.deadline)

		// Continuously notify PubSub that processing is still happening on this batch.
		if !l.noExtend {
			go func() {
				defer func() {
					l.logger.Printf("ack extender done for %v", ids)
					xdone <- struct{}{}
				}()

				for {
					select {
					case <-subctx.Done():
						return
					case <-xfinish:
						return
					case <-time.After(delay):
						l.logger.Printf("modify ack deadline for %vs, ids=%v", ackDeadline.Seconds(), ids)

						err := client.ModifyAckDeadline(subctx, &pubsubpb.ModifyAckDeadlineRequest{
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
		}

		eachmsgc := make(chan error)
		doneallc := make(chan error, 1)

		// Wait for all messages in this batch (of 1 message, actually) to finish.
		if len(res.ReceivedMessages) > 0 {
			go func(n int) {
				count := 0
				for {
					<-eachmsgc
					count++
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
					err = client.Acknowledge(subctx, &pubsubpb.AcknowledgeRequest{
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

		<-doneallc     // wait for all messages (just 1 actually) to finish
		close(xfinish) // this will terminate our ack extender goroutine
		if !l.noExtend {
			<-xdone // wait for our extender
		}
	}
}

// NewLengthySubscriber creates a lengthy subscriber object for PubSub.
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
		s.logger = log.New(os.Stdout, "[longsub/pubsub] ", 0)
	}

	return s
}
