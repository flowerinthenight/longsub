package gcppubsub

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	pubsubv1 "cloud.google.com/go/pubsub/apiv1"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"github.com/dchest/uniuri"
	"github.com/flowerinthenight/longsub"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Callback func(ctx interface{}, data []byte) error

type Option interface {
	Apply(*LengthySubscriber)
}

type withClient struct{ c *pubsubv1.SubscriberClient }

func (w withClient) Apply(o *LengthySubscriber) { o.client = w.c }

// WithClient sets the PubSub client. If not provided, an internal client is
// used using the environment's credentials.
func WithClient(v *pubsubv1.SubscriberClient) Option { return withClient{v} }

type withDeadline int

func (w withDeadline) Apply(o *LengthySubscriber) { o.deadline = int(w) }

// WithDeadline sets the deadline option.
func WithDeadline(v int) Option { return withDeadline(v) }

type withMaxMessages int

func (w withMaxMessages) Apply(o *LengthySubscriber) { o.maxMessages = int(w) }

// WithMaxMessages sets the maximum messages retrieved during a pull. Default = 1.
func WithMaxMessages(v int) Option { return withMaxMessages(v) }

type withNoExtend bool

func (w withNoExtend) Apply(o *LengthySubscriber) { o.noExtend = bool(w) }

// WithNoExtend sets the flag to not extend the ack deadline.
func WithNoExtend(v bool) Option { return withNoExtend(v) }

type withLogger struct{ l *log.Logger }

func (w withLogger) Apply(o *LengthySubscriber) { o.logger = w.l }

// WithLogger sets the logger option. Can be silenced by setting v to:
//
//	log.New(ioutil.Discard, "", 0)
func WithLogger(v *log.Logger) Option { return withLogger{v} }

type LengthySubscriber struct {
	ctx          interface{} // any arbitrary data passed to callback
	client       *pubsubv1.SubscriberClient
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
	var err error
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
	client := l.client
	if client == nil {
		client, err = pubsubv1.NewSubscriberClient(subctx)
		if err != nil {
			l.logger.Printf("NewSubscriberClient failed: %v", err)
			return err
		}

		// Close if internal.
		defer client.Close()
	}

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
					l.logger.Printf("%s: ack extender done for %v", subname, ids)
					xdone <- struct{}{}
				}()

				for {
					select {
					case <-subctx.Done():
						return
					case <-xfinish:
						return
					case <-time.After(delay):
						l.logger.Printf("%s: modify ack deadline for %vs, ids=%v",
							subname, ackDeadline.Seconds(), ids)

						err := client.ModifyAckDeadline(subctx,
							&pubsubpb.ModifyAckDeadlineRequest{
								Subscription:       subname,
								AckIds:             ids,
								AckDeadlineSeconds: int32(ackDeadline.Seconds()),
							},
						)

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

type DoArgs struct {
	ProjectId      string // required
	TopicId        string // required
	SubscriptionId string // required

	// Required. The callback function to process the message asynchronously.
	// The callback is responsible for ack'ing the message or not.
	ReceiveCallback func(context.Context, *pubsub.Message)

	MaxOutstandingMessages int // optional, defaults to 1

	// Optional. Defaults to true when MaxOutstandingMessages is 1, else it
	// defaults to false (StreamingPull/async).
	Synchronous bool

	// Optional. Used only when creating the subscription (if not exists).
	// Defaults to 1 minute.
	AckDeadline time.Duration
}

// Do is our helper function to setup an async PubSub subscription. So far, this
// works as intended but the frequency of multiple redelivery of messages is
// surprisingly high than LengthySubscriber. Still under observation while being
// used in non-critical workflows.
//
// Since this is a long subscription library, the default use case is that every
// message's processing time will most likely be beyond the subscription's ack
// time and will be processing one message at a time, although you can set
// 'MaxOutstandingMessages' to > 1 in which case some form of concurrent
// processing can still be done.
//
// The function will block until ctx is cancelled or if setup returns an error.
func Do(ctx context.Context, args DoArgs) error {
	t, err := GetTopic(args.ProjectId, args.TopicId) // ensure
	if err != nil {
		return fmt.Errorf("GetTopic failed: %w", err)
	}

	bctx := context.Background()
	client, err := pubsub.NewClient(bctx, args.ProjectId)
	if err != nil {
		return fmt.Errorf("NewClient failed: %w", err)
	}

	defer client.Close()
	_, err = GetSubscription(args.ProjectId, args.SubscriptionId, t) // ensure
	if err != nil {
		return fmt.Errorf("GetSubscription failed: %w", err)
	}

	sub := client.Subscription(args.SubscriptionId)
	maxOutstandingMessages := 1
	if args.MaxOutstandingMessages > 1 {
		maxOutstandingMessages = args.MaxOutstandingMessages
	}

	sub.ReceiveSettings.MaxOutstandingMessages = maxOutstandingMessages
	sub.ReceiveSettings.Synchronous = args.Synchronous
	if maxOutstandingMessages == 1 {
		sub.ReceiveSettings.Synchronous = true
	}

	err = sub.Receive(ctx, args.ReceiveCallback)
	if err != nil {
		return fmt.Errorf("Receive failed: %w", err)
	}

	return nil
}
