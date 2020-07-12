package longsub

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/dchest/uniuri"
)

type SqsMessageCallback func(ctx interface{}, data []byte) error

type Option interface {
	Apply(*SqsLongSub)
}

type withRegion string

func (w withRegion) Apply(o *SqsLongSub) { o.region = string(w) }

// WithRegion sets the region option.
func WithRegion(v string) Option { return withRegion(v) }

type withAccessKeyId string

func (w withAccessKeyId) Apply(o *SqsLongSub) { o.key = string(w) }

// WithAccessKeyId sets the access key id option.
func WithAccessKeyId(v string) Option { return withAccessKeyId(v) }

type withSecretAccessKey string

func (w withSecretAccessKey) Apply(o *SqsLongSub) { o.secret = string(w) }

// WithSecretAccessKey sets the secret access key option.
func WithSecretAccessKey(v string) Option { return withSecretAccessKey(v) }

type withRoleArn string

func (w withRoleArn) Apply(o *SqsLongSub) { o.roleArn = string(w) }

// WithRoleArn sets the role arn option to assume to.
func WithRoleArn(v string) Option { return withRoleArn(v) }

type withTimeout int64

func (w withTimeout) Apply(o *SqsLongSub) { o.timeout = int64(w) }

// WithTimeout sets the timeout option.
func WithTimeout(v int64) Option { return withTimeout(v) }

type withNoExtend bool

func (w withNoExtend) Apply(o *SqsLongSub) { o.noExtend = bool(w) }

// WithNoExtend sets the flag to not extend the visibility timeout.
func WithNoExtend(v bool) Option { return withNoExtend(v) }

type withFatalOnQueueErr bool

func (w withFatalOnQueueErr) Apply(o *SqsLongSub) { o.fatalOnQueueError = bool(w) }

// WithFatalOnQueueError sets the function to crash when queue error.
func WithFatalOnQueueError(v bool) Option { return withFatalOnQueueErr(v) }

type withLogger struct{ l *log.Logger }

func (w withLogger) Apply(o *SqsLongSub) { o.logger = w.l }

// WithSecretAccessKey sets the logger option.
func WithLogger(v *log.Logger) Option { return withLogger{v} }

type SqsLongSub struct {
	ctx   interface{} // arbitrary data passed to callback function
	queue string

	logger *log.Logger

	region  string
	key     string
	secret  string
	roleArn string

	noExtend          bool // if true, no attempt to extend visibility per message
	fatalOnQueueError bool // if true, we crash if there are queue-related errors

	timeout  int64
	callback SqsMessageCallback
}

func (l *SqsLongSub) Start(quit context.Context, done chan error) error {
	localId := uniuri.NewLen(10)
	l.logger.Printf("sqs lengthy subscriber started, id=%v, time=%v", localId, time.Now())

	if l.timeout < 3 {
		return fmt.Errorf("timeout should be >= 3s")
	}

	sess, _ := session.NewSession(&aws.Config{
		Region:      aws.String(l.region),
		Credentials: credentials.NewStaticCredentials(l.key, l.secret, ""),
	})

	var svc *sqs.SQS
	if l.roleArn != "" {
		cnf := &aws.Config{Credentials: stscreds.NewCredentials(sess, l.roleArn)}
		svc = sqs.New(sess, cnf)
	} else {
		svc = sqs.New(sess)
	}

	var timeout int64 = l.timeout
	queueName := l.queue
	vistm := "VisibilityTimeout"

	resultURL, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: aws.String(queueName)})
	if err != nil {
		if !l.fatalOnQueueError {
			l.logger.Printf("GetQueueUrl failed, err=%v", err)
			return err
		} else {
			l.logger.Fatalf("GetQueueUrl failed, err=%v", err)
		}
	}

	attrOut, err := svc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: []*string{&vistm},
		QueueUrl:       resultURL.QueueUrl,
	})

	if err != nil {
		if !l.fatalOnQueueError {
			l.logger.Printf("GetQueueAttributes failed, err=%v", err)
			return err
		} else {
			l.logger.Fatalf("GetQueueAttributes failed, err=%v", err)
		}
	}

	vis, err := strconv.Atoi(*attrOut.Attributes[vistm])
	if err != nil {
		l.logger.Printf("visibility conv failed, err=%v", err)
		return err
	}

	donech := make(chan error, 1)
	var term int32

	// Monitor if we are requested to quit. Note that ReceiveMessage will block based on timeout
	// value so we could still be waiting for some time before we can actually quit gracefully.
	go func() {
		<-quit.Done()
		atomic.StoreInt32(&term, 1)
		donech <- nil
	}()

	l.logger.Printf("start listen, queue=%v, visibility=%vs", queueName, vis)

	for {
		// Should we terminate via quit?
		if atomic.LoadInt32(&term) > 0 {
			l.logger.Printf("requested to terminate, id=%v", localId)
			break
		}

		result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:              resultURL.QueueUrl,
			AttributeNames:        aws.StringSlice([]string{"SentTimestamp"}),
			MaxNumberOfMessages:   aws.Int64(1),
			MessageAttributeNames: aws.StringSlice([]string{"All"}),
			// If running in k8s, don't forget that default grace period for shutdown is 30s.
			// If you set 'WaitTimeSeconds' to more than that, this service will be
			// SIGKILL'ed everytime there is an update.
			WaitTimeSeconds: aws.Int64(timeout),
		})

		if err != nil {
			l.logger.Printf("get queue url failed, err=%v", err)
			continue
		}

		if len(result.Messages) == 0 {
			continue
		}

		var extendval = (vis / 3) * 2
		var ticker = time.NewTicker(time.Duration(extendval) * time.Second)
		var extend, cancel = context.WithCancel(context.TODO())
		extendch := make(chan error, 1)

		if l.noExtend {
			extendch <- nil
		} else {
			go func(queueUrl, receiptHandle string) {
				defer func() {
					l.logger.Printf("visibility timeout extender done for [%v]", receiptHandle)
					extendch <- nil
				}()

				change := true
				for {
					select {
					case <-extend.Done():
						return
					case <-ticker.C:
						if change {
							_, err := svc.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
								ReceiptHandle:     aws.String(receiptHandle),
								QueueUrl:          aws.String(queueUrl),
								VisibilityTimeout: aws.Int64(int64(vis)),
							})

							if err != nil {
								l.logger.Printf("[q=%v] extend visibility timeout for [%v] failed, err=%v", l.queue, receiptHandle, err)
								change = false
								continue
							}

							l.logger.Printf("[q=%v] visibility timeout for [%v] updated to %v", l.queue, receiptHandle, vis)
						}
					}
				}
			}(*resultURL.QueueUrl, *result.Messages[0].ReceiptHandle)
		}

		// Call message processing callback.
		err = l.callback(l.ctx, []byte(*result.Messages[0].Body))
		ack := true
		if rq, ok := err.(Requeuer); ok {
			if rq.ShouldRequeue() {
				ack = false
			}
		}

		if ack {
			_, err = svc.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      resultURL.QueueUrl,
				ReceiptHandle: result.Messages[0].ReceiptHandle,
			})

			if err != nil {
				l.logger.Printf("delete message failed, err=%v", err)
			}
		}

		// Terminate our extender goroutine and wait.
		cancel()
		ticker.Stop()
		<-extendch
	}

	done <- <-donech
	return nil
}

func NewSqsLongSub(ctx interface{}, queue string, callback SqsMessageCallback, o ...Option) *SqsLongSub {
	s := &SqsLongSub{
		ctx:      ctx,
		queue:    queue,
		region:   os.Getenv("AWS_REGION"),
		key:      os.Getenv("AWS_ACCESS_KEY_ID"),
		secret:   os.Getenv("AWS_SECRET_ACCESS_KEY"),
		roleArn:  os.Getenv("ROLE_ARN"),
		timeout:  5,
		callback: callback,
	}

	for _, opt := range o {
		opt.Apply(s)
	}

	if s.logger == nil {
		s.logger = log.New(os.Stdout, "[sqs]", 0)
	}

	return s
}
