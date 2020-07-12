package longsub

type Requeuer interface {
	ShouldRequeue() bool
}

// RequeueError is an error wrapper that implements the 'Requeuer' interface
// so we can check if a pubsub message processing error will be resubmitted back
// to queue.
type RequeueError struct {
	error
	requeue bool
}

func (re RequeueError) ShouldRequeue() bool { return re.requeue }

// NewRequeueError is our convenience function for creating a RequeueError object.
func NewRequeueError(err error, requeue ...bool) RequeueError {
	rq := true
	if len(requeue) > 0 {
		rq = requeue[0]
	}

	return RequeueError{err, rq}
}
