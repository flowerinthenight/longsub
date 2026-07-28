package gcppubsub

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func topicName(project, id string) string {
	return fmt.Sprintf("projects/%v/topics/%v", project, id)
}

func subscriptionName(project, id string) string {
	return fmt.Sprintf("projects/%v/subscriptions/%v", project, id)
}

// GetTopic retrieves a PubSub topic. It creates the topic if it doesn't exist.
func GetTopic(project, id string) (*pubsub.Publisher, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("NewClient failed: %w", err)
	}

	// Not closing the client here; the returned publisher is tied to it.
	name := topicName(project, id)
	_, err = client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: name})
	switch {
	case err == nil: // exists
	case status.Code(err) == codes.NotFound:
		_, err = client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: name})
		if err != nil {
			return nil, fmt.Errorf("CreateTopic failed: %w", err)
		}
	default:
		return nil, fmt.Errorf("GetTopic failed: %w", err)
	}

	return client.Publisher(name), nil
}

// GetSubscription retrieves a PubSub subscription. It creates the subscription if it doesn't exist, using the
// provided topic object. The default Ack deadline, if not provided, is one minute.
func GetSubscription(project, id string, topic *pubsub.Publisher, ackdeadline ...time.Duration) (*pubsub.Subscriber, error) {
	var extra []GetSubScription2Extra
	if len(ackdeadline) > 0 {
		extra = append(extra, GetSubScription2Extra{AckDeadline: ackdeadline[0]})
	}

	return GetSubscription2(project, id, topic, extra...)
}

type GetSubScription2Extra struct {
	AckDeadline           time.Duration
	EnableMessageOrdering bool
}

// GetSubscription2 is GetSubscription with a more flexible options.
func GetSubscription2(project, id string, topic *pubsub.Publisher, extra ...GetSubScription2Extra) (*pubsub.Subscriber, error) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("NewClient failed: %w", err)
	}

	// Not closing the client here; the returned subscriber is tied to it.
	name := subscriptionName(project, id)
	_, err = client.SubscriptionAdminClient.GetSubscription(ctx,
		&pubsubpb.GetSubscriptionRequest{Subscription: name})

	switch {
	case err == nil: // exists
	case status.Code(err) == codes.NotFound:
		if topic == nil {
			return nil, fmt.Errorf("topic is required when creating %v", name)
		}

		deadline := time.Second * 60
		var enableMessageOrdering bool
		if len(extra) > 0 {
			if extra[0].AckDeadline > 0 {
				deadline = extra[0].AckDeadline
			}

			enableMessageOrdering = extra[0].EnableMessageOrdering
		}

		_, err = client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
			Name:                  name,
			Topic:                 topic.String(),
			AckDeadlineSeconds:    int32(deadline.Seconds()),
			EnableMessageOrdering: enableMessageOrdering,
		})

		if err != nil {
			return nil, fmt.Errorf("CreateSubscription failed: %w", err)
		}
	default:
		return nil, fmt.Errorf("GetSubscription failed: %w", err)
	}

	return client.Subscriber(name), nil
}

// DelSubscription converts the client into an utter introvert.
func DelSubscription(project, name string) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("NewClient failed: %w", err)
	}

	defer client.Close()
	err = client.SubscriptionAdminClient.DeleteSubscription(ctx,
		&pubsubpb.DeleteSubscriptionRequest{Subscription: subscriptionName(project, name)})

	if err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("DeleteSubscription failed: %w", err)
	}

	return nil
}

// PublishRaw is a convenience function for publishing raw data to a topic.
func PublishRaw(ctx context.Context, topic *pubsub.Publisher, msg []byte) (string, error) {
	res := topic.Publish(ctx, &pubsub.Message{Data: msg})
	return res.Get(ctx)
}
