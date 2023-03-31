package gcppubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	gpubsub "cloud.google.com/go/pubsub"
	zpubsub "github.com/NYTimes/gizmo/pubsub"
	gzpubsub "github.com/NYTimes/gizmo/pubsub/gcp"
)

// GetTopic retrieves a PubSub topic. It creates the topic if it doesn't exist.
func GetTopic(project, id string) (*gpubsub.Topic, error) {
	ctx := context.Background()
	client, err := gpubsub.NewClient(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("NewClient failed: %w", err)
	}

	// defer client.Close()
	topic := client.Topic(id)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("Exists failed: %w", err)
	}

	if !exists {
		return client.CreateTopic(ctx, id)
	}

	return topic, nil
}

// GetSubscription retrieves a PubSub subscription. It creates the subscription if it doesn't exist, using the
// provided topic object. The default Ack deadline, if not provided, is one minute.
func GetSubscription(project, id string, topic *gpubsub.Topic, ackdeadline ...time.Duration) (*gpubsub.Subscription, error) {
	ctx := context.Background()
	client, err := gpubsub.NewClient(ctx, project)
	if err != nil {
		return nil, fmt.Errorf("NewClient failed: %w", err)
	}

	// defer client.Close()
	sub := client.Subscription(id)
	exists, err := sub.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("Exists failed: %w", err)
	}

	if !exists {
		deadline := time.Second * 60
		if len(ackdeadline) > 0 {
			deadline = ackdeadline[0]
		}

		return client.CreateSubscription(ctx, id, gpubsub.SubscriptionConfig{
			Topic:       topic,
			AckDeadline: deadline,
		})
	}

	return sub, nil
}

// GetPublisher is a simple wrapper to create a PubSub publisher using gizmo's Publisher interface.
func GetPublisher(project, id string) (zpubsub.MultiPublisher, *gpubsub.Topic, error) {
	ctx := context.Background()
	// Ensure that it exists.
	t, err := GetTopic(project, id)
	if err != nil {
		return nil, nil, fmt.Errorf("GetTopic failed: %w", err)
	}

	p, err := gzpubsub.NewPublisher(ctx, gzpubsub.Config{
		ProjectID: project,
		Topic:     id,
	})

	if err != nil {
		return nil, nil, fmt.Errorf("NewPublisher failed: %w", err)
	}

	return p, t, nil
}

type PubsubPublisher struct {
	mp zpubsub.MultiPublisher
	rt *gpubsub.Topic
}

func (p *PubsubPublisher) Test() error {
	return p.mp.PublishRaw(context.Background(), "test", []byte("hello world"))
}

func (p *PubsubPublisher) Publish(key string, data interface{}) error {
	if p.mp == nil {
		return fmt.Errorf("publisher is nil")
	}

	b, err := json.Marshal(data)
	if err != nil {
		return err
	}

	err = p.mp.PublishRaw(context.Background(), key, b)
	if err != nil {
		return err
	}

	return nil
}

func (p *PubsubPublisher) RawTopic() *gpubsub.Topic { return p.rt }

func NewPubsubPublisher(projectId string, topicname string) (*PubsubPublisher, error) {
	// Make sure the publisher is created if it doesn't exist.
	cp, t, err := GetPublisher(projectId, topicname)
	if err != nil {
		return nil, err
	}

	return &PubsubPublisher{cp, t}, nil
}

// DelSubscription converts the client into an utter introvert.
func DelSubscription(project, name string) error {
	ctx := context.Background()
	client, err := gpubsub.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("NewClient failed: %w", err)
	}

	defer client.Close()
	sub := client.Subscription(name)
	exists, err := sub.Exists(ctx)
	if err != nil {
		return fmt.Errorf("Exists failed: %w", err)
	}

	if exists {
		err = sub.Delete(ctx)
		if err != nil {
			return fmt.Errorf("Delete failed: %w", err)
		}
	}

	return nil
}
