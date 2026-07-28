package awssqs

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/dchest/uniuri"
)

func NewHelper(region, key, secret, rolearn string) *Helper {
	return &Helper{
		region:  region,
		key:     key,
		secret:  secret,
		rolearn: rolearn,
	}
}

type Helper struct {
	region  string
	key     string
	secret  string
	rolearn string
}

func (u *Helper) config(ctx context.Context) (aws.Config, error) {
	return awsConfig(ctx, u.region, u.key, u.secret, u.rolearn)
}

func (u *Helper) sqsSvc(ctx context.Context) (*sqs.Client, error) {
	cfg, err := u.config(ctx)
	if err != nil {
		return nil, fmt.Errorf("config failed: %w", err)
	}

	return sqs.NewFromConfig(cfg), nil
}

func (u *Helper) snsSvc(ctx context.Context) (*sns.Client, error) {
	cfg, err := u.config(ctx)
	if err != nil {
		return nil, fmt.Errorf("config failed: %w", err)
	}

	return sns.NewFromConfig(cfg), nil
}

func (u *Helper) GetAcctId(ctx context.Context) (*string, error) {
	cfg, err := u.config(ctx)
	if err != nil {
		return nil, fmt.Errorf("config failed: %w", err)
	}

	svc := sts.NewFromConfig(cfg)
	res, err := svc.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, fmt.Errorf("GetCallerIdentity failed: %w", err)
	}

	return res.Account, nil
}

// GetSqsAllowAllPolicy returns a policy that can be used when creating an SQS queue that allow
// all SQS actions for everybody.
func (u *Helper) GetSqsAllowAllPolicy(ctx context.Context, queue string) string {
	acct, err := u.GetAcctId(ctx)
	if err != nil {
		return ""
	}

	if acct == nil {
		return ""
	}

	return `{
  "Version":"2008-10-17",
  "Id":"id` + strings.ToLower(uniuri.NewLen(10)) + `",
  "Statement":[
    {
	  "Sid":"sid` + strings.ToLower(uniuri.NewLen(10)) + `",
	  "Effect":"Allow",
	  "Principal":"*",
	  "Action":"SQS:*",
	  "Resource":"` + fmt.Sprintf("arn:aws:sqs:%s:%s:%s", u.region, *acct, queue) + `"
    }
  ]
}`
}

// GetSqs creates an SQS queue and returning the queue url and attributes.
func (u *Helper) GetSqs(ctx context.Context, name string) (*string, map[string]string, error) {
	svc, err := u.sqsSvc(ctx)
	if err != nil {
		return nil, nil, err
	}

	policy := u.GetSqsAllowAllPolicy(ctx, name)
	create, err := svc.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String(name),
		Attributes: map[string]string{"Policy": policy},
	})

	if err != nil {
		return nil, nil, fmt.Errorf("CreateQueue failed: %w", err)
	}

	qAttr, err := svc.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       create.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
	})

	if err != nil {
		return nil, nil, fmt.Errorf("GetQueueAttributes failed: %w", err)
	}

	return create.QueueUrl, qAttr.Attributes, nil
}

// GetSqsFifo creates an SQS FIFO queue and returning the queue url and attributes.
func (u *Helper) GetSqsFifo(ctx context.Context, name string) (*string, map[string]string, error) {
	if !strings.HasSuffix(name, ".fifo") {
		name += ".fifo"
	}

	svc, err := u.sqsSvc(ctx)
	if err != nil {
		return nil, nil, err
	}

	policy := u.GetSqsAllowAllPolicy(ctx, name)
	create, err := svc.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(name),
		Attributes: map[string]string{
			"Policy":                    policy,
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})

	if err != nil {
		return nil, nil, fmt.Errorf("CreateQueue failed: %w", err)
	}

	qAttr, err := svc.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       create.QueueUrl,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
	})

	if err != nil {
		return nil, nil, fmt.Errorf("GetQueueAttributes failed: %w", err)
	}

	return create.QueueUrl, qAttr.Attributes, nil
}

// GetTopic returns the ARN of a newly created topic or an existing one. CreateTopic API
// returns the ARN of an existing topic.
func (u *Helper) GetTopic(ctx context.Context, name string) (*string, error) {
	svc, err := u.snsSvc(ctx)
	if err != nil {
		return nil, err
	}

	res, err := svc.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(name)})
	if err != nil {
		return nil, fmt.Errorf("CreateTopic failed: %w", err)
	}

	return res.TopicArn, nil
}

type SubscribeToTopicInput struct {
	QueueName  string
	TopicArn   string
	Attributes map[string]string
}

// SubscribeToTopic creates the queue, or use an existing queue, and subscribe to the
// provided SNS topic.
func (u *Helper) SubscribeToTopic(ctx context.Context, in *SubscribeToTopicInput) (*sns.SubscribeOutput, error) {
	if in == nil {
		return nil, fmt.Errorf("input cannot be nil")
	}

	_, qattr, err := u.GetSqs(ctx, in.QueueName)
	if err != nil {
		return nil, fmt.Errorf("GetSqs failed: %w", err)
	}

	svc, err := u.snsSvc(ctx)
	if err != nil {
		return nil, err
	}

	return svc.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn:   aws.String(in.TopicArn),
		Protocol:   aws.String("sqs"),
		Endpoint:   aws.String(qattr["QueueArn"]),
		Attributes: in.Attributes,
	})
}

// SetupSnsSqsSubscription creates a subscription of sub to topic. It returns topic's ARN along with error.
func (u *Helper) SetupSnsSqsSubscription(ctx context.Context, topic, sub string) (*string, error) {
	topicArn, err := u.GetTopic(ctx, topic)
	if err != nil {
		return nil, err
	}

	in := &SubscribeToTopicInput{
		QueueName:  sub,
		TopicArn:   *topicArn,
		Attributes: map[string]string{"RawMessageDelivery": "true"},
	}

	_, err = u.SubscribeToTopic(ctx, in)
	if err != nil {
		return nil, err
	}

	return topicArn, nil
}
