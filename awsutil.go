package longsub

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/dchest/uniuri"
)

func NewAWSUtil(region, key, secret, rolearn string) *AWSUtil {
	return &AWSUtil{
		region:  region,
		key:     key,
		secret:  secret,
		rolearn: rolearn,
	}
}

type AWSUtil struct {
	region  string
	key     string
	secret  string
	rolearn string
}

func (u *AWSUtil) session() *session.Session {
	sess, _ := session.NewSession(&aws.Config{
		Region:      aws.String(u.region),
		Credentials: credentials.NewStaticCredentials(u.key, u.secret, ""),
	})

	return sess
}

func (u *AWSUtil) sqsSvc() *sqs.SQS {
	sess := u.session()
	var svc *sqs.SQS
	if u.rolearn != "" {
		cnf := &aws.Config{Credentials: stscreds.NewCredentials(sess, u.rolearn)}
		svc = sqs.New(sess, cnf)
		return svc
	}

	svc = sqs.New(sess)
	return svc
}

func (u *AWSUtil) snsSvc() *sns.SNS {
	sess := u.session()
	var svc *sns.SNS
	if u.rolearn != "" {
		cnf := &aws.Config{Credentials: stscreds.NewCredentials(sess, u.rolearn)}
		svc = sns.New(sess, cnf)
		return svc
	}

	svc = sns.New(sess)
	return svc
}

func (u *AWSUtil) GetAcctId() (*string, error) {
	sess := u.session()
	var svc *sts.STS
	if u.rolearn != "" {
		cnf := &aws.Config{Credentials: stscreds.NewCredentials(sess, u.rolearn)}
		svc = sts.New(sess, cnf)
	} else {
		svc = sts.New(sess)
	}

	res, err := svc.GetCallerIdentity(&sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, fmt.Errorf("GetCallerIdentity failed: %w", err)
	}

	return res.Account, nil
}

// GetSqsAllowAllPolicy returns a policy that can be used when creating an SQS queue that allow
// all SQS actions for everybody.
func (u *AWSUtil) GetSqsAllowAllPolicy(queue string) string {
	acct, err := u.GetAcctId()
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
func (u *AWSUtil) GetSqs(name string) (*string, map[string]*string, error) {
	svc := u.sqsSvc()
	policy := u.GetSqsAllowAllPolicy(name)
	create, err := svc.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  aws.String(name),
		Attributes: map[string]*string{"Policy": aws.String(policy)},
	})

	if err != nil {
		return nil, nil, fmt.Errorf("CreateQueue failed: %w", err)
	}

	qAttr, err := svc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl:       create.QueueUrl,
		AttributeNames: []*string{aws.String("All")},
	})

	if err != nil {
		return nil, nil, fmt.Errorf("GetQueueAttributes failed: %w", err)
	}

	return create.QueueUrl, qAttr.Attributes, nil
}

// GetTopic returns the ARN of a newly created topic or an existing one. CreateTopic API
// returns the ARN of an existing topic.
func (u *AWSUtil) GetTopic(name string) (*string, error) {
	svc := u.snsSvc()
	res, err := svc.CreateTopic(&sns.CreateTopicInput{Name: aws.String(name)})
	if err != nil {
		return nil, fmt.Errorf("CreateTopic failed: %w", err)
	}

	return res.TopicArn, nil
}

type SubscribeToTopicInput struct {
	QueueName  string
	TopicArn   string
	Attributes map[string]*string
}

// SubscribeToTopic creates the queue, or use an existing queue, and subscribe to the
// provided SNS topic.
func (u *AWSUtil) SubscribeToTopic(in *SubscribeToTopicInput) (*sns.SubscribeOutput, error) {
	if in == nil {
		return nil, fmt.Errorf("input cannot be nil")
	}

	_, qattr, err := u.GetSqs(in.QueueName)
	if err != nil {
		return nil, fmt.Errorf("GetSqs failed: %w", err)
	}

	svc := u.snsSvc()
	return svc.Subscribe(&sns.SubscribeInput{
		TopicArn:   aws.String(in.TopicArn),
		Protocol:   aws.String("sqs"),
		Endpoint:   qattr["QueueArn"],
		Attributes: in.Attributes,
	})
}

// SetupSnsSqsSubscription creates a subscription of sub to topic. It returns topic's ARN along with error.
func (u *AWSUtil) SetupSnsSqsSubscription(topic, sub string) (*string, error) {
	topicArn, err := u.GetTopic(topic)
	if err != nil {
		return nil, err
	}

	in := &SubscribeToTopicInput{
		QueueName:  sub,
		TopicArn:   *topicArn,
		Attributes: map[string]*string{"RawMessageDelivery": aws.String("true")},
	}

	_, err = u.SubscribeToTopic(in)
	if err != nil {
		return nil, err
	}

	return topicArn, nil
}
