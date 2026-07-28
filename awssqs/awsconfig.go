package awssqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// awsConfig builds the AWS config shared by the subscriber and the helper. An empty region
// falls back to AWS_REGION, and an empty key/secret pair falls back to the default credential
// chain (env, shared config, IRSA, ECS, EC2 IMDS). If roleArn is non-empty, the resolved
// credentials are used to assume it.
func awsConfig(ctx context.Context, region, key, secret, roleArn string) (aws.Config, error) {
	opts := []func(*config.LoadOptions) error{}
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}

	if key != "" && secret != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(key, secret, ""),
		))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return cfg, err
	}

	if roleArn != "" {
		// Cache the assumed-role credentials, otherwise every API call triggers an
		// AssumeRole roundtrip to STS.
		cfg.Credentials = aws.NewCredentialsCache(
			stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), roleArn),
		)
	}

	return cfg, nil
}
