![main](https://github.com/flowerinthenight/longsub/workflows/main/badge.svg)
[![Go Reference](https://pkg.go.dev/badge/github.com/flowerinthenight/longsub.svg)](https://pkg.go.dev/github.com/flowerinthenight/longsub)

## Overview

**longsub** is a small wrapper package for lengthy subscriptions for both [AWS SQS](https://aws.amazon.com/sqs/) and [GCP Pub/Sub](https://cloud.google.com/pubsub/). It will setup the subscription and attempts to extend the processing window at message level until the processing is done, or failed, or requeued. Useful if you want to keep the queue timeout relatively short (for quick message redelivery) but have the option for subscribers to go beyond the timeout (without redelivery) to process each of the messages.

> [!NOTE]
> `v3` moved AWS support from [aws-sdk-go](https://github.com/aws/aws-sdk-go) (end-of-support since July 2025) to [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2). For the `awssqs.Helper`, this means all methods now take a `context.Context` as first argument, and queue attributes are `map[string]string` instead of `map[string]*string`. The `LengthySubscriber` API is unchanged.
>
> `v2` changed the GCP callback arguments from `[]byte` in `v1` to `CallbackArgs` to include the `Attributes` map.
>
> GCP PubSub now supports async subscription. I recommend using that instead of this.

Check out the [examples](./examples/) provided for reference on how to use the package.

## Authentication

For AWS, the following environment variables will be used.
```bash
AWS_REGION
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY

# Optional. If non-empty, longsub will attempt to
# assume this role using the credentials above.
ROLE_ARN
```

If `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` are empty, longsub falls back to the AWS SDK's
default credential chain (shared config, IRSA, ECS task role, EC2 IMDS), so it also works when
running under an instance profile or service account with no static keys.

For GCP, either the following environment variable:
```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/svcacct.json
```

or Pub/Sub access from the runtime environment (for ex., GCE, Workload Identity Federation, etc.) is required.

## License

This library is licensed under the [MIT License](./LICENSE).
