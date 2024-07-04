![main](https://github.com/flowerinthenight/longsub/workflows/main/badge.svg)
[![Go Reference](https://pkg.go.dev/badge/github.com/flowerinthenight/longsub.svg)](https://pkg.go.dev/github.com/flowerinthenight/longsub)

**NOTE**: GCP PubSub now supports async subscription. I recommend using that instead of this.


## Overview

`longsub` is a small wrapper package for lengthy subscriptions for both [AWS SQS](https://aws.amazon.com/sqs/) and [GCP Pub/Sub](https://cloud.google.com/pubsub/). It will setup the subscription and attempts to extend the processing window at message level until the processing is done, or failed, or requeued. Useful if you want to keep the queue timeout relatively short (for quick message redelivery) but have the option for subscribers to go beyond the timeout (without redelivery) to process each of the messages.

Check out the [examples](./examples/) provided for reference on how to use the package.

## Authentication

For AWS, the following environment variables will be used.
```bash
# Required
AWS_REGION
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY

# Optional. If non-empty, package will attempt
# to assume this role using the key/secret above.
ROLE_ARN
```

For GCP, either the following environment variable:
```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/svcacct.json
```

or Pub/Sub access from the runtime environment (for ex., GCE, Workload Identity Federation, etc.) is required.
