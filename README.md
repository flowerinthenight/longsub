![main](https://github.com/flowerinthenight/longsub/workflows/main/badge.svg)

## Overview

`longsub` is a small wrapper package for lengthy subscriptions for both [SQS](https://aws.amazon.com/sqs/) and [PubSub](https://cloud.google.com/pubsub/). It will setup the subscription and attempts to extend the processing window at message level until the processing is done, or failed, or requeued.

Check out the [examples](./examples/) provided for reference on how to use the package.

## Authentication

For AWS, the following environment variables will be used.
```bash
# Required
AWS_REGION
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY

# Optional. If non-empty, package will attempt to assume this role
# using the key/secret above.
ROLE_ARN
```

For GCP, the path of the service account JSON file is required.
```bash
GOOGLE_APPLICATION_CREDENTIALS=/etc/longsub/longsubsvcacct.json
```
