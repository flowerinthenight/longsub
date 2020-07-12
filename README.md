![Go](https://github.com/flowerinthenight/longsub/workflows/Go/badge.svg)

## Overview

`longsub` is a small wrapper package for lengthy subscriptions for both [SQS](https://aws.amazon.com/sqs/) and [PubSub](https://cloud.google.com/pubsub/). It will setup the subscription and attempts to extend the processing window at message level until the processing is done, or failed, or requeued.
