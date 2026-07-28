![main](https://github.com/flowerinthenight/longsub/workflows/main/badge.svg)
[![Go Reference](https://pkg.go.dev/badge/github.com/flowerinthenight/longsub.svg)](https://pkg.go.dev/github.com/flowerinthenight/longsub)

## Overview

**longsub** is a small wrapper package for lengthy subscriptions for both [AWS SQS](https://aws.amazon.com/sqs/) and [GCP Pub/Sub](https://cloud.google.com/pubsub/). It will setup the subscription and attempts to extend the processing window at message level until the processing is done, or failed, or requeued. Useful if you want to keep the queue timeout relatively short (for quick message redelivery) but have the option for subscribers to go beyond the timeout (without redelivery) to process each of the messages.

> [!NOTE]
> * `v3` moved AWS support from [aws-sdk-go](https://github.com/aws/aws-sdk-go) (end-of-support since July 2025) to [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2). For the `awssqs.Helper`, this means all methods now take a `context.Context` as first argument, and queue attributes are `map[string]string` instead of `map[string]*string`. The `LengthySubscriber` API is unchanged.
>
> * `v3` also moved GCP support to [cloud.google.com/go/pubsub/v2](https://pkg.go.dev/cloud.google.com/go/pubsub/v2). The `LengthySubscriber` behavior is unchanged, but the surrounding types follow the upstream rename: `GetTopic` now returns a `*pubsub.Publisher` (was `*pubsub.Topic`), `GetSubscription`/`GetSubscription2` return a `*pubsub.Subscriber` (was `*pubsub.Subscription`), and `WithClient` takes a `*apiv1.SubscriptionAdminClient` (was `*apiv1.SubscriberClient`). `DoArgs.Synchronous` is gone, as upstream removed `ReceiveSettings.Synchronous` in v2. The deprecated gizmo-based helpers (`GetPublisher`, `PubsubPublisher`, `NewPubsubPublisher`) were removed; use `PublishRaw` or the `*pubsub.Publisher` returned by `GetTopic` directly.
>
> * `v2` changed the GCP callback arguments from `[]byte` in `v1` to `CallbackArgs` to include the `Attributes` map.
>
> * GCP PubSub now supports async subscription. I recommend using that instead of this.

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

For GCP, longsub creates its Pub/Sub clients without explicit credentials, so
[Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/application-default-credentials)
are used. ADC looks for credentials in this order:

1. The `GOOGLE_APPLICATION_CREDENTIALS` environment variable, pointing to a service account key
   or external account (Workload Identity Federation) credential file:
   ```bash
   GOOGLE_APPLICATION_CREDENTIALS=/path/to/svcacct.json
   ```

2. The user credentials written by:
   ```bash
   gcloud auth application-default login
   ```
   Handy for local development. Add `--impersonate-service-account=<sa>@<project>.iam.gserviceaccount.com`
   if you need to run as a specific service account.

3. The attached service account from the runtime environment's metadata server (GCE, GKE Workload
   Identity, Cloud Run, Cloud Functions, App Engine, etc.). This is the recommended setup for
   deployed workloads since there is no key file to manage.

Whichever principal ADC resolves to needs the relevant Pub/Sub permissions (for ex.,
`roles/pubsub.subscriber` on the subscription for subscribers, `roles/pubsub.publisher` on the
topic for publishers).

## License

This library is licensed under the [MIT License](./LICENSE).
