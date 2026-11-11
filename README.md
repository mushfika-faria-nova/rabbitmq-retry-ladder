# rabbitmq-retry-ladder

**Who this is for:** Spring Boot teams whose RabbitMQ consumers need retries that wait in the broker, survive restarts, never lose a message, and leave an audit trail.

Spring Boot starter for loss-free RabbitMQ consumer retries with exponential backoff, a plateau and a time budget.

If you use Kafka, Spring already has `@RetryableTopic`. If you use RabbitMQ, there is no equivalent.
This library fills that gap, and adds a time budget and an audit trail.

> Status: Phase 4. Retries work against a real RabbitMQ. The demo and a quick start come in Phase 5. Where it stands: [docs/00-status.md](docs/00-status.md). Design: [docs/02-design.md](docs/02-design.md).

## What it does

When a consumer fails, the message waits in a RabbitMQ delay queue (no thread sleeps), then comes back and is tried again.
The waits grow, then level off, and retries stop after a time budget:

```yaml
retry-ladder:
  initial-delay: 1m      # first wait
  multiplier: 2          # each wait × 2 (use 1 for a fixed delay)
  max-delay: 8m          # the plateau
  window: 30m            # stop 30 minutes after the first failure
  max-attempts: 100      # safety limit
  non-retryable: [com.example.InvalidPayloadException]   # park at once, don't retry
  queues:
    orders: {}           # uses the defaults above
    payments:
      window: 2h         # only this one changes
```

These values are also the defaults, so you only set what you want to change.
A queue name with dots goes in brackets: `"[notifications.delivery-status]": {}`.

This gives waits of 1m, 2m, 4m, 8m, then 8m repeating, until 30 minutes have passed.
The library logs this ladder at startup, so you can check your settings at a glance.

Every message carries its own history in headers: how many retries, when it first failed, when it last waited and for how long.
When it stops, it is parked in `<queue>.dlq` with the reason: `window`, `attempts` or `non-retryable`.

## Why not RabbitMQ's built-in delayed retry?

RabbitMQ 4.3 added a built-in delayed retry for quorum queues. It is simpler, and if it fits your case, use it.
This library is for cases where it doesn't fit, such as payments, banking alerts and anything audited:

| You need | RabbitMQ 4.3 built-in | This library |
|---|---|---|
| Exponential waits with a plateau (1m, 2m, 4m, 8m) | No: the wait grows linearly with the attempt count | Yes |
| Stop after a time budget ("30 minutes after the first failure") | No: it stops by attempt count only | Yes |
| Skip retries for errors that can never succeed | No | Yes, with reason `non-retryable` |
| An audit trail: first-failure time, each wait, why it stopped | Only a delivery count | Yes, in headers, metrics and logs |
| Works before RabbitMQ 4.3 | No | Yes, on RabbitMQ 4.x |

Use the built-in feature when a simple, attempt-based, linear retry is enough.
Why the broker holds retries at all (and not Resilience4j, Kafka or Redis): [decision 7](docs/03-decisions.md).

## Project layout

| Path | What it is |
|---|---|
| `README.md` | this page: what it is, how to use it, where things are |
| `docs/00-status.md` | where the project stands, open decisions, backlog |
| `docs/01-requirements.md` | what the library must do (R-1 to R-8) and the tests that prove it |
| `docs/02-design.md` | how it works: formula, stop rule, queues, flows, guarantees |
| `docs/03-decisions.md` | all design decisions in one table |
| `LICENSE` | Apache-2.0 |
| `.gitignore` | files git ignores |
| `build.gradle.kts`, `settings.gradle.kts` | the Gradle build |
| `gradlew`, `gradlew.bat`, `gradle/` | the Gradle wrapper and the pinned versions |
| `retry-ladder-spring-boot-starter/` | the library |
| `demo/` | an example app |
| `compose.yaml` | RabbitMQ for local runs |
| `.github/workflows/` | CI: build and all tests on every pull request |

## Guarantees

- No message is lost while the broker is healthy. A duplicate is possible, so handlers must be safe to run twice.
- No ordering guarantee: a retried message goes behind newer ones.

Licensed under Apache-2.0.
