# Requirements: rabbitmq-retry-ladder

**Who this is for:** anyone deciding whether this library fits their problem, or checking that it keeps its promises.
**After reading:** you know the problem, what the library must do, and which test proves each part. How it works is in [02-design.md](02-design.md).

## Problem

A consumer often fails because something it depends on is down for a short
time: a downstream API, a database failover, a rate limiter. The message is
fine. If we try again a little later, it will work.

RabbitMQ has no good built-in way to say "try again later". So teams end up
with one of three bad results:

* The message comes straight back and loops hot, using CPU and starving other
  messages.
* The consumer sleeps while holding the message. A restart loses the retry
  state, and one slow message blocks a consumer slot.
* The message goes to a dead-letter queue and waits for a person to replay it.

## What it must do

"Phase" is the build phase that proves it. The phases are listed in [00-status.md](00-status.md).

| ID | It must… | Proven by | Phase |
|---|---|---|---|
| R-1 | Make the waits grow, then level off. Settings: first wait, growth factor (1 = fixed wait), longest wait. The number of delay queues follows from the settings, never from the attempts, with a safe upper limit. | growth then plateau; a fixed wait uses one queue; bad settings stop startup; arrival times per retry | 3, 4 |
| R-2 | Stop retrying once a time budget, counted from the first failure, would be passed. An attempt limit is the backstop when the budget can't be worked out. | a retry landing exactly on the limit is allowed, 1 ms over is not; a message without its start time uses the attempt limit; a broken start time on the first failure is replaced; a message out of budget is parked; a replayed message starts a fresh history | 3, 4 |
| R-3 | Park errors that can never succeed at once, without retrying. The team lists which errors those are. | a listed error (also when wrapped) is parked at once; an unlisted one is retried | 3, 4 |
| R-4 | Never lose or change a message while the broker is healthy. A duplicate is possible. Retry copies and parked messages keep the same body, properties, `messageId` and the app's own headers; the library only adds its own headers. | a retry that can't be stored is parked, not dropped; a crash before the ack loses nothing; a message that finally succeeds is processed once by the demo's duplicate-safe handler; a retried and a parked message have the same body, properties, delivery mode and app headers as the original, even when the app's code changed them; an `Error` from the app's code parks the message and the listener keeps going | 4 |
| R-5 | Wait without using consumer threads, survive restarts and deploys, let any instance continue a retry, and let queues with different budgets share the same delay queues. | no thread sleeps during a wait; a retry scheduled before a restart arrives after it unchanged; two budgets share delay queues without affecting each other | 4 |
| R-6 | Keep each message's history with it: retry count, first failure, last wait, and, when parked, when and why it stopped (time, attempts or non-retryable). | the history is on every retry copy and on every parked message | 3, 4 |
| R-7 | Make every retry visible: counters, and one log line per retry that shows its whole timeline. Never log the message body. | the counters go up as expected; the log line has every field and no body | 4 |
| R-8 | Take its settings from Spring Boot properties, refuse bad values at startup, and print the retry plan for each queue at startup. | bad values stop startup; the startup log shows the expected plan | 2 |

## Limits

- Java 25, Spring Boot 4, Spring AMQP, RabbitMQ 4.x (pinned in tests).
- Gradle (Kotlin DSL), 9.1 or newer, with the wrapper checked in.
- One small API: a single `RetryLadder` bean used in a manual-ack listener.
- CI builds and runs every test on each pull request.
- Apache-2.0. Clean room: no employer code, names or config.

## Out of scope

- Exactly-once delivery, so handlers must be safe to run twice.
- Ordering: a retried message goes behind newer ones.
- Storing duplicate-check keys. The demo shows a simple duplicate-safe handler.

The detailed guarantees and their reasons are in [02-design.md](02-design.md), section 5.
