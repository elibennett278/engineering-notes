# Expiring Stale Reservations: Rate-Limited API Job Processing with Queue and Cron

Short answer: for rate-limited job processing, use cron as the clock and a durable queue as the work ledger. In a B2B SaaS system that expires stale reservations after a fixed hold window, the queue should own admission and retry state, while the worker owns idempotency. Cron alone is suitable only when missed work, backlog, and duplicate side effects are genuinely acceptable.

I've been paged by both missed jobs and duplicate deliveries, including the familiar 429 path. The useful lesson was not a particular scheduler choice. It was that scheduling, rate limiting, and completion are different responsibilities. A per-minute API limit is a property of the downstream dependency. A cron expression can't enforce it, and a queue can't decide whether an expiry is still valid without business state.

## How should rate-limited job processing use a queue or cron?

Start with the reservation record, not the timer. Suppose a reservation has `held_until` and a stable reservation ID. A periodic scan can find records whose hold window has elapsed and publish an expiry command. The command should carry the ID and the expected state. The worker then checks the current record before making the side effect. A reservation that was paid, cancelled, or extended in the meantime must not be expired merely because an old timer fired.

That check also defines the recovery story. A scan may publish the same reservation twice because it raced with a worker restart, and a worker may finish the external call just before its lease expires. The next worker must be allowed to inspect the business record and the idempotency record, then safely conclude that the expiry is already complete or no longer applicable. If the external operation cannot accept an idempotency key, put the state transition behind a conditional write and make the downstream call depend on the recorded transition. It is a little more bookkeeping up front; it is much easier to reason about than trying to infer truth from delivery count.

Cron is good at saying “look now.” It is a poor place to keep hundreds of pending operations. If the scan launches every API call immediately, it has created a burst. If it sleeps between calls, it has become a fragile queue hidden inside one process. A durable queue gives each expiry a place to wait, while a shared limiter controls the rate across worker replicas.

The important state transitions are small: pending, leased, completed, or retryable. A `429 Too Many Requests` response means the dependency is asking the client to slow down; the response may include `Retry-After` to indicate when to try again. Treating 429 as permanent loses work. Retrying immediately creates another burst.

Cron is the clock.

Keep it boring.

## How should a Node.js backend handle retries and idempotency for expiry jobs?

The runtime does not change the delivery contract. A Node.js producer, a Go worker, or a managed HTTP consumer can all receive the same message twice. At-least-once delivery is a normal reason to design the consumer around an idempotency key rather than around an assumption that delivery happens once.

For reservation expiry, use a key such as `reservation-expire:{reservation_id}:{hold_version}`. Store the successful result under that key with an atomic uniqueness constraint. On a repeated delivery, return the recorded result or confirm that the reservation is already in the desired state. Acknowledge the message only after the state change and completion record are durable. The exact storage technology is a separate choice; the invariant is not.

The retry policy needs a boundary too. Honor `Retry-After` when it is present. Otherwise use bounded exponential backoff with jitter, and send work that exceeds its retry budget to a dead-letter path for inspection. AWS documents dead-letter queues as a way to handle messages that cannot be successfully consumed; the general operational point applies to any queue with an equivalent mechanism. The expiry should be observable by reservation ID, attempt count, next-attempt time, and final disposition.

I am not sure which backend has the lowest total cost for an unknown workload. The answer depends on whether a worker, a state store, and queue operations already exist, as well as on backlog size and the cost of missed reservations. “Cheapest” is therefore a decision criterion, not a reliable architecture. Count the operational pieces needed to recreate durability, throttling, retry visibility, and deduplication.

The bill is operational, too.

## Comparing cron, a simple queue, and a managed queue

| Option | Fits when | Main trade-off |
| --- | --- | --- |
| Cron alone | Expiry work is small, periodic, and safe to recompute | The process must rebuild backlog, pacing, retry, and duplicate protection |
| Cron plus a durable queue | A scan creates work and downstream calls have a per-minute limit | There are two boundaries to observe: scheduled discovery and delivery |
| A self-managed queue | The team already operates its queue and state store | Reliability and rate state remain on the team's runbook |
| A managed queue | The team wants queue durability without owning the queue service | Provider-specific delivery, retention, and integration limits still affect the design |
| A scheduled HTTP worker | The worker endpoint is public and the workload is small | Endpoint authentication, redelivery, and idempotency still need explicit handling |

The middle option is usually the least surprising for stale reservations: cron discovers expired candidates, the queue absorbs them, and workers pace calls to the dependency. That is not a claim that every queue is worth its overhead. Stick with cron when the expiry operation is a local database transition, can be rerun safely, and does not call a constrained external API. Choose a queue when a backlog must survive a process restart or when retry timing is part of the business reliability contract.

The catch is coordination. Ten workers that each enforce the same local per-minute allowance can still exceed a global allowance. The limiter must be shared, or concurrency must be constrained at a single controlled boundary. Test this with multiple workers and a deliberately low limit before production; a one-worker happy path proves very little.

## A small worker path that makes the contract visible

This Go example is intentionally plain. It shows the order that matters: check the completion key, wait for admission, call the dependency, honor a retry delay, then persist completion. The map is only a teaching stand-in; production completion state needs durable storage and an atomic uniqueness guarantee.

```go
package main

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

type Job struct {
	ReservationID string
	HoldVersion   string
}

type CompletionStore interface {
	Done(ctx context.Context, key string) (bool, error)
	MarkDone(ctx context.Context, key string) error
}

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<min(attempt, 6)) * 100 * time.Millisecond
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func process(ctx context.Context, client *http.Client, store CompletionStore, endpoint string, job Job) error {
	key := fmt.Sprintf("reservation-expire:%s:%s", job.ReservationID, job.HoldVersion)
	done, err := store.Done(ctx, key)
	if err != nil || done {
		return err
	}

	for attempt := 0; ; attempt++ {
		// Acquire shared rate-limit capacity here before the request.
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(retryDelay(resp, attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("unexpected status: %s", resp.Status)
		}
		return store.MarkDone(ctx, key)
	}
}
```

The code deliberately does not mark completion before the side effect. That ordering leaves a narrow crash window between the side effect and `MarkDone`, so the side effect itself must tolerate the same key or the server must provide an idempotency facility. No retry library can remove that requirement. It can only make the delay and attempt accounting easier to implement.

## When is cron the better answer?

Use cron by itself when the job is a reconciliation query that can safely recompute state, the work is bounded, and the dependency has no meaningful burst limit. Keep a durable cursor or query by `held_until`; do not treat the cron tick as proof that every candidate was processed. Emit counts for discovered, completed, retried, and skipped reservations. Alert on age of the oldest pending candidate, not only on whether the scheduler started.

Do not use the simple pattern for a workflow that needs fan-out and join semantics, an event log with independent consumers, or exact historical replay. Those requirements call for a workflow or streaming design with the relevant primitives. Likewise, a queue is a poor answer if the team cannot operate or observe the worker that consumes it.

The final test is a runbook question: can someone explain what happens after a restart, a duplicate delivery, a 429, a worker timeout, and a reservation update during processing? If the answer names durable state, bounded retries, shared rate ownership, and an idempotent expiry command, the choice is defensible. If it only names a scheduler, the design is unfinished.

## References

- AWS SQS dead-letter queues: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- MDN, HTTP 429 Too Many Requests: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429

## Further reading

- AWS SQS dead-letter queues: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- MDN, HTTP 429 Too Many Requests: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429
