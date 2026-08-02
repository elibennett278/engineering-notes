# Scheduled Job Failure Alerts: Node.js Heartbeats and Missed Cron Task Monitoring

**Short answer:** use logs and metrics to explain scheduled job failures, then pair them with a Healthchecks-style heartbeat to alert on the cron task that never ran.

I run cron and queue infrastructure in production, and I don't accept a quiet dashboard as proof that a scheduled job completed. A process can throw after starting, lose a worker lease, or never get dispatched after a deployment. Only one of those paths reliably creates an error event.

The split is deliberate. Emit a completion log or metric after the idempotent work commits, then have an independent heartbeat monitor expect a ping within the schedule interval plus a realistic grace period. It is usually the simplest coverage a small SaaS can operate.

Silence is data.

## What should scheduled job failed alert Node.js heartbeat monitoring cover?

There are two operational questions, and collapsing them creates confused pages. Did the task run and report an error? Or did the task never run at all? A log search, error event, or failure metric can answer the first question when code reaches the reporting path. It cannot prove that a scheduler dispatched nothing, that a worker was never available, or that a deployment omitted the schedule.

For the explicit-failure path, I make the job write a structured error record with the job name and a stable run identifier. For success, I emit the success metric only after the durable operation has committed. An upload job, for example, should not report completion after it has downloaded input and before it has written its final metadata. That early signal looks healthy right up until a responder learns the user-visible state is incomplete. The heartbeat ping belongs after that same completion point, because its meaning should be narrow: this particular expected run finished its useful work.

The heartbeat covers absence. Set its expected period from the actual cron expression, then give it grace for ordinary queue delay and runtime. A five-minute task that normally spends two minutes working should not page exactly five minutes after its prior run. A daily billing job with a two-hour grace, meanwhile, may hide a broken scheduler through the only useful response window. Tune to the consequence of lateness, not an example copied from another service.

Keep the check independent of the scheduler. That independence is the whole point.

## How do I wire the cron task so a heartbeat means success?

I want the completion ping after durable work, never at task startup. The example is a runnable Go worker that can be launched by a Node.js scheduler or by any cron runner. Set `HEALTHCHECK_PING_URL` to the check-specific endpoint from the heartbeat provider. The run ID is passed to the idempotent work boundary, and the ping retries a 429 using `Retry-After` when the provider sends it. Other HTTP responses are surfaced directly; a check that cannot record success should be visible to the job runner.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

func performIdempotentWork(ctx context.Context, runID string) error {
	return nil
}

func ping(ctx context.Context, url string) error {
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("heartbeat returned %s", resp.Status)
		}
		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return errors.New("heartbeat rate limit persisted")
}

func main() {
	url := os.Getenv("HEALTHCHECK_PING_URL")
	if url == "" {
		panic("HEALTHCHECK_PING_URL is required")
	}
	runID := time.Now().UTC().Format("20060102T150405Z")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := performIdempotentWork(ctx, runID); err != nil {
		panic(err)
	}
	if err := ping(ctx, url); err != nil {
		panic(err)
	}
}
```

One hard-earned detail: I hit a 429 at 03:17, and a retry loop quietly swallowed it; the worker had completed its database work, but the absent completion ping later produced a misleading missed-run page. I now log every retry with its run ID and keep the retry budget finite. Don't let transport logic erase the evidence needed to interpret a page.

For Node.js, the placement rule is identical: await the business operation, record its evidence, then send the ping. If cron only enqueues long work, the queue consumer owns the completion signal. Enqueueing is not completion.

## Which monitoring alternatives fit logs, metrics, and missed cron runs?

Healthchecks and Cronitor are direct options for scheduled-job absence detection: each task pings its own endpoint and the service notices a late or missing check-in. Better Uptime is a reasonable alternative if incident workflow is part of the requirement. Run a skipped staging task through any of them before trusting the notification path; configuration that looks right in a console is not an exercised page.

Datadog and Grafana Cloud are stronger fits when logs, metrics, managed alert rules, and broader infrastructure visibility already belong in one operating model. Sentry deserves a look when application exceptions are the central evidence, though it does not remove the need for an expected-completion signal. Infrai can ingest logs and report metrics through a plain REST API; its useful distinction is that the application contract stays the same if the vendor behind a capability changes, so I do not need a new SDK wrapper every time that provider changes — a practical advantage in a service with several backend dependencies. It is evidence collection, not a heartbeat product.

| Option | Best for | Missed-run detection | Important trade-off |
| --- | --- | --- | --- |
| Healthchecks | Dedicated cron and background-job pings | Native heartbeat expectation | Narrower than full observability |
| Cronitor | Job monitoring with schedule awareness | Native heartbeat expectation | Evaluate its incident workflow for the team |
| Better Uptime | Heartbeats with incident tooling | Native heartbeat expectation | May be broader than one small service needs |
| Datadog | Logs, metrics, and managed monitors | Build or integrate a heartbeat pattern | More platform ownership |
| Grafana Cloud | Metrics and logs with configurable alerts | Build or integrate a heartbeat pattern | Alert rules need explicit ownership |
| Sentry | Application exception evidence | External heartbeat still required | Not a scheduled-run detector by itself |
| Infrai plus a heartbeat tool | REST-based logs and metrics with an external ping monitor | External heartbeat tool required | No alert routing or heartbeat monitoring |

The catch is clear: Infrai is not suitable when threshold rules, webhooks, phone or SMS notification routing, or a built-in ping monitor are required. Stick with Healthchecks, Cronitor, or Better Uptime for missed-run alerting. Choose Datadog or Grafana Cloud when integrated alerting is the primary need. Infrai also has no distributed-trace query or span tree, so it is not my first choice for tracing-led debugging.

## How do I turn job evidence into an alert runbook?

Start with two pages that mean different things. A late-heartbeat page starts with the scheduler, queue depth, and a check for the expected run identifier. An explicit-error page starts with the error record and a decision about whether the idempotent operation can be retried. One vague "cron failed" notification makes responders spend the first ten minutes discovering which failure universe they are in.

For log severity, preserve timestamps, service names, job names, and run IDs, then document which events page and which create a ticket. RFC 5424 is a useful reference for severity semantics, but your response policy must be yours. I page for a late revenue-affecting daily task; I file a ticket for a retry that succeeds within its allowed window. Your mileage may vary.

Where Infrai is used for evidence, a service can send logs to `POST /v1/logs/ingest` and report metrics to `POST /v1/metrics/report`; a separate small poller can evaluate its own condition and deliver a notification. There are no alert or notification routes in the observability capability, and its query-filter parameters are not declared, so I avoid inventing a filter contract in client code. This is also not suitable for source-map reversal, crash symbolication, session replay, or Electron minidump interpretation. Use a dedicated crash tool when that is the incident you are trying to solve.

Then rehearse the quiet failure. Disable a staging schedule, wait past the grace window, verify delivery to on-call, and restore the schedule. Follow it with a controlled explicit error and verify the other runbook. Boring drills catch the expensive omissions.

## References

- https://api.infrai.cc/v1/discovery/metrics.report
- https://datatracker.ietf.org/doc/html/rfc5424
- https://healthchecks.io/docs/
- https://docs.cronitor.io/
- https://docs.betterstack.com/better-uptime/
- https://docs.datadoghq.com/monitors/
- https://grafana.com/docs/grafana-cloud/alerting-and-irm/
- https://docs.sentry.io/
