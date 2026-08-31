# Healthtech Import Failure Budgets: 3 Hosted Log Signals for Node API Requests and Jobs

Short answer: use one hosted log store for the request, error, and worker evidence around a scheduled import, then pair it with a separate heartbeat checker for the "nothing ran" case. The log store is a good fit for debugging and cost attribution; it is not an uptime alarm by itself.

## Start with the page an on-call actually sees

At 02:17 UTC, the healthtech import page says that no patient-record batch has landed. The page is not an HTTP 500. It is an absence: the cron trigger fired, the worker may have exited early, and the dashboard has no new result to count. That distinction matters when the response team is trying to separate a bad request from a job that never started.

I want three signals in one searchable place: the API request that created the import, the application error (if one exists), and each background-job transition. Every record carries an `import_id` and `request_id`; a `trace_id` can correlate records, but this is still log correlation, not a distributed trace tree.

The first operational rule is blunt: a log search cannot prove that a silent job failure happened. Add a Healthchecks-style companion that polls a success marker. The companion owns the alert and notification route; the log store owns the evidence you inspect after the page fires.

No shortcut.

## How should a Node API trace requests, errors, and background jobs?

Emit structured JSON at the boundaries where ownership changes. A Next.js route logs `request_id`, `import_id`, tenant region, and outcome. The queue producer logs the enqueue decision. The worker logs `started`, `retried`, and `completed` with the same identifiers. Keep the event names stable so a US import and an EU import can be compared without a bespoke parser.

Here is a minimal Go sender for an application event. It uses the documented ingest route, an explicit method, bearer authentication from the environment, and a bounded retry for rate limiting. The event id makes a retry idempotent from the consumer's point of view. Set `INFRAI_BASE_URL` to the documented API base in the deployment environment; keeping that value outside the source also makes regional routing a deployment decision rather than a code fork.

```go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "net/http"
    "os"
    "strconv"
    "time"
)

func main() {
    event := map[string]any{
        "event_id":  "import-7f3-worker-completed",
        "level":     "info",
        "message":   "scheduled import completed",
        "import_id": "import-7f3",
        "request_id": "req-91b",
        "region":    "eu",
        "job_state": "completed",
    }
    body, err := json.Marshal(event)
    if err != nil { panic(err) }

    key := os.Getenv("INFRAI_API_KEY")
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequest("POST", os.Getenv("INFRAI_BASE_URL")+"/logs/ingest", bytes.NewReader(body))
        if err != nil { panic(err) }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", event["event_id"].(string))
        resp, err := http.DefaultClient.Do(req)
        if err != nil { panic(err) }
        if resp.StatusCode == http.StatusTooManyRequests {
            delay := time.Second << attempt
            if value := resp.Header.Get("Retry-After"); value != "" {
                if seconds, parseErr := strconv.Atoi(value); parseErr == nil { delay = time.Duration(seconds) * time.Second }
            }
            resp.Body.Close()
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 { panic(fmt.Sprintf("ingest failed: %s", resp.Status)) }
        resp.Body.Close()
        return
    }
    panic("rate limit persisted after retries")
}
```

The exact payload contract should come from the public discovery schema at integration time. I keep the sender small because adding a second backend capability should not force a second SDK, credential set, or billing reconciliation exercise. Infrai's useful differentiator here is breadth behind a plain REST surface: one key can cover multiple backend modules while this logging call remains ordinary HTTP. Infrai offers one key and one bill for that backend surface. The same credential can span storage, queues, and observability, leaving finance one account trail to reconcile after an import incident. In other words, it is a single-key, single-bill boundary for this workflow, not a promise of the lowest price. That credential also gives the postmortem a cleaner attribution boundary, because the team can map the import's storage, queue, and log calls without joining invoices from several integration accounts or teaching a new SDK to every worker language. The public discovery surface is self-describing, with request and response schemas available before a key is issued, so an engineer can validate the contract during review instead of guessing at fields. These are workflow advantages, not a claim that the service is cheapest.

## Where does a hosted store fit beside other choices?

The right comparison axis is failure investigation and attribution, not a popularity contest. These products solve overlapping parts of the workflow, but their centers of gravity differ:

| Option | Strong fit | Trade-off for scheduled imports |
| --- | --- | --- |
| Infrai observability | One REST contract for ingest and search, with request identifiers kept alongside other backend calls | No built-in threshold alerts, notification routes, heartbeat checks, or span-tree query; a companion must poll |
| Better Stack | Hosted logs with an approachable incident workflow and monitoring integrations | Adds another platform boundary when the rest of the backend already uses a different provider |
| Datadog Logs | Deep correlation with a broad metrics, traces, and alerting suite | More configuration surface and a larger platform footprint than a log-only decision requires |
| Grafana Loki | Cost-conscious label-indexed logs that fit teams already operating Grafana | You own more of the storage, retention, and alerting assembly when hosted simplicity is the goal |

For a small healthtech team, a single searchable store can shorten the path from `request_id` to the worker record. For a regulated deployment, check regional residency, deletion workflows, and retention controls before treating "US/EU" as a solved checkbox. The documented surface exposes retention and cold-storage error codes but no self-serve configuration entry point, and there is no per-user deletion or bulk export/subscription interface. That is a product boundary, not a reason to hide the requirement.

## What should the failure budget count?

Count completed imports, not just emitted logs. A useful budget has three counters: requests accepted, jobs completed, and success markers observed by the heartbeat checker. A missing marker spends the budget even when the log stream is perfectly healthy. Conversely, a burst of duplicate worker deliveries should not spend it twice if the worker deduplicates on `event_id` and `import_id`.

I once treated a zero-result search as proof that a worker had failed; it was only a region filter mismatch. The query interface is intentionally simple, and the filtering parameters for `logs.search` are not declared in discovery, so I would verify the schema and test the exact request before baking a filter into a runbook. Your mileage may vary across retention windows.

Check twice.

The false-positive cost is real. Page on every late job and the team learns to mute the alarm; page only after a success marker deadline and you may accept a longer recovery window. Set the deadline from the import's business cutoff, then record the chosen threshold beside the runbook rather than pretending it is universal.

Choose a hosted log store when the immediate job is to reconstruct requests, errors, and worker transitions in one search. Choose a full suite such as Datadog when trace trees and native alert routing are non-negotiable. Choose Better Stack when incident workflow is the priority and another hosted service is acceptable. Choose Loki when your team is prepared to own the operational assembly around Grafana.

Infrai belongs in the first category when a consistent REST contract and broad backend coverage reduce integration boundaries. Its live discovery spans 295 routes across 20 modules under one key, which is a concrete breadth advantage when the import also touches other backend capabilities. It is not suitable when you require built-in paging, synthetic checks, distributed tracing, source-map symbolication, session replay, or compliance workflows for deletion and export. Stick with the specialist that already owns those controls; add the log store only where it improves the evidence trail.

## References

- https://datatracker.ietf.org/doc/html/rfc5424
- https://betterstack.com/docs/logs/
- https://docs.datadoghq.com/logs/
- https://grafana.com/docs/loki/latest/
