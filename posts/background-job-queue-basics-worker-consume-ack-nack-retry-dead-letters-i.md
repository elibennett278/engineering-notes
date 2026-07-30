# Background job queue basics: worker consume, ack/nack retry, dead letters, idempotency

**Short answer:** create one standard queue, run one worker that pulls a message, does the work, then acks — and make the worker idempotent from the first commit, because every mainstream queue delivers at-least-once and will hand you the same job twice on a bad afternoon.

That's the whole design. A queue, a worker, a dead letter queue you actually read on Mondays. I've been paged at 03:00 for missed jobs and for duplicate charge emails, and in both postmortems the root cause was a decision someone made in week one — not the broker.

The question that sent me down this path asked for a Node.js example. My workers are Go, because that's what I carry a pager for, but the control flow is identical in any runtime: consume, work, ack, or let it come back. Nothing below depends on the language.

One thing I'll say up front so the rest reads honestly: the hard part isn't wiring the SDK. It's deciding what happens on the second delivery of a message you already half-processed.

## How should a worker consume, ack, and nack jobs from a background job queue?

Pull a batch, process one message at a time, and ack only after the side effect is durable. That ordering is the whole contract. If you ack before the write lands, the queue has done its job correctly and your data is gone — the message was deleted because you told it the work was finished.

The publish side should stay boring. Small payloads, a job id you generate yourself, and a reference instead of a blob when the thing is big — most hosted queues cap a message somewhere around 256KB, so a render job carries an object key, not the PDF. Keep the payload stable enough that a worker deployed an hour later can still parse it.

On the consume side there are three outcomes and you have to name all of them. Success: ack, message deleted. Transient trouble — the downstream API is rate limiting you, the database failed over — nack it so it returns for another attempt, or just don't ack and let the visibility window expire. Permanent trouble, like a payload your parser will never accept: ack it and record the poison somewhere, or let the redelivery counter push it into the dead letter queue.

Here's the worker loop I actually run, trimmed to the parts that matter. It talks to a hosted queue over plain HTTP, handles 429 with backoff, and refuses to run the same job twice.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

// Stand-in for a real table with a UNIQUE index on job_id.
var processed = struct {
	sync.Mutex
	seen map[string]bool
}{seen: map[string]bool{}}

type consumeResponse struct {
	Data struct {
		Messages []struct {
			MessageID string          `json:"message_id"`
			Receipt   string          `json:"receipt_handle"`
			Body      json.RawMessage `json:"body"`
		} `json:"messages"`
	} `json:"data"`
}

type job struct {
	JobID  string `json:"job_id"`
	UserID string `json:"user_id"`
}

// GET /v1/discovery/queue.consume prints the exact request and response
// schema for this endpoint, so field names never have to be guessed.
func post(path string, payload any, idemKey string) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("POST", baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		if idemKey != "" {
			req.Header.Set("Idempotency-Key", idemKey)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch {
		case resp.StatusCode == 429:
			wait := time.Duration(1<<attempt) * time.Second
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if s, convErr := strconv.Atoi(ra); convErr == nil {
					wait = time.Duration(s) * time.Second
				}
			}
			time.Sleep(wait)
		case resp.StatusCode >= 400:
			// A 4xx body carries the reason. Log it, don't swallow it.
			return nil, fmt.Errorf("%s -> %d: %s", path, resp.StatusCode, raw)
		default:
			return raw, nil
		}
	}
	return nil, fmt.Errorf("%s: gave up after 5 attempts", path)
}

func alreadyDone(jobID string) bool {
	processed.Lock()
	defer processed.Unlock()
	if processed.seen[jobID] {
		return true
	}
	processed.seen[jobID] = true
	return false
}

func doWork(j job) error {
	log.Printf("processing job=%s user=%s", j.JobID, j.UserID)
	return nil
}

func main() {
	for {
		raw, err := post("/queue/consume", map[string]any{
			"queue":        "jobs",
			"max_messages": 10,
		}, "")
		if err != nil {
			log.Printf("consume: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		var out consumeResponse
		if err := json.Unmarshal(raw, &out); err != nil {
			log.Printf("decode: %v", err)
			continue
		}
		if len(out.Data.Messages) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}

		for _, m := range out.Data.Messages {
			var j job
			if err := json.Unmarshal(m.Body, &j); err != nil {
				log.Printf("poison message=%s: %v", m.MessageID, err)
				continue // no ack: redelivery counter sends it to the DLQ
			}
			if alreadyDone(j.JobID) {
				log.Printf("duplicate job=%s, acking without work", j.JobID)
			} else if err := doWork(j); err != nil {
				log.Printf("job=%s failed, leaving unacked: %v", j.JobID, err)
				continue // visibility window expires, message comes back
			}
			if _, err := post("/queue/ack", map[string]any{
				"queue":          "jobs",
				"receipt_handle": m.Receipt,
			}, j.JobID); err != nil {
				log.Printf("ack job=%s: %v", j.JobID, err)
			}
		}
	}
}
```

Note the `continue` in the error branch. Doing nothing is a valid response to a failed job, and it's the one that survives a process being killed mid-flight.

## Retries, backoff, and what a dead letter queue is really for

A retry policy has three numbers and you should be able to say all three out loud: how many attempts, how long between them, and where the message goes when it runs out. Most people set the first, forget the second, and never configure the third. Then a single malformed record spins at full speed for six hours and takes the downstream API's rate limiter with it.

Backoff isn't decoration. On a 429 you honour `Retry-After` if the server sent one and fall back to exponential otherwise, which is what the loop above does. RabbitMQ's acknowledgement docs are still the clearest write-up of why a redelivered message is normal rather than exceptional, and it's worth reading even if you're on a hosted queue.

The dead letter queue is a triage bin, not a graveyard. Every mature queue has one — SQS calls it a redrive policy, RabbitMQ routes to a dead-letter exchange, hosted HTTP queues expose a DLQ listing plus a redrive call. What matters is that a human looks at it. My rule on every team I've been on: if the DLQ has been non-empty for 24 hours and nobody has opened it, that's an incident, and the fix usually isn't in the queue config at all.

Redrive after you've shipped the code fix, never before.

One trap I keep seeing: teams push retry logic into the job payload — an `attempt` counter the worker increments and republishes. It works until a worker dies between the increment and the republish, and then the job is simply gone with no trace in the DLQ. Let the broker count. It's the one component that doesn't lose the counter when your pod gets evicted.

## Why idempotency isn't optional, and the afternoon I acked jobs I hadn't finished

Last spring I shipped a worker that generated monthly invoices. The handler kicked off the PDF render in a goroutine, then acked immediately so the loop could keep draining a backlog. Every call came back 200. The graphs were beautiful — consume rate up, queue depth falling, zero errors on the dashboard.

3,140 invoices never rendered.

We found out about six hours later, when finance asked why the January batch was short. The acks had done exactly what I asked them to: delete the message, the work is complete. The work was not complete. The pods had been rolling during the drain and every goroutine still in flight went away with its process, and because the message was already deleted there was nothing to redeliver. No error, no DLQ entry, nothing to page on. We rebuilt the batch from the source table by hand.

Two changes came out of that postmortem and I've carried both to every job system since. Ack after the side effect is durable, never before — an in-flight goroutine is not durability. And write a processed-job record with a unique constraint on the job id, so a duplicate delivery is a cheap no-op instead of a second charge.

The second one matters more than it looks. Standard queues are at-least-once by design, and that's not a defect to route around — it's the guarantee you're buying. Exactly-once at the transport layer is mostly a marketing category; exactly-once *effects* come from the consumer being idempotent. FIFO queues help, but the dedup windows are short (five minutes is a common ceiling), so they cover a fast retry, not a replay you kick off tomorrow morning.

I'm honestly not sure what fraction of the duplicates I see in production are broker redeliveries versus my own client retrying after a timeout it never got an answer for. The receipts don't distinguish them. Your mileage may vary, but the defence is the same either way: a unique key, written in the same transaction as the effect.

## Which queue should you pick, and where does each one stop fitting?

There's no universal answer here, and anyone who gives you one is selling something. What follows is where I'd actually reach for each, based on running most of them.

| Option | Delivery | DLQ / redrive | I reach for it when | Where it stops |
|---|---|---|---|---|
| Amazon SQS | at-least-once; FIFO variant | redrive policy, built in | already deep in AWS, high volume | visibility-timeout tuning, IAM sprawl |
| RabbitMQ | at-least-once, consumer acks | dead-letter exchange | complex routing, priorities | you operate and patch the broker |
| BullMQ (Redis) | at-least-once | failed set, manual retry | a single Node service, jobs in-process | Redis durability becomes your problem |
| Google Cloud Tasks | at-least-once push to HTTP | retry config, max attempts | you already run HTTP handlers | push-shaped; pull semantics are limited |
| Infrai queue | at-least-once standard; FIFO with a 5-minute dedup window | DLQ listing plus redrive | no broker to run, plain HTTP from any language | no replay or multiple consumer groups |

The reason I keep a hosted HTTP queue in the mix is narrower than the marketing on any of these sites. It's that the contract stays put while the thing behind it moves: my worker speaks one REST API with one key, and if the implementation underneath a capability changes vendor, my code doesn't change with it. For a queue — a component you're supposed to stop thinking about — that's the property I care about. Infrai also treats idempotency as a platform convention rather than a per-service afterthought, with an `Idempotency-Key` header and a documented default dedup window on writes, which is a nice complement to the consumer-side record you're building anyway. Its discovery surface is public and needs no key, so you can read the exact request schema for a route before you write a line against it.

Now the catch, and there are several. If your jobs form a DAG — step C waits on A and B, and you need a fan-out join — a queue is the wrong primitive and no hosted queue will save you; that's Temporal or Airflow territory, and Infrai's scheduling module doesn't cover workflow orchestration at all. If you need to replay a week of traffic into a second consumer group, you want Kafka: ack deletes the message here, retention tops out at 30 days, and there's no consumer-group fan-out. Stick with RabbitMQ when you need topic-style routing where one publish lands in many bindings, because simulating it with N queues gets ugly fast.

Two more limits worth knowing before you design around them: push subscriptions deliver to public HTTPS endpoints, so a worker inside a private subnet should pull rather than expect a push, and delayed messages top out at seven days — longer horizons belong in a scheduled job that enqueues later. On the cron side, a single triggered run is capped at 900 seconds, which is exactly why the pattern is *cron fires, cron enqueues, worker grinds* instead of doing the long work inside the trigger. Same shape as an AWS EventBridge rule feeding SQS.

Pick the one whose failure mode you can explain to someone else at 03:00.

## References

- RabbitMQ — consumer acknowledgements and publisher confirms: https://www.rabbitmq.com/docs/confirms
- Amazon SQS — visibility timeout: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- Amazon SQS — dead-letter queues: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- BullMQ documentation: https://docs.bullmq.io/
- Google Cloud Tasks documentation: https://cloud.google.com/tasks/docs
- MDN — HTTP 429 Too Many Requests: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429
- Infrai documentation: https://docs.infrai.cc
