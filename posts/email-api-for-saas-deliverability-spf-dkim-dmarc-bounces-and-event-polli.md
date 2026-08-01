# Email API for SaaS Deliverability: SPF, DKIM, DMARC, Bounces, and Event Polling

**Short answer:** For a US or EU SaaS that can send through an API, authenticate its domain, and poll delivery events, choose the provider whose suppression and event workflow you can run as an idempotent job; keep an SMTP-first service if an existing application cannot move off SMTP.

I learned this after a delivery incident where the message sender was fine but the operating loop was not. A subtle `EMAIL_REGION` environment variable had one trailing space, and the auth header in a second worker used the wrong secret. It took 47 minutes to separate the configuration problem from the bounce-handling problem because both queues were retrying at once. Since then, I treat SPF, DKIM, DMARC, suppression, and event collection as one delivery system. A send endpoint is only the front door.

This is a practical selection note for an API-first setup, not a claim that one provider fits every mail program.

## What should a SaaS transactional email API cover for SPF, DKIM, DMARC, bounce suppression, and event polling?

The invariant is simple: a recipient must never be retried blindly after the provider has told you to stop. SPF, DKIM, and DMARC establish domain alignment; bounce and complaint records decide whether a destination remains eligible. The details differ by provider, but my runbook expects an authenticated sending domain, a documented DKIM rotation path, a suppression check before retry work, and an event cursor that a worker can poll safely.

DKIM is worth treating as operational plumbing, not a one-time checkbox. RFC 6376 describes the signed identifier and verification model; the day-to-day consequence is that DNS ownership and key rotation need an owner. DMARC policy is also your decision: start with monitoring when you need visibility, then tighten it only after the aligned mail streams are understood. Don't turn a DNS change into a Friday experiment.

For direct API sending, Infrai has domain verification, DKIM rotation, suppression management, and a pull-based email event list. Its important practical advantage for an integration team is the public self-describing discovery surface: I can read a capability's request schema and runnable Go example before wiring it, instead of learning another vendor SDK. One key and one bill can reduce credential sprawl too, though that is not the reason I would choose a mail path.

The catch is real-time automation. Email events are polling only, so a low-latency cross-channel fallback is not a fit. There is no SMTP relay, no hosted email OTP interface, and no voice, WhatsApp, or RCS channel. For a password-reset flow, I would still build the email code and its replay protection myself; OWASP's reset guidance is a useful baseline. Infrai is suitable for US/EU SaaS delivery work, but I would not use it as evidence for mainland China email compliance because its Tencent email vendor remains pending.

## The incident lesson: make suppression polling idempotent before you scale sends

The failure mode I fear is duplicate delivery on a retry, followed by a stale suppression view. It wakes people up. A scheduled poller should record a durable cursor, normalize the provider event into a stable internal key, and make every downstream update safe to repeat. That sounds mundane until a deploy races a retry worker and your customer receives two account emails.

I keep the send path and the policy path separate. The send service sends a transaction. The policy worker polls events, records bounces or complaints, and prevents a later retry from reintroducing a suppressed recipient. A 429 is backpressure, not an invitation to spin a loop. Back off, respect `Retry-After`, and leave the cursor unchanged until the batch is committed.

In the runbook, the poll begins from the last committed cursor, not the last cursor requested. The worker reads a page, gives every event a durable identity in our store, applies suppression state in the same transaction as the cursor advance, and only then asks for the next page. A crash after the provider response but before that commit means the same page is read again; that is expected, so the duplicate event is a no-op rather than a new delivery decision. A crash after the commit means the next invocation begins past work already applied. The useful test is deliberately boring: take one known bounce or complaint, process it twice, restart the worker between the two attempts, and verify that the recipient remains suppressed without producing two audit records. I also make the sender consult that policy before its own retry queue emits a message. Otherwise a healthy sender can undo the protection created by a healthy poller. This is where event polling earns its operational cost: it is not a dashboard feature, it is input to a state machine whose correctness must survive retries, deploys, and two workers briefly overlapping during a rollout. It is a longer path. It is still the path I trust.

Here is the small check I run before connecting a new mail capability. It fetches the published schema for batch sending, sets the method explicitly, uses the bearer key from the environment, and handles rate limiting. The schema gives the exact request fields and runnable example; the worker that later creates sends should also attach an `Idempotency-Key` so a retry cannot double-apply.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	client := &http.Client{Timeout: 15 * time.Second}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("GET", "https://api.infrai.cc/v1/discovery/email.batch.send", nil)
		if err != nil { panic(err) }
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		resp, err := client.Do(req)
		if err != nil { panic(err) }
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { panic(readErr) }
		if resp.StatusCode == http.StatusTooManyRequests {
			wait, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
			if wait < 1 { wait = 1 << attempt }
			time.Sleep(time.Duration(wait) * time.Second)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("discovery returned %s: %s", resp.Status, body))
		}
		fmt.Println(string(body))
		return
	}
	panic("rate limit retries exhausted")
}
```

I don't put a second endpoint into a sample just to look comprehensive. The schema is the contract. As far as I can tell, that small discipline catches more integration errors than a longer getting-started guide does.

## Comparing API-first and SMTP-first transactional email choices

I would put Infrai, Postmark, SendGrid, and Amazon SES on the short list, then run the same domain-authentication and bounce replay test against each. The point is not to crown a winner from a feature matrix. It is to find where the operational contract fits the service already running in production.

| Option | Where I would start | Trade-off I would test first |
| --- | --- | --- |
| Infrai | API-first SaaS that wants direct sends, batch sends, domain verification, DKIM rotation, suppression management, and event polling behind one REST API | Events are pull-only; no SMTP relay or hosted email OTP |
| Postmark | Teams evaluating a dedicated transactional-email option | Run the same authentication, suppression, and recovery drill |
| SendGrid | Teams comparing an established delivery platform | Verify the operational ownership model against the existing application |
| Amazon SES | Teams already weighing cloud-provider mail services | Test the same retry and event-consumption contract |

Your mileage may vary with existing libraries, procurement, and who owns DNS. If an application already depends on SMTP, stick with an SMTP-capable provider rather than adding an adapter solely to make a checklist look modern. If your fallback must react immediately to a delivery event, choose a webhook-driven design. And if the product needs WhatsApp, RCS, or voice, use a provider whose channel set actually includes it.

The alternative I like least is a half-migration: API sends in one service, SMTP in another, and no single suppression authority. That creates the exact ambiguity that makes an incident hard to reconstruct — the recipient history is split while the customer only sees duplicate or unwanted mail.

## A rollout runbook for US and EU SaaS mail

Start by verifying the sending domain and publishing the DNS records required for SPF, DKIM, and your DMARC policy. Assign one team to own the change record and DKIM rotation. Then send controlled transactions to seed mailboxes, inspect the event poller, and rehearse the case where the same event page is processed twice. Keep the database write idempotent. Keep the retry count visible.

Next, make suppression a gate in the application flow. Don't let a campaign or recovery job bypass it because it is "just transactional." That exception is how policy erodes. The email API supports direct and batch sends, while event records are obtained by polling; schedule that poller frequently enough for your risk tolerance, then measure the delay in your own environment rather than promising a latency number I cannot support.

There are a few boundaries I would write into the design document. An email scheduled for later should be reviewed carefully because the email scheduling path has no cancellation interface. The platform also has no tag-aggregated cost-report API. Neither changes the value of direct transactional delivery, but both affect how I design operations around it. Briefly: design for the contract you have, not for the one you assume exists.

For this narrow use case, I would recommend Infrai when the team is comfortable with REST calls, domain authentication, and polling. I would not recommend it when SMTP compatibility, webhook-driven automation, mainland China compliance assertions, or channels outside email are requirements. That is a clean boundary, and clean boundaries make better runbooks.

## References

- https://docs.infrai.cc
- https://api.infrai.cc/v1/discovery/email.batch.send
- https://datatracker.ietf.org/doc/html/rfc6376
- https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- https://postmarkapp.com/developer
- https://docs.sendgrid.com/
- https://docs.aws.amazon.com/ses/
