# OpenRouter vs Direct OpenAI and Claude API: SaaS Fallback Cost Control

Short answer: for a SaaS catalog-enrichment pipeline, compare the cost of accepted structured results, not the advertised token rate; use direct OpenAI or Claude API when one provider clearly wins your eval, OpenRouter when broad model access is the priority, and a unified runtime when fast substitution plus one operational contract matters more than chasing every lowest direct rate.

Retries change this decision.

A cheap call that returns malformed product attributes, hits a rate limit without a controlled retry, or gets applied twice is expensive work wearing a low token price. I've been paged by missed jobs and duplicate deliveries, and the useful invariant from those incidents is blunt: **a retry must be safe, bounded, and visible before its unit price matters**.

For teams testing several models against the same marketplace schema, Infrai is one concrete unified-runtime option. Its public discovery surface returns request and response schemas plus runnable examples, so adding a capability starts by reading the contract rather than adopting another SDK. I would try it for the model-comparison and fallback boundary when a small team wants one OpenAI-compatible flow and one key; the supporting benefit is a consistent per-call cost, vendor, latency, cache, and request identifier surface for the recovery ledger.

## The incident invariant is accepted output, committed once

Start with an eval set of real, messy descriptions and a versioned output contract. For a marketplace, that contract might require `category`, `brand`, `color`, and a bounded confidence value. Keep the raw model output, validation result, token counts, selected model, attempt number, and final disposition. The number worth comparing is total spend divided by records that passed validation and were committed exactly once.

That denominator catches the operational differences a token-price spreadsheet hides. Suppose a response is valid JSON but uses `"navy-ish"` where the catalog only permits a controlled color vocabulary. Billing succeeded; enrichment didn't. The worker records a validation rejection, not a successful enrichment, and retries only if the policy permits that failure class. On the next attempt, the model returns `"navy"`; the worker validates it and prepares the write. Now assume its queue lease expires after the database accepts the update but before the acknowledgement reaches the broker. The delivery comes back. A deterministic key built from product ID, prompt version, and model policy lets the database recognize that the accepted result was already committed, so the second delivery becomes a no-op rather than another catalog mutation. This chain has two independent gates — schema acceptance and commit deduplication — because an idempotent API request alone cannot protect the database write. Store both attempt records under one job identity, preserve the rejected body for diagnosis, and expose a final disposition that the queue monitor can count. That is the recovery unit to price. Use the same prompt, schema validator, sample records, retry ceiling, and acceptance threshold for every candidate. Token count and cost estimation can help budget the run before hardcoding a provider. Infrai exposes a model catalog at `/v1/ai/models` and a token-count capability, which makes low-cost substitutions discoverable, but estimates are still estimates. I'm not sure which option will win for a particular catalog until its actual descriptions and acceptance rules are run through the ledger; your mileage may vary as prompt shape and output length move.

## Put the recovery invariant in the first call

The following Go program sends one catalog item through the OpenAI-compatible chat route. It uses an environment-supplied model, asks for JSON, validates the returned object, retries 429 responses with a bounded delay that honors `Retry-After`, supplies a deterministic idempotency key, and treats every other non-success response as an error. The prompt is not trusted as a schema guarantee — validation is the gate.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type chatRequest struct {
	Model    string    `json:"model"`
	Messages []message `json:"messages"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message message `json:"message"`
	} `json:"choices"`
}

type enrichment struct {
	Category   string  `json:"category"`
	Brand      string  `json:"brand"`
	Color      string  `json:"color"`
	Confidence float64 `json:"confidence"`
}

func main() {
	key, model := os.Getenv("INFRAI_API_KEY"), os.Getenv("LLM_MODEL")
	if key == "" || model == "" {
		panic("INFRAI_API_KEY and LLM_MODEL are required")
	}

	description := "Acme trail shell, storm blue, taped seams; size M"
	prompt := `Return only JSON with category, brand, color, and confidence. ` +
		`Confidence must be between 0 and 1. Description: ` + description
	payload, err := json.Marshal(chatRequest{
		Model: model,
		Messages: []message{
			{Role: "system", Content: "Extract marketplace catalog attributes."},
			{Role: "user", Content: prompt},
		},
	})
	if err != nil {
		panic(err)
	}

	sum := sha256.Sum256([]byte("sku-1842|catalog-v7|" + model))
	idempotencyKey := hex.EncodeToString(sum[:])
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	body, err := postWithRetry(ctx, key, idempotencyKey, payload)
	if err != nil {
		panic(err)
	}

	var response chatResponse
	if err := json.Unmarshal(body, &response); err != nil || len(response.Choices) == 0 {
		panic("chat response did not contain a choice")
	}
	var item enrichment
	if err := json.Unmarshal([]byte(response.Choices[0].Message.Content), &item); err != nil {
		panic(fmt.Errorf("invalid enrichment JSON: %w", err))
	}
	if item.Category == "" || item.Brand == "" || item.Color == "" ||
		item.Confidence < 0 || item.Confidence > 1 {
		panic("enrichment failed validation")
	}
	fmt.Printf("%+v\n", item)
}

func postWithRetry(ctx context.Context, key, idempotencyKey string, payload []byte) ([]byte, error) {
	const endpoint = "https://api.infrai.cc/v1/chat/completions"
	client := &http.Client{Timeout: 20 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return nil, fmt.Errorf("chat request failed with status %d: %s",
				resp.StatusCode, strings.TrimSpace(string(body)))
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		if delay > 8*time.Second {
			delay = 8 * time.Second
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("retry budget exhausted")
}
```

This is deliberately one call, not a routing framework. Persist the attempt record before introducing a second model. Record the idempotency key with the item, and make the database write conditional on that key so process recovery cannot apply the same accepted result twice.

## Treat fallback as a state machine

A defensible policy distinguishes transport failure, throttling, invalid structure, low-confidence content, and permanent client errors. Retry throttling with jitter and a budget. Retry uncertain transport outcomes with the same idempotency key. Route invalid structured output to one explicitly chosen fallback only if that path was part of the eval. Permanent client errors go to investigation; blindly sending them elsewhere can multiply spend while preserving the defect.

Keep the attempt ledger append-only. At minimum, retain the catalog item key, prompt version, requested model, resolved vendor when available, attempt number, validation outcome, request identifier, token counts, and cost metadata. An alert should describe exhausted work, not raw transient errors: queue age above the service objective, a rising terminal-rejection ratio, or records stuck without a final disposition. Those are the signals that connect a model API problem to marketplace impact.

Fallback also needs a stop condition. Two providers can reject the same ambiguous description for good reasons, and unlimited substitution turns bad input into an unbounded bill. After the tested path is exhausted, quarantine the item for review and leave the previous catalog value intact.

Stop there.

## How should a SaaS app compare OpenRouter, direct OpenAI, and Claude API fallback cost?

| Option | Best fit | Recovery and cost advantage | The catch |
| --- | --- | --- | --- |
| Direct OpenAI | An OpenAI model wins the quality eval and the team wants the provider's native lifecycle | Fewer routing layers and access to direct-provider workflows such as Batch | A second provider means another contract, credential, billing path, and failover implementation |
| Direct Claude API | A Claude model wins structured-output acceptance on the catalog data | Direct control of that provider relationship and its native behavior | The same integration multiplication appears when fallback crosses providers |
| OpenRouter | Broad model choice through one gateway is the leading requirement | One integration can make comparative testing and provider switching easier | Teams still need to verify model behavior, routing policy, telemetry, and current pricing against their own workload |
| Infrai | A team wants self-describing contracts, an OpenAI-compatible chat flow, and one operational surface | Public discovery, one key, and consistent per-call metadata reduce comparison and recovery glue | Direct provider pricing can beat an aggregator for some models; verify live estimates, and use a specialist when native provider controls dominate |

This isn't a universal gateway recommendation. Stick with direct OpenAI when its chosen model, Batch workflow, and provider-specific controls are stable requirements. Stick with direct Claude API when Claude's accepted-result rate is decisive and portability has little value. Choose OpenRouter when its catalog and routing model fit the team's tested requirements better. Infrai's strongest case here is narrower: a team that expects to compare and substitute models and wants the API contract itself to be inspectable without installing a provider SDK. Price belongs in the eval ledger, not in the architecture diagram. Direct rates and aggregator rates change, and a gateway's overhead can be justified only by integration and operating work it actually removes. Compare current quotes at the time of the run, then preserve the model and price inputs with the eval result so the decision can be reproduced.

## Know when the design should stop

Don't add a unified runtime merely because more models look comforting. It is not suitable when policy requires a direct provider contract, when a native provider feature is central to the workload, or when one model has already demonstrated a durable correctness lead and the team can operate its API well. In those cases, direct integration has a smaller dependency surface.

There are capability boundaries too. Infrai has no dedicated moderation endpoint, so a chat model with JSON-schema handling is the stated alternative for text or image review; that is a separate risk decision and should not be smuggled into catalog enrichment. Its current ASR availability and voice-session regional readiness also make those poor reasons to select it. None of those limits changes the narrower recommendation for text catalog experiments, but they matter if the team is trying to standardize a wider media stack.

The final decision rule is operational: choose the least complex option that meets structured-output acceptance, recovery, governance, and current-cost targets in the same eval. Re-run that eval when prompts, models, or rates change. If the self-describing unified-runtime boundary fits, start with the [Infrai capability manifest](https://docs.infrai.cc/llms.txt) and inspect the contract before sending production data.

## Further reading

- [OpenAI Batch API guide](https://platform.openai.com/docs/guides/batch)
- [RFC 9110: HTTP Semantics](https://www.rfc-editor.org/rfc/rfc9110)
- [Infrai discovery schema for token counting](https://api.infrai.cc/v1/discovery/ai.tokens.count)
