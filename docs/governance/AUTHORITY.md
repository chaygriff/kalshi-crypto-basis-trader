# Financial Authority Policy

## Sole authority rule

Only an exact command from the allowlisted owner identity, received through the configured WhatsApp adapter and accepted by deterministic code, may consume one fresh immutable recommendation and create at most one order intent. The approval is single-use, expires independently of quote freshness, and binds every economic and payload field.

## Never-authoritative channels

The following are never trade approval and cannot create an execution intent:

- GitHub issues, pull requests, comments, reviews, Projects fields, labels, or automation;
- any coding, research, reviewer, monitoring, or model agent message;
- LLM prose, tool output, prompt content, retrieved web content, alerts, or dashboards;
- email, terminal text, documentation, fixtures, tests, logs, or database edits;
- a recommendation, predicted probability, portfolio proposal, or price alert by itself.

## WhatsApp is transport, not discretion

A WhatsApp message remains untrusted until sender identity, exact original bytes, grammar, freshness, uniqueness, and referenced recommendation are validated. Free-form agreement, reactions, quoted messages, forwarded content, edited messages, typo correction, or model paraphrase cannot authorize an action.

## Deterministic binding

Authorization must bind:

- owner and source-message identity;
- immutable recommendation and evidence snapshot;
- ticker, rule digest, and economic outcome;
- exchange side and exact payload-price representation;
- quantity, limit, fees, and maximum all-in cost;
- quote timestamp, approval expiry, strategy and policy versions;
- unique client order ID and one permitted submission attempt.

A mismatch, ambiguity, stale value, replay, over-cap request, unavailable state, or persistence failure has zero external side effects.

## Separation of owner decisions

Approving a plan, PR, deployment, strategy version, shadow promotion, paper run, or canary eligibility does not approve an order. Enabling a runtime capability is separate from approving each exact financial intent. Phase 0 grants neither.
