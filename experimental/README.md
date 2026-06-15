# Experimental (NOT deployed)

These statements explore an **agentic remediation** branch — using Confluent's built-in
`AI_COMPLETE` (Gemini, via `CREATE CONNECTION` + `CREATE MODEL`) to turn efficiency events into
natural-language remediation recommendations.

They are kept here for reference but are **not part of the deployed pipeline**, and the main
README does not claim them.

## Why they're not deployed

`AI_COMPLETE` is a **non-deterministic** function. Confluent Cloud for Apache Flink only allows a
non-deterministic function over an **append-only (insert-only)** stream — it rejects it over the
**changelog (retract/upsert)** streams that the windowed ML functions and dedup queries produce
("...can not satisfy the determinism requirement for correctly processing update messages").

An append-only window aggregation (`GROUP BY window_start, window_end`) feeding `AI_COMPLETE` into
an explicit no-primary-key sink *does* reach `RUNNING`, but in our testing did not reliably
materialize output rows within the demo window (async model-inference + windowing). Rather than
ship a node that isn't demonstrably producing data, we left it out (no red/empty nodes in the
lineage).

A production version would likely use **Confluent Streaming Agents** (`AI_RUN_AGENT` / `CREATE
AGENT`), which are designed for this, or stage events to an explicitly append-only topic first.

## Files

- `06a_gemini_connection.sql` — `CREATE CONNECTION` to Google AI (Gemini). The API key is a
  placeholder; in practice the connection is created via the Confluent CLI so the key is never
  written to a file.
- `06b_gemini_model.sql` — `CREATE MODEL` (text generation) for remediation.
- `06c_remediation.sql` — `AI_COMPLETE` over an events stream.
- `08_events.sql` — a unified/idle event stream intended to feed the remediation.

No API keys or secrets are stored here.
