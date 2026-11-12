---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 3
round: 1
timestamp: "2026-11-10T21:02:56Z"
outcome: "failed"
reviewer_model: "claude-opus-5.5"
input_sha256: "5438212dbb7f2a40950cc23be10e82a974c56f40e9f4c86f9ab91ecc72d8c761"
---

## Runner Failure

Invalid CLI event on line 10: Expecting ',' delimiter: line 1 column 42250 (char 42249)
Invalid CLI event on line 517: Expecting ',' delimiter: line 1 column 7695 (char 7694)

## Original Response

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-3-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Which requests are retried** (`ContentUnderstandingProtocol.scala` L327–346, L368). Retries happen only when `analysisRetry && settings.retryThrottledRequests` is true.
  - A GET retries on any 429.
  - A POST retries only when `admissionRejected(POST, 429, location)` holds and `location` is empty. `location` is the raw `Operation-Location` header, so any handle, valid or invalid, blocks replay.
  - Transport failures, 408s, 5xx responses and header-parse failures are not 429s, so they are never replayed.
  - A complete POST 429 whose body can't be read or is too large keeps status 429 with no location. It is treated as a definitive rejection and retried, which matches the guide (L408–417).
- [x] **Call sites.**
  - Submission in `execute` passes `analysisRetry = true` (L638), covering both analyze and submit.
  - Polling passes `analysisRetry = analyzer.isEmpty` (L612–613), so analyzer-creation polling stays excluded.
  - `getAnalyzerResponse` and the `createAnalyzer` PUT keep the default `false`.
  - `ContentUnderstandingConcurrencySuite` L193–221 asserts the management budget is unchanged with the flag on and off.
- [x] **Fallback-only backoff** (`ContentUnderstandingThrottling.scala` L14–31).
  - A valid header returns its exact value and resets growth. That includes 0, past dates (clamped to 0) and huge values.
  - With a missing or invalid header, the base is `max(1000, pollingDelay)` and the cap is `max(60000, base)`.
  - Jitter is at most 10% downward and never drops below base. The `nextLong(lower, current + 1)` bounds are always valid, and there is no overflow.
  - A new `Backoff` is created on each `retry` call (L38), so it resets per exchange.
  - By design, the first fallback wait is exactly base. This is documented and is not a defect.
- [x] **Budget accounting** (`ContentUnderstandingThrottling.scala` L39–52; `ContentUnderstandingBudget.scala` L14–49).
  - Each 429 attempt plus its wait is refunded through `mark`/`refundSince`. Time charged before the mark stays charged, and the final non-429 attempt is charged.
  - `maxPollAttempts` counts one `exchange` call no matter how many 429s it absorbed.
  - `pause` checks for cancellation in 50 ms slices and doesn't overflow at `Long.MaxValue`.
  - `checkInterrupted` (L269–276) uses `AsyncClient.isTaskCancelled` (`Clients.scala` L53–57), which carries the owning task's context onto async workers.
- [x] **Retry-After parsing** (`ContentUnderstandingResponseParsing.scala` L91–105). Delta seconds are accepted up to 19 digits, with a BigInt clamp to `Long.MaxValue`. RFC 1123 dates are clamped at 0. The `retryDelay` refactor (`ContentUnderstandingProtocol.scala` L278–279) keeps the management fallback to `pollingDelay`.
- [x] **Throttling logs** (`ContentUnderstandingLogging.scala` L25–30).
  - They contain only the method, retry count, wait and task IDs, and fire on retries 1, 2, 4, 8 (Throttling L45–47).
  - No URLs, credentials, headers or bodies are logged.
  - Async workers log "taskContext=unavailable", which matches the documented convention (guide L503–508).
- [x] **Notifying the execution pool** (`ContentUnderstandingBudget.scala`, `ContentUnderstandingProtocol.scala` L343).
  - The new `blocking` calls are local to CU. The shared `HTTPClients.scala` L459–472 already wraps the handler in `blocking`, so the nested call does nothing and causes no harm.
  - Shared HTTP code is unchanged.
- [x] **`allowInputTruncation`** (`ContentUnderstandingParams.scala` L139–148: `ServiceParam[Boolean]`, `isURLParam = true`, scalar and column setters).
  - In `ContentUnderstanding.scala` it is included in active params (L89), typed as BooleanType (L171–172), validated as a Boolean (L189), and added to the query with `value.toString` (L317–323).
  - When unset or null it is omitted, and an explicit false is sent. There is no version allowlist, and poll mode ignores it.
  - Covered by `ContentUnderstandingOptionsSuite` and Python `test_ContentUnderstanding.py` L171–228, including copy/save/load.
- [x] **Opt-in default.** `retryThrottledRequests -> false` (`ContentUnderstandingParams.scala` L175). `ContentUnderstandingLimits` is mixed in only through `ContentUnderstandingParams` (L12–13). `Settings` is wired at `ContentUnderstanding.scala` L147.
- [x] **Python inspection** (`core/src/main/python/synapse/ml/core/schema/Utils.py`).
  - Only the generic `isSet`, `hasDefault`, `getOrDefault` and `extractParamMap` overrides were removed.
  - Generated typed setters and constructor kwargs go to the JVM and remove the value from `_paramMap` (`Wrappable.scala` L209–228, L516–522).
  - Copy, save and load still work through the transfer methods.
  - The real-MLflow test (L230–310) asserts that `concurrency` is logged, credentials are not logged, and execution still uses the key or token.
- [x] **Docs.**
  - The guide explains parameter logging versus credential-bearing model artifacts (L134–160) and matches `Autologging.md` L12–23.
  - The throttling section (L375–399) and the parameter table (L488–497) match the implementation.
  - Links follow the repository's existing sibling-notebook convention.
- [x] **Notebook** (cells at L76–100 and L330–361).
  - `resource_origin` rejects user info and non-HTTP(S) schemes.
  - The origin preflight checks only the pending, non-null handles, before `poller.transform` and before the `errorifexists` write.
  - Recovery doesn't depend on the analyzer or deployment settings.
  - The notebook test (`test_ContentUnderstanding.py` ~L654–760) shows a mismatched origin produces no write and no requests, one POST in total, and no requests from an already-settled snapshot.
- [x] **Notebook exclusions.** The generated name contains the Synapse exclusion token, the Databricks CPU filter matches the title, and the Fabric include-list excludes the notebook by default.
- [ ] **Execution:** not applicable. I ran no builds, tests or live CU calls, as the review plan forbids. The driver-supplied results were not reproduced:
  - CU: 188 of 189 passed on the broad run. The 38/38 RecoverySuite pass is from a separate rerun after the reflection test was updated, not one all-green invocation.
  - Python: 96 passed, 7 live tests skipped.
  - Black passed.

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "Primary estimate; no human or repository tier override",
    "models": "Active review-code Medium complementary-family rule, resolved from the current task model catalog",
    "stageBehavior": "Six concurrent independent lenses; human requested review after the fixes",
    "safetyStop": "No new timeout or budget; no safety controls disabled",
    "delivery": "Human iteration-mode instruction: no commit or push without explicit approval"
  },
  "changeSize": "Twenty-six source, test and documentation files relative to PR head. Most volume is tests and documentation; production changes are localized to CU request options, throttling and time accounting, and shared Python parameter inspection.",
  "cyclomaticComplexity": "Moderate: definitive-admission retry eligibility, cancellation, fallback-only backoff, elapsed-budget refunds, credential inspection and durable notebook recovery interact with existing state machines.",
  "rationale": "Six complementary lenses cover the localized production changes and cross-connector inspection implications. The user explicitly requested another review after fixing the recovered findings. Retain all earlier failed evidence; a transport failure cannot count as signoff.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "overrideSource": "User: when the fixes are in please start review after this. User selected fallback-only adaptive backoff and HasAllowInputTruncation. Existing iteration-mode instruction overrides automatic commit/PR.",
  "reviewAttempt": 6,
  "stageAttempt": 3,
  "sourceHead": "41b54ec47d514aac82494c70ed2963a8bc69ff5d",
  "target": "master",
  "artifactDirectory": "reviews/pr-2815",
  "uncertainty": "The CLI JSON transport failure may recur. Offline evidence does not establish live CU compatibility or approval of matching packaged Python/JVM artifacts."
}

## Driver collection outcome

**BLOCKED, not clean signoff.** All six Medium-tier CLI reviewers completed and
all six owning runners persisted their artifacts, but each runner exited 1
because CLI JSON events were malformed. Each child CLI exited 0; that does not
override the runner failure. Runtime model identity was `claude-opus-5.5` for
every lens, with requested reasoning `max`. The effective decision is preserved
above. No masking bypass, event repair, runner modification or automatic retry
was performed.

| Lens | Readable response | Findings | Owning runner |
|---|---|---|---|
| 1: Broad sweep | CLEAN | 0 | Failed transport; persisted |
| 2: Architecture | ISSUES_FOUND | 5 low | Failed transport; persisted |
| 3: Robustness | ISSUES_FOUND | 5 low | Failed transport; persisted |
| 4: Detailed correctness | CLEAN | 0 | Failed transport; persisted |
| 5: Tests | ISSUES_FOUND | 2 medium, 3 low | Failed transport; persisted |
| 6: Polish | ISSUES_FOUND | 4 low | Failed transport; persisted |

These are recovered reviewer reports, not accepted independent signoffs.
Lens 2 additionally disclosed that a root-level grep returned prior-review
artifact text. Its independence is compromised even apart from the transport
failure. The other results are not certified independent through the invalid
transport. All original responses, diagnostics and earlier attempts are retained.

### Remaining reported findings

There are 19 reported findings: 2 medium and 17 low, including duplicates. No
high or critical findings were reported. They remain open for triage; no source
changes were made after the review snapshot was captured.

The medium findings are both test-quality gaps, not demonstrated runtime bugs:

1. The throttled Spark cancellation subtest observes driver cancellation and
   absence of new requests, but does not directly prove that executor-side
   waiting terminates promptly with `interruptOnCancel=false` (L5 I1; also L3 I5).
2. Opt-in coverage does not explicitly pin POST 401/403 and GET 503 behavior
   against accidental non-429 retry/refund changes (L5 I2).

Low findings, grouped without discarding duplicates:

- Async throttle logging does not use the available owning-task helper
  (L2 I2, L3 I2, L6 I2).
- First fallback and base-equals-cap retries have no jitter (L2 I5, L3 I1,
  L6 I1). L1 and L4 explicitly accepted this as intentional. Changing it would
  alter the recorded fallback policy; do not silently treat that as settled.
- A retrying rejected POST does not observe a sibling's partition shared-stop
  flag (L3 I3).
- Per-exchange warnings can be numerous across a batch (L3 I4), while a single
  very long retry sequence can have hours of silence (L6 I3). These suggestions
  pull in different directions and require a coherent logging policy.
- Request-kind flags/private reflection, retry-parameter trait grouping and
  ServiceParam query conversion are architecture suggestions (L2 I1/I3/I4).
  Trait regrouping was already deliberately declined in the prior resolution.
- Exact public-path Retry-After wiring, per-exchange reset, and endpoint
  validation/default-port branches have additional coverage suggestions
  (L5 I3/I4/I5).
- The notebook describes model-deployment mappings as for analyzer creation,
  although the parameter is sent on analysis submission (L6 I4).

The suggested new test timeouts are not approved: the user's no-new-timeouts
instruction remains in force. Any follow-up should preserve exact valid
Retry-After handling and the approved public trait name.

### Snapshot and validation

All 26 source-file SHA-256 values still matched the captured manifest after
collection. The shared diff hash remains
`48d111d93f005b4eccbba64e99914e47a404919e477fcd607926f394c42bc072`,
on HEAD `41b54ec47d514aac82494c70ed2963a8bc69ff5d`.

The targeted offline evidence supplied before dispatch remains unchanged:
66 shared Scala tests and 1 notebook selector passed; CU passed 188/189 in the
broad run followed by 38/38 in the corrected RecoverySuite; Python passed
96 tests and 241 subtests with 7 live tests skipped. Compile/style/codegen,
Black, notebook cell/schema/output checks and sidebar syntax passed.
The earlier broader Bash/WSL and Black-extra TLS limitations still apply.

No commit, push, ADO edit or live CU call was made. These artifacts remain with
the uncommitted changes for the user's later explicit landing instruction.

## Driver resolution update before stage attempt 4

The user requested an autonomous fix/review loop. All original outcomes above
remain failed transport, not signoff. Copilot CLI was updated through the
supported updater to 1.0.109; a synthetic serialization probe produced 30 valid
JSON events and a terminal result. No masking, runner validation, or prior
events were changed. The next complete runner batch must prove transport.

Recovered findings were addressed in the working tree: one analysis flag and a
direct package test seam replace the two-flag/reflection coupling; retry opt-in
has its own trait; query encoding uses ServiceParam.toValueString; diagnostics
resolve owning async Spark task IDs and share a minute-rate-limited partition
reporter with long-wait heartbeats. Shared-stop now interrupts rejected POST
retry waits while accepted-handle GET retries continue. Backoff remains
per-exchange and the approved deterministic first/base-at-cap cases are now
explicitly documented. Notebook mapping documentation now describes analysis
submission rather than analyzer creation.

Regressions cover exact forwarded waits, backoff reset between POST and GET,
unchanged opt-in 401/403/503 behavior, shared-stop and accepted GET behavior,
reporter cadence/heartbeats, async task IDs, endpoint validation/default ports,
and executor-task drain within the cancellation test's existing ten-second
window. No new execution timeout was added.

Aggregate compile/style/codegen passed. Broad CU validation passed 191/193;
the nested test registration and incorrect 404 expected status were corrected.
Both affected suites passed 17/17, covering all 194 distinct CU cases across
the runs. Notebook schema, seven Python cells, empty outputs, sidebar syntax,
and diff hygiene passed. Python regression collection is still running at this
update; the next review will not dispatch until it succeeds.

Python subsequently passed: 96 tests and 248 subtests, seven live tests skipped.
The cancellation test confirmed executor-task drain within its existing window.
Stage attempt 4 is authorized to dispatch against a fresh immutable snapshot.
