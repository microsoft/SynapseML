---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 11
round: 6
timestamp: "2026-11-11T05:11:39Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "503aca9035a9d85ca10c61f8da850a6ae9a8da719fb133c1f70ae6dc81e276db"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-11-review-6-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Coverage.** I read every indexed current range in files 1–31 and compared each against the removed baseline. I read the following supporting code outside the index: `HTTPTransformer.scala` 95-153, `SharedVariable.scala`, `Clients.scala`, `HTTPClients.scala` 440-480, the notebook runners (`SynapseTests`, `DatabricksUtilities`, `FabricNotebookTests`) and `LangchainTransform.py`. I did not open or search the `reviews\` paths.
- [x] **Requirement 1, retry scope.**
  - `Transport.scala:35-36` limits `throttled` to GET 429 or POST 429 that has no Operation-Location (`admissionRejected`, 21-22).
  - `Protocol.scala:325` turns retry on only when `analysis && retryThrottledRequests`, and management calls pass `analysis = false`.
  - `HasRetryThrottledRequests` defaults to false. With retry off, `Throttling.retry` (110-140) runs the attempt once.
  - `refundSince(mark)` (`Budget.scala:22-24`) runs only after a throttled attempt and its wait.
  - `BasicAsyncReply` and the `HTTPClients` handlers are not in the diff.
- [x] **Requirement 2, Retry-After parsing and fallback.**
  - `retryAfterDeltaMillis` (`ResponseParsing.scala:90-97`) clamps a BigInt result to `Long.MaxValue`. `normalizeDeltaSeconds` (`Throttling.scala:79-90`) strips padding and saturates values with more than 19 digits. `pause` calculates `delay - elapsed`, so a huge delay cannot overflow into an early retry.
  - The legacy `retryDelay` (`Protocol.scala:261-262`) now calls `retryAfterMillis` (99-104), which is the removed baseline logic moved unchanged except that `Try` became `safely`. ThrottlingPolicySuite (66, 78-91) covers these cases.
  - The fallback base, cap, doubling and jitter match the requirement (`Throttling.scala:73-107`).
- [x] **Shared stop and observability.**
  - `Context` (62-69) applies the stop only to POST.
  - `rowHandler` (661-682) creates one `Context` per handler closure. Each Spark task deserializes its own copy of the transformer and its `@transient lazy` client (`HTTPTransformer.scala:107-133`), so the "per partition" claim in the docs holds.
  - `ThrottleReporter` (`Logging.scala:15-36`) logs the first retry and then at most once every 60 s, and pause ticks count toward that limit. `throttled` (56-59) logs only the method, retry count, wait and task IDs.
  - A suppressed 429 keeps its status and body, and the original error is nested as `innererror` (`Protocol.scala:310-315`, 336-338).
- [x] **Performance.**
  - With retry off, the only added work is a boolean check.
  - The new `blocking` blocks in `exchange` (`Protocol.scala:333`) and `pause` (`Budget.scala:32`) have no effect when nested inside the existing `AsyncHTTPClient` and `SingleThreadedHTTPClient` `blocking` calls (`HTTPClients.scala:461`, 469). Effective concurrency is unchanged.
  - The 50 ms ticks and the synchronized reporter run only during throttled waits.
  - Removing the Python overrides cuts JVM round trips.
- [x] **Requirements 3–4, truncation flag and Python inspection.**
  - `HasAllowInputTruncation` (`Params.scala:142-151`) is a separate trait from the retry flag. It is omitted when unset or null, sent when false, and ignored in poll mode.
  - The `Utils.py` overrides of isSet, hasDefault, getOrDefault and extractParamMap are gone. `explainParam` (156-179) still redacts.
  - `PyCodegenSuite` 188-191 and `ContentUnderstandingSuite:639` guard against reintroducing them.
  - The `Autologging.md` caveat about JVM-backed versus pure-Python params matches `LangchainTransform.py:244/264/308`.
- [x] **Requirements 5–6, notebook and compatibility.**
  - The notebook validates config and origins before any call or write, then creates the analyzer eagerly with `allowReplace=False` and requires `ready`.
  - `modelDeployments` is set only for analysis and only when non-empty. Results are saved once with `errorifexists` and reloaded before display, with no exactly-once claim. Defaults stay 120/600000/1000, and the Spark session uses `getOrCreate`.
  - Every notebook runner excludes the notebook: Synapse 49-77, Databricks CPU filters before partitioning at 254-261, GPU/Rapids are allowlists, and Fabric uses an allowlist (230-240).
  - Public and serialized shapes are unchanged. `Settings` and `Budget` are package-private.
- [x] **Documentation.**
  - The parameter table, observability text and Retry-After text in `Content Understanding.md` (lines 500-530) match the code: cadence, cumulative retry count, per-exchange backoff reset, no URLs or bodies in logs, saturation, the RFC850 boundary and the unchanged legacy parser.
  - Notebook links and the `sidebars.js` entry follow existing conventions, and the link targets exist.
- [x] **Supplied evidence.** I relied on the reported results without rerunning them: Scala compile, test-compile and style; 208 CU Scala tests; 81 Python tests with 213 subtests; Black. I executed nothing.
- [x] **Optional suggestions, not counted as issues.**
  - `Context` could be renamed `PartitionContext`.
  - "configured wait" (docs line 518) could read "planned full wait (not the remaining time)".
  - The log line does not say whether the wait came from Retry-After or the fallback.
  - `pause` calls `onTick` twice at entry. This is harmless because the reporter rate-limits.
  - The interrupt test (`ThrottlingSuite` 187/189/194) has no timeout on `await` or `join`. In practice the stub's replies (throttled, then accepted) bound it to about 60 s; adding timeouts would make it sturdier.
- [ ] Not checked: `pipeline.yaml`, so I could not confirm CI activation. I did not test against the live Content Understanding service or packaged artifacts.