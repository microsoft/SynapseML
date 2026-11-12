---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 6
stage_attempt: 8
round: 6
timestamp: "2026-11-11T02:29:54Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "d0e4ea6e194baef3bc03fbfcd4ab470e5cb65d2ba0d7b3e422569259144f8d98"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\pr-2815\task-5837660-attempt-6-stage-attempt-8-review-6-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Coverage.** I read every current range in all 31 indexed files (1–7 CU main Scala, 8–10 Python tests, 11–16 Scala test hunks, 17 `Utils.py`, 18 `Wrappable.scala`, 19 `PyCodegenSuite`, 20–23 nbtest, 24 guide, 25 `Autologging.md`, 26 `sidebars.js`, 27–30 new Scala files, 31 notebook) and compared them against the removed-baseline lines. I did not open `reviews\pr-2815` or any other reviewer artifact.
- [x] **Req 1 (throttled retries).**
  - In `ContentUnderstandingTransport.scala` 21–36, `throttled` requires a 429 that is either a GET or a POST with no `Operation-Location` header of any kind. The header comes from `getFirstHeader` at `Protocol.scala` 361, so even an invalid value disqualifies the retry.
  - In `Protocol.scala` 317–336, `retryEnabled = analysis && opt-in`. Management calls pass `analysis = false` (699–701, 725, 753–754).
  - `HasRetryThrottledRequests` defaults to false (`Params.scala` 153–163).
  - `refundSince(mark)` runs only on the throttled branch (`Throttling.scala` 46–75).
  - `BasicAsyncReply` is untouched.
- [x] **Req 2 (Retry-After, backoff, shared stop).**
  - `ResponseParsing.scala` 90–104: valid Retry-After values are used exactly with no cap. Zero is honored, and past dates clamp to 0.
  - `Throttling.scala` 27–44: a new `Backoff` is created for each exchange. The base is max(1000, pollingDelay) and the cap is max(60000, base). The delay doubles, with jitter in [max(base, current − current/10), current]. The first wait is exact, and a base of 60000 or more stays constant.
  - `submissionStopped` applies to POST only (16–23).
  - A suppressed 429 keeps its status and body, and the prior error is nested as `innererror` (`Protocol.scala` 310–315, 334–335).
  - `Budget.pause` ticks every 50 ms, and tick exceptions propagate (`Budget.scala` 19–55). `checkInterrupted` also honors task cancellation without a thread interrupt (`Protocol.scala` 252–259; `Clients.scala` 53–57).
- [x] **Observability.**
  - `ThrottleReporter` logs the first event, then at most one per 60 s, counting both retry and wait ticks (`Logging.scala` 15–36).
  - Messages contain only method, retryCount, waitMs, category and stage/partition/attempt IDs. They fall back to `taskContext=unavailable` (38–40, 55–67), which matches the guide's statement about calls outside Spark tasks. No URL, body or secret is logged.
- [x] **Req 3 (`allowInputTruncation`).**
  - `ContentUnderstanding.scala` 84–97: the param is active only outside poll mode, and column bindings in poll mode are filtered out.
  - 172–173: the column must be BooleanType.
  - 320–324: the query parameter is added only when set, so unset or null values are omitted and false is sent.
  - It is a separate trait (`Params.scala` 142–151) with no API-version allowlist.
- [x] **Req 4 (Python inspection).** The `isSet`/`hasDefault`/`getOrDefault`/`extractParamMap` overrides are gone from `Utils.py`. A grep of cognitive and core main `*.scala` finds no remaining Python method definitions under those names. `explainParam` redaction and the typed getters still use `_java_service_value` and `_service_param_scalar_to_python`. The `Wrappable` stub exclusion is removed consistently.
- [x] **Req 5 (notebook).**
  - Cells run in order: configuration, sample-input, create-analyzer, submit-and-save, recovery-paths, poll-and-save, display-fields.
  - Writes use `errorifexists`, and results are reloaded before display. The analyzer is created eagerly with `allowReplace=False`.
  - Config and origin are validated before any recovery call or write. `modelDeployments` is applied only to analysis and omitted when empty.
  - There is no exactly-once claim.
- [x] **Req 6 (compatibility and unattended exclusions).**
  - `execute` gained a defaulted parameter, but it lives inside `private[contentunderstanding] object ContentUnderstandingProtocol`.
  - New params have defaults or stay unset, so the serialized shape is preserved.
  - The new notebook is excluded from Synapse (`SynapseTests.scala` 49–69) and Databricks CPU (`DatabricksUtilities.scala` 254–261). The `FabricNotebookTests.scala` 226–240 allow-list (outside the diff) does not include it, and the GPU/Rapids filters (280–286) do not match it.
- [x] **Documentation accuracy.**
  - Param docs (`Params.scala` 107–123, 153–181) match the guide's opt-in, bounds and Retry-After sections.
  - The guide links at lines 42 and 665 use the repo's extensionless `../Quickstart%20-%20…` style under `trailingSlash: true` (`docusaurus.config.js` 17), the same as `Causal Inference\Overview.md:61` and `Deep Learning\ONNX.md:268`.
  - The notebook links (lines 22, 452) point to files that exist.
  - The pure-Python `LangchainTransformer` caveat in `Autologging.md` is accurate (`LangchainTransform.py` 244, 264, 308).
- [x] **Performance.** Each 50 ms tick does one atomic read plus a synchronized clock check, and logging is rate-limited. There is no global throttling and no new execution timeout. A service that keeps sending `Retry-After: 0` gets back-to-back retries, but that follows directly from the requirement to honor the header exactly, so I did not count it as a defect.
- [ ] **Not run or reproduced.** I executed no build, test, codegen, Black, or live Content Understanding call. The supplied validation (compile/style, byte-identical codegen, 40 codegen + 201 CU Scala tests, 99 Python + 250 subtests, 24 notebook tests, 7 live skips) was reviewed as reported, not reproduced.
- [ ] **Optional suggestions, not counted as issues.**
  - During one long wait, the once-a-minute log line repeats the same retryCount and waitMs. Adding a waiting/retrying phase or elapsed time would make it clearer.
  - `ContentUnderstandingThrottling.Context` also holds the stop for shared authentication, analyzer or deployment failures, which are not throttling. Its doc comment explains this.