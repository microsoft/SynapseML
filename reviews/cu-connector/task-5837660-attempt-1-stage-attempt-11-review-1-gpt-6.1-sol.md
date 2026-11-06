---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 11
round: 1
timestamp: "2026-11-04T17:15:34Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "1cf9df9a0087beda1a45682a6d17d6543c5d7097601c8b38228796fcd5063bf1"
---

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: copilot-cli
attempt: 1
stage_attempt: 11
round: 1
timestamp: "2026-11-04T17:01:28.796Z"
outcome: issues_found
reviewer_model: gpt-6.1-sol
input_sha256: 2dc8d691af10046d826005022ba79295805216cbd99a60b92d85fea1da97e753
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-11-review-1-gpt-6.1-sol.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Traced submission, polling, management, authentication, and handle retention through `ContentUnderstanding.scala`, `ContentUnderstandingProtocol.scala`, and `CognitiveServiceBase.scala`.
- [x] Inspected `ContentUnderstandingDnsResolver.scala` and the recovery/concurrency suites, including physical worker bounds, admission recovery, and stalled-DNS handling.
- [x] Reviewed `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala`, shared Python `Utils.py` and `java_params_patch.py`, and `cognitive\src\test\python\synapsemltest\services\test_ContentUnderstanding.py`.
- [x] Traced internal-column allocation through `DatasetExtensions.scala`, `SimpleHTTPTransformer.scala`, and `Parsers.scala`; inspected session-resolver tests, connector documentation, and the approved `pipeline.yaml` entry.
- [x] Considered the supplied passing local validation evidence without treating it as live-service or external-CI evidence.
- [ ] No commands or tests were executed in this read-only review; the findings below are based on static analysis.

## Issues

### Issue 1: JVM-backed `isSet` breaks inherited Python parameter inspection
- **Severity**: Medium
- **File**: core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala
- **Line(s)**: 83–89, 205–212
- **Description**: The generated `isSet` now returns true for service parameters stored only on the JVM, while generated setters explicitly remove their Python `_paramMap` entries. PySpark's inherited `Params.explainParam` indexes `_paramMap[param]` when `isSet(param)` is true. Consequently, a configuration-only call such as `ContentUnderstanding().setAnalyzerId("prebuilt-read").explainParams()` raises `KeyError`. Neither `ComplexParamsMixin` nor `java_params_patch.py` bridges this inherited reader.
- **Risk**: Standard SparkML parameter inspection fails for CU and other generated service wrappers, including consumers that inspect parameters for diagnostics or integration.
- **Suggested Fix**: Make inherited parameter inspection compatible with JVM-backed service parameters rather than overriding only `isSet`. Preserve scalar/column distinctions and stale-value protections. Add focused `explainParam`/`explainParams` regressions for CU and an existing service wrapper after setters and parameter transfer.

### Issue 2: DNS deadline expiration loses the definite pre-send classification
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingDnsResolver.scala
- **Line(s)**: 13–14, 55–65, 79–82
- **Description**: A blocked DNS lookup that exhausts its budget throws `OperationDeadlineExceeded`, carrying only the generic `OperationTimedOut` diagnostic. `exchangeBeforeDeadline` records this as a transport failure, and `dnsFailedBeforeSend` in `ContentUnderstandingProtocol.scala:543–559` recognizes only `DnsResolutionFailed` and `DnsCapacityExceeded`. A submit operation therefore returns `Unknown`, indistinguishable from a timeout after a possibly accepted POST, even though DNS never returned and no HTTP request could have been sent.
- **Risk**: Callers cannot distinguish this safely resubmittable, unsent work from ambiguous accepted work, contrary to the required DNS failure distinction. Workflows selecting saved `Rejected` rows leave these documents unresolved.
- **Suggested Fix**: Preserve an explicit pre-send DNS-timeout classification and return `Rejected` for submissions while retaining poll handles and appropriate timeout diagnostics in poll mode. Keep permits held until physical DNS completion. Extend the stalled-DNS regression to exercise submit mode and verify zero connections and distinguishable diagnostics.

### Issue 3: Nested HTTP temporary columns still use the wrong session resolver
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala
- **Line(s)**: 59–63, 376–385
- **Description**: The input Dataset's resolver is captured for the outer CU pipeline, but it does not reach `SimpleHTTPTransformer.makePipeline`. That transformer allocates `parsedInput` and `unparsedOutput` through the overload using ambient `SQLConf.get.resolver` (`SimpleHTTPTransformer.scala:119–120`, `DatasetExtensions.scala:51–52`). With a case-insensitive input Dataset and a different case-sensitive active session, an existing `PARSEDINPUT` column is not recognized as a collision. `CustomInputParser.withColumn("parsedInput", ...)` replaces it under the Dataset's resolver, and subsequent cleanup drops it. The existing cross-session regression covers the opposite direction only.
- **Risk**: A supported multi-session transformation can silently discard an input column, violating the connector's input-preservation contract.
- **Suggested Fix**: Carry the Dataset resolver through nested HTTP temporary-column allocation, or conservatively reserve those internal names case-insensitively without changing shared Spark settings. Add the reverse-direction cross-session regression with `PARSEDINPUT` and `UNPARSEDOUTPUT`, asserting preserved names and values.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Inherited Python parameter inspection remains inconsistent with JVM-only state.
- **How verified**: Static inspection; regression not executed.

### Issue 2
- **Status**: Open
- **What changed**: Pending.
- **Why**: DNS deadline failures remain indistinguishable from ambiguous submission timeouts.
- **How verified**: Static trace through resolver exceptions and submission classification; regression not executed.

### Issue 3
- **Status**: Open
- **What changed**: Pending.
- **Why**: Nested HTTP allocation still consults the active session rather than the input Dataset.
- **How verified**: Static trace through allocation, replacement, and cleanup; regression not executed.

## Driver follow-up: attempt 11

The original review and verdict above are preserved. Fixes are implemented; post-fix validation is pending.

- **Issue 1:** Corrected the reported symptom against the installed PySpark 3.5 implementation: inspection omitted JVM-backed values, rather than raising `KeyError`. The offline regression failed because `explainParam(subscriptionKey)` returned `()` instead of the configured synthetic value. `ComplexParamsMixin.explainParam` now delegates JVM-only ServiceParams to JVM inspection and retains ordinary Python handling for pending Python values. Coverage exercises both CU and an existing AnalyzeDocument wrapper.
- **Issue 2:** Submit and analyze stalled-DNS regressions returned `Unknown` before the fix; poll behavior passed. `WireResponse.notSent` now explicitly records pre-execute budget expiry and DNS failures, and submission decoding returns `Rejected`, using `RequestNotSent` for a pre-send deadline. Polling still retains its handle. Actual DNS failure diagnostics survive simultaneous deadline expiry. DNS permits remain held until the physical lookup ends.
- **Issue 3:** The reverse-session regression reproduced ambiguous `unparsedOutput`, not the specific silent-loss symptom claimed above. Nested HTTP temporary names now conservatively avoid case-insensitive collisions, including configured output/error names, without mutating SQLConf. Both session directions assert preserved input names and values.
- **Red evidence:** `cu-attempt11-scala-red.log`: 3 failures in 4 selected tests. `cu-attempt11-python-red2.log`: 2 failures in 15 tests, including the separately reviewed loaded-column corruption.
- **Validation in progress:** Initial style run passed core main/test and cognitive main checks, then found the expanded DNS helper exceeded the method-length limit. The helper was split without suppressing the rule. `cu-attempt11-fixes-validation2` is rerunning style, compilation, shared HTTP/CU suites, codegen and local publication. Full Black passed with 205 files unchanged. Generated Python/JAR verification remains pending.

### Effective review decision and human direction

- **Tier/source:** Max, direct contract, explicitly selected by the driving human; stage 1, attempt 11.
- **Size/complexity:** Broad, 27 reviewed diff paths across runtime, codegen, Python transfer, tests, documentation and CI selection. High interaction complexity from concurrency, transport failures, Spark execution, credential selection and JVM/Python state.
- **Primary/reviewers:** Primary `gpt-6-astra`; live-catalog selections `gpt-6.1-sol` at `max`, `grok-4.7` at `low` under the human override, and `claude-opus-5.5` at `max`. Runtime receipts identified those same models.
- **Gate:** All three returned persisted `issues_found`; stage 1 remains dirty. Unavailable Grok may be excluded only under the retained human exception when GPT and Opus clear. Available Grok findings count. No safety controls or existing limits were changed.
- **Independence:** Audited GPT/Grok/Opus transport tool calls (68/44/127): read-only view/search/glob calls, with no prior/sibling feedback-path queries. Opus reported prompt truncation and incorrectly described the frozen patch location; its dirty result is not comprehensive signoff.
- **Latest human direction:** Keep existing SynapseML concurrency and align timeout behavior with comparable operations. No executor-wide operation cap was approved or added. Shared per-partition concurrency and HTTP timeouts remain; the CU total budget is best-effort, with request-phase timeouts capped by remaining budget and no join/reset of an already-started abort. Accepted-handle preservation and no automatic ambiguous POST replay remain requirements.
- **External limits:** No live CU, remote CI, commit, push or ADO writes were performed. Fresh target verification remains unavailable; no merge-ready claim is made.

### Post-fix validation completed

- `cu-attempt11-fixes-validation2.log`: cognitive test scalastyle, compilation/test compilation, 7/7 shared SimpleHTTPTransformer tests and 82/82 CU main/recovery/concurrency tests passed. Earlier core main/test and cognitive main style checks passed on the same production sources.
- Codegen and local core/cognitive publication succeeded. Project-scoped documentation-artifact settings avoided the unnecessary Javadoc work without changing repository build configuration.
- `cu-attempt11-python-fixed.log`: 15/15 offline tests passed, including inspection, loaded non-CU column bindings, copy/save/load, explicit analyzer calls and persisted-handle polling. The generated shared `Utils.py` matched its canonical source byte-for-byte.
- Actual loaded JAR SHA256s matched rebuilt artifacts: core `df7f14cd24fb7c5bf630a502d57c3d477b5a9ba1f81f708b8881e59c95f034d1`; cognitive `de20088725a5ad07a1de320054286cc41a012e702a49c05ad5f4a3b8ba2174e5`. Only the four stale snapshot cache JARs were refreshed; published local artifacts were retained.
- Issues 1-3 are locally addressed with the corrected symptoms documented above. Original reviewer verdict remains `issues_found`; stage advancement awaits the next independent review attempt.