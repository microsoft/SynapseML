---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 9
round: 1
timestamp: "2026-11-04T08:28:37Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "8dff0f47a81228427fd26833c83f384cb81bfa800d7833889e0934163efcfcbd"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-9-review-1-grok-4.7.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the supplied diff for ContentUnderstanding transform/protocol, header validation, DNS deadline handling, Python codegen, and docs/tests.
- [x] Checked `ContentUnderstanding.scala` transform (SQLConf mutation) and `ContentUnderstandingProtocol.scala` deadline executor against the lazy DataFrame path.
- [x] Checked `HTTPSchema.scala` marker stripping, `HTTPTransformer.scala` unset `concurrentTimeout` (`Duration.Inf`), and `Wrappable.scala` ServiceParam setters.
- [x] Checked `ContentUnderstandingDnsResolver.scala` cached-pool timeout behavior and `ServiceAuthHeaders.resolve` precedence against row-auth classification.
- [ ] Did not execute tests or compute `input_sha256`; this pass is read-only and the prompt forbids commands.

## Issues

### Issue 1: Transform mutates the active Spark SQLConf
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstanding.scala
- **Line(s)**: 59-72
- **Description**: `transform` reads `SQLConf.get` and toggles `CASE_SENSITIVE` for the whole active session, then restores the previous value in `finally`. That object is shared by other queries on the session. The toggle also cannot protect later analysis: the returned DataFrame is lazy, and the conf is restored before an action runs. Collision checks already use `DatasetExtensions.resolverFor(dataset)`.
- **Risk**: Concurrent jobs can analyze or resolve columns under the wrong case-sensitivity setting. A concurrent conf change can also be overwritten by the restore. Cross-session case handling is not actually fixed at action time.
- **Suggested Fix**: Do not write `SQLConf`. Keep using the dataset session resolver for unused-name and output/error checks. If Spark analysis must follow that session, build and analyze the plan with that session active instead of mutating the caller’s conf.

### Issue 2: Generated ServiceParam setters drop Python param state
- **Severity**: Medium
- **File**: core/src/main/scala/com/microsoft/azure/synapse/ml/codegen/Wrappable.scala
- **Line(s)**: 201-211
- **Description**: Every generated `setX` / `setXCol` now does `self._paramMap.pop(...)` after the JVM call and does not `_set` the new value. The same pattern is duplicated for `documentBytes`. Getters that call the JVM still work, but PySpark `isSet`, `copy(extra)`, and `_transfer_params_to_java` only see `_paramMap`.
- **Risk**: Repo-wide, not CU-only. A value set in Python can disappear from the Python stage after the setter, so a later copy or transfer can omit it or disagree with the JVM object. The pop correctly clears a stale map entry, but it also discards the value just set.
- **Suggested Fix**: After a successful JVM set, record the new value with `_set` (or equivalent) instead of only popping. Keep the pop-before-set only to remove a stale opposite scalar/column entry.

### Issue 3: Deadline and DNS control can exhaust executor threads
- **Severity**: Medium
- **File**: cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/contentunderstanding/ContentUnderstandingDnsResolver.scala
- **Line(s)**: 30-66
- **Description**: `BudgetedDnsResolver` submits each lookup to an unbounded cached pool and only cancels the `Future` on timeout. JVM DNS lookups usually ignore interruption, so timed-out lookups keep a thread until the resolver returns. `DeadlineExecutor` in `ContentUnderstandingProtocol.scala` (lines 98-110) is a single thread for every in-flight abort, so aborts queue behind one another.
- **Risk**: Many partitions hitting slow DNS can accumulate daemon threads for the life of the executor and delay operation-budget aborts past `maxOperationDuration`. `RejectedExecutionException` handling never runs on an unbounded pool.
- **Suggested Fix**: Cap DNS workers and fail with the existing safe DNS diagnostic when saturated. Abort deadlines from the calling task or a pool sized for configured concurrency, and do not treat `cancel(true)` as proof that a lookup has stopped.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Resolved
- **What changed**: Removed shared `SQLConf` mutation; temporary column names are checked case-insensitively and error columns are assembled before output-column renaming.
- **Why**: Dataset-specific resolution must not mutate another workload's shared session configuration.
- **How verified**: Cross-session resolver and case-variant schema regressions passed in `ContentUnderstandingRecoverySuite` (34/34) and `ContentUnderstandingSuite` (40/40).

### Issue 2
- **Status**: Resolved
- **What changed**: Generated ServiceParam `isSet` now consults JVM parameter state, without copying service values into Python `_paramMap`.
- **Why**: Preserve JVM-backed parameter semantics for `isSet` and copying while avoiding a second Python-side copy of sensitive service values.
- **How verified**: Code generation and Scala generated-source assertions passed; the added Python runtime regression has not yet been executed.

### Issue 3
- **Status**: Resolved
- **What changed**: DNS admission is bounded; deadline-abort scheduling now uses four workers rather than one.
- **Why**: Slow DNS and serialized aborts must not cause unbounded worker growth or delay independent operation deadlines.
- **How verified**: `ContentUnderstandingConcurrencySuite` passed 2/2, covering DNS saturation and independent deadline aborts.