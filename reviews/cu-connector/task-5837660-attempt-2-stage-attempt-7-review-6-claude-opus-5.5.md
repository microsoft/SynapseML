---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 2
stage_attempt: 7
round: 6
timestamp: "2026-11-06T00:29:12Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "a070df7ef087b67e489406b6b9c3170daf5797f9dd75d78acab25408f82f7aba"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-2-stage-attempt-7-review-6-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Source inventory.** I used glob on `cognitive\src`. Content Understanding main and test sources exist only under `services\contentunderstanding`. The old `services\form\contentunderstanding` test tree has no stale stubs or fixtures left.
- [x] **Internal marker is removed before sending.**
  - `core\...\io\http\HTTPSchema.scala` defines the marker at 218–221 and matches its name case-insensitively.
  - `toHTTPCore` removes marker headers before the request goes out (line 209).
  - `usesFabricAuth` (169–172) needs the value `true` plus a `MwcToken` Authorization header. The connector's marker value is `content-understanding-row-auth` (`ContentUnderstandingProtocol.scala:99`), so it cannot be mistaken for Fabric auth.
  - The comment at `ContentUnderstanding.scala:335–338` explains why the existing marker channel is reused.
- [x] **Logs avoid service data.** `ContentUnderstandingLogging.scala:12–35`:
  - `safely` keeps interrupts and swallows logging errors that are not fatal.
  - Cleanup warnings log only the exception class.
  - The shared-stop warning logs a fixed category, the HTTP status, and task IDs, or `taskContext=unavailable`.
- [x] **Shared stop.** The warning is logged once per handler through `compareAndSet` (`ContentUnderstandingProtocol.scala:681`). Poll mode skips the stop check (674). The inner error keeps only allowlisted codes from bodyless responses (683–687), and the response returns HTTP 200 so the row keeps its result.
- [x] **Management logging.** `ContentUnderstanding.scala:445–467` logs outcome, elapsed seconds and exception class through `getPayload` with no exception passed in (`SynapseMLLogging.scala:106–117`). `InterruptedException` is recorded and rethrown.
- [x] **Transform runs on a fixed settings snapshot.**
  - `buildInternalTransformer` (`ContentUnderstanding.scala:400–424`) copies the transformer and validates it with the Dataset's own column-name resolver.
  - It rejects output or error columns that collide with input columns.
  - Its temporary column names are case-insensitively unique.
  - It drops the temporary columns at the end.
  - The inherited `handlingFunc` refuses to run (343–345).
  - `transform` (`66–71`) does not change SQLConf.
- [x] **Management calls.**
  - `createAnalyzer` requires an explicitly set `analyzerId`, and the definition must pass the JSON bounds before the PUT (`ContentUnderstanding.scala:472–483`).
  - `validateManagement` is driver-only and rejects column-bound settings (428–436).
- [x] **Guide matches the code.** I checked `docs\Explore Algorithms\AI Services\Content Understanding.md` lines 340–470:
  - The bounds table and the rejection of `concurrentTimeout` match `validateConfiguration`.
  - The Retry-After text (19 digits or HTTP dates, never shortened) matches.
  - DNS capacity, fresh per-operation clients, and the warning that response size is not a peak-memory limit match.
  - The retry selection uses only saved results without a handle that are `Rejected`, or `Failed` with `SharedConfigurationFailure`.
  - The exact-name and ambiguity rules (lines 222–229) match `validateParamSchema` and `inputTransformer`.
- [x] **Output parser.** `ContentUnderstanding.scala:490–525` reads the connector's own response JSON without Spark's JSON parser, which matches the guide (line 426). It registers as an internal Python/R wrapper and skips R file generation.
- [ ] **Tests not run.** This review was read-only. I rely on the validation evidence you supplied (snapshot compile, two Scala regressions, snapshot8 style/codegen/Black and 32 Python tests) and did not rerun anything.

I found no performance, logging, documentation, naming or retained-contract issue in the parts I checked. This clean result covers only this review round; it is not merge readiness, and earlier deferrals still stand.

## Driver evidence qualification

Input SHA256, runtime identity, finalized receipt and unchanged snapshot9 verified. All14 tool results were audited with no feedback exposure or scope violation. This independently clean result counts toward the final three-model scoped gate. The complete driver decision and validation provenance are appended to the GPT artifact for this same attempt.