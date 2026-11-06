---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 2
stage_attempt: 5
round: 6
timestamp: "2026-11-05T23:48:38Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "923ff3b45a9db0e0e1788df1ad9c0c1520aab42b932d7b2bfda10d8e92f51fbe"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-2-stage-attempt-5-review-6-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed HTTP execution, admission classification, retained polling state, deadlines, bounded DNS capacity, and Retry-After parsing in `ContentUnderstandingProtocol.scala`, `ContentUnderstandingDnsResolver.scala`, and `ContentUnderstandingResponseParsing.scala`.
- [x] Reviewed sanitized management/shared-stop logging and exception summaries against the assertions in `ContentUnderstandingSubmissionSuite.scala:39-156,271-308`.
- [x] Traced Python pending-parameter transfer through `ContentUnderstandingParams.scala:185-199,244-251`, `core\src\main\python\synapse\ml\core\schema\Utils.py`, and the generated ServiceParam definitions in `Wrappable.scala`.
- [x] Compared recovery, credential-persistence, concurrency, and management documentation in `docs\Explore Algorithms\AI Services\Content Understanding.md` with implementation and supplied tests.
- [x] Reviewed supplied validation evidence: five exact-snapshot Retry-After regressions passed; the 165-test result belongs to the preceding snapshot. No tests or commands were executed during this review.
- [ ] Live-service compatibility, current publication/runtime integration, and external CI were not verified; they remain outside this independent pass.

## Issues

### Issue 1: Failed pending-parameter transfer silently restores stale JVM configuration
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingParams.scala
- **Line(s)**: 244-251; related setter at 185-192
- **Description**: `_transfer_params_to_java` removes a pending ServiceParam before its setter succeeds and silently discards `None`. For example, configure valid scalar document bytes, then call `stage.set(stage.documentBytes, "body")`. The next transform raises the dedicated setter’s `TypeError`, but the pending override has already disappeared. A subsequent transform without correcting the parameter uses the old JVM bytes. Setting a pending value to `None` similarly restores the previous JVM scalar or column binding without reporting an error.
- **Risk**: Retrying after a configuration failure can submit different documents or use stale credentials without an explicit corrective setter. This violates the retained unified Python/JVM parameter lifecycle and invalid-input hardening contract.
- **Suggested Fix**: Read pending values without removing them; let dedicated setters invalidate entries only after successful JVM assignment. Reject unsupported pending `None` explicitly rather than skipping it. Add focused regressions proving a failed transfer remains pending and prevents later dispatch until corrected, and that `None` cannot silently reactivate stale configuration.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Pending overrides must not disappear before successful transfer or explicit correction.
- **How verified**: Static tracing of generated setters, parameter definitions, and transfer ordering; runtime verification remains pending.

## Driver resolution and effective decision

Original reviewer output above is preserved. The effective tier was human-selected High/direct, stage6, review attempt2/stage attempt5, with human-selected GPT/Opus high and Grok low reasoning. The clean-only stage gate overrides High's usual dirty-second-attempt advancement for this scoped polish loop. Primary model: gpt-6-astra. The broad39-path contribution has high qualitative complexity across HTTP concurrency, deadlines, credentials and Python/JVM persistence. Existing outer controls and deferred scope remain unchanged.

| Runtime reviewer | Result | Seconds | Independent |
| --- | --- | --- | --- |
| gpt-6.1-sol | 1 Medium | 171.596 | Yes |
| grok-4.7 | CLEAN | 144.493 | No: a root-wide search exposed prior feedback filenames |
| claude-opus-5.5 | 1 Low | 185.345 | Yes |

Input hashes are retained in each artifact's frontmatter. Driver audit checked saved inputs, finalized runtime identities, actual tool arguments/results and unchanged snapshot7. GPT/Opus did not access feedback. Grok's original CLEAN is preserved but cannot count as independent signoff.

**Issue1 resolved locally:** CU transfer now reads a pending value without popping it; unsupported pending None raises TypeError. Existing dedicated setters remain responsible for removing the override after successful JVM assignment. No cache, transactional state or lifecycle consolidation was introduced. The destructive transfer was already present in committed HEAD f3a7963c, not introduced by recent management or Retry-After fixes.

**Evidence:** Three new Python tests produced eight failing subtests before the production fix. After regeneration, all32 offline tests in test_ContentUnderstanding.py passed against current compiled classes and generated wrappers, including repeated failed transform, copy/save rejection, zero HTTP before correction, successful explicit correction, scalar/column settings, persistence and existing service wrappers. The initial missing-pytest launcher failure was infrastructure-only; validation used the existing unittest runner without installing dependencies. Cognitive main/test style and compile, core/cognitive codegen and pinned Black205 unchanged passed. Source/generated Utils matched. Snapshot8 SHA256: 89a2e0462cf1a441513583ff7d96d9e228a1142e80009db3a994b018f8c97523.

The earlier165 Scala tests belong to snapshot6, and the five Retry-After tests to snapshot7; neither is represented as a fresh full Scala run on snapshot8. A new independent three-model pass is still required. No commit, push, live CU, rebase, ADO edit or CI trigger occurred.