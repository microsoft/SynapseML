---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 2
stage_attempt: 2
round: 6
timestamp: "2026-11-05T22:47:01Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "59c872ee948e748a0bb6129ecd7320fb2570853a3bbe4de268db415e53ce8e92"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-2-stage-attempt-2-review-6-gpt-6.1-sol.md
- **Issues Found**: 2
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the supplied contribution, including Scala implementation, Python lifecycle changes, serializer compatibility, tests, documentation, and approved pipeline selector.
- [x] Traced polling and management recovery through `ContentUnderstandingProtocol.scala`, `ContentUnderstandingTransport.scala`, and the admission/submission suites.
- [x] Inspected DNS worker capacity, response bounds, sanitized logging, and corresponding concurrency/recovery tests.
- [x] Checked shared dependencies in `CognitiveServiceBase.scala`, `Utils.py`, `Wrappable.scala`, `ComplexParamsSerializer.scala`, and the HTTP pipeline; confirmed provenance stripping in `HTTPSchema.scala:209`.
- [x] Compared published polling and recovery guidance in `Content Understanding.md` against current execution paths.
- [ ] Tests were not executed. Supplied validation results remain caller-provided evidence; current Python runtime, publication, live CU, external CI, and latest-target integration were not independently established.

## Issues

### Issue 1: Polling-delay documentation incorrectly includes the first poll-only GET
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingParams.scala
- **Line(s)**: 113–115
- **Description**: The parameter documentation says `pollingDelay` applies before every operation poll, including the first. However, `ContentUnderstandingProtocol.scala:644` starts poll-only mode with `firstDelay = 0L`. The first GET is immediate regardless of `pollingDelay`; only subsequent GETs use the fallback. The website repeats the inaccurate claim at `docs\Explore Algorithms\AI Services\Content Understanding.md:387`.
- **Risk**: Users can miscalculate initial request timing and budget consumption, particularly when repeatedly invoking poll-only mode with one allowed attempt.
- **Suggested Fix**: Preserve existing execution behavior and qualify both descriptions: the delay includes the first post-submission poll, but poll-only mode starts immediately. Regenerate Python documentation normally.

### Issue 2: Management deadline diagnostics still recommend operation polling
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 195–196, 743–745
- **Description**: `timeoutError` advises “Retain any handle and resume polling.” This diagnostic also reaches direct analyzer retrieval and expiration before the final definition GET after successful creation. Direct retrieval has no operation handle; the retained creation handle is a management URL that public CU poll mode does not accept. `ContentUnderstandingTransport.scala:32–36` likewise selects this generic timeout advice before considering `analyzerRetrieval`, unlike its corrected non-timeout retrieval advice.
- **Risk**: Users following the diagnostic can attempt unsupported polling instead of retrieving the analyzer definition with `getAnalyzer`.
- **Suggested Fix**: Make timeout advice retrieval-aware or neutral, without changing `OperationTimedOut`, retained state, or diagnostic precedence. Extend the existing retrieval-advice tests to cover management deadline outcomes.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed locally after final review; not independently re-reviewed.
- **What changed**: Scala parameter text and the Markdown guide distinguish the immediate first poll-mode GET from fallback waits after submission and between polls/retries. Regenerated Python documentation through cognitive/codegen.
- **Why**: Documentation should distinguish post-submission polling from poll-only entry.
- **How verified**: Driver validation passed all 47 tests in AdmissionSuite and SubmissionSuite. The new public-transformer regression preserves immediate poll entry and fallback waits after submission/subsequent polls. All four generated Python descriptions contain the corrected text.

### Issue 2
- **Status**: Addressed locally after final review; not independently re-reviewed.
- **What changed**: The shared OperationTimedOut message is neutral: "The operation budget was exhausted." No status code, timing, retained response, handle, or precedence logic changed.
- **Why**: Recovery advice should match the supported management API.
- **How verified**: Direct analyzer retrieval and completed-creation/final-GET deadline regressions both failed on the old resume-polling advice and passed after the change. Existing completed status, HTTP status, raw response, ID, handle, and request-count assertions remain intact.

## Final driver disposition

- The authorized polish-only High follow-up completed both valid attempts. No third model-review attempt was launched.
- Final independent reports remain ISSUES_FOUND: GPT two Low (146.409 seconds), Grok one Medium (140.294 seconds), Opus one Low (87.437 seconds). These represent two unique findings; all three reported the same polling-documentation mismatch.
- Original frontmatter, findings, severities, and verdicts above are unchanged. The driver resolutions describe later local edits, not independent clean signoff.
- Independence audit completed before local corrections: all input hashes and actual tool arguments/results were checked; no prior/sibling feedback exposure. Failed guessed-path searches were retained rather than treated as proof of absence. Frozen reviewed snapshot4 was verified unchanged after collection.
- Reviewed snapshot: `cu-stage6-attempt4.patch`, SHA256 `8cc9fcf107a0a9e01d87dd1cf98ff435a81c5be871424e3116a1771d9a475d40`. The earlier 161/161 full-CU result applies to that snapshot.
- Final local snapshot, not independently re-reviewed: `cu-stage6-attempt5.patch`, SHA256 `ce1451e0b2bb20af83054257fdc8b2e2d72049c9ea65926d9a1290fff1479394`, 488269 bytes, 38 source paths. Exactly five files differ from snapshot4: Protocol, Params, AdmissionSuite, SubmissionSuite, and the CU guide.
- Final validation: cognitive main/test scalastyle, main/test compile, all 47 tests in the two affected suites (zero failed/canceled/ignored/pending), and cognitive/codegen passed. Black left 205 files unchanged; whitespace checks passed. Python class arguments, Param metadata, setter, and getter descriptions were inspected after regeneration.
- Evidence in session files: `cu-polish-final-wording-red.log` (two expected failures, one timing regression passed), `cu-polish-final-wording-green.log`, `cu-polish-final-wording-black.log`, and both preserved snapshot manifests. The latest 47-test result is not presented as a post-edit rerun of all 161 tests.
- All six selected polish findings and their scoped follow-up corrections are addressed locally. Earlier architecture/coverage deferrals and the documented asynchronous TaskContext limitation remain outside this cycle. No claim of merge readiness.
- PR #2698 still targets master at `f3a7963c217677e9b6564cc6736495e0f1bcdce4`. No commit, push, rebase, live CU call, provisioning, ADO edit, or CI trigger was performed.