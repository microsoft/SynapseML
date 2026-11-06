---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 2
stage_attempt: 3
round: 6
timestamp: "2026-11-05T23:10:47Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "f6284f5c6d233dcefbc33c86d9c003770785a1b5d29a705341cc86cd32e50c08"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-2-stage-attempt-3-review-6-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the supplied contribution and inspected CU submission, polling, creation, deadlines, DNS capacity, parsing bounds, and sanitized logging implementations.
- [x] Traced management recovery advice through `ContentUnderstandingProtocol.scala:198-200, 609-626, 771-780` and `ContentUnderstandingTransport.scala:35-38`, against operation-location validation at `ContentUnderstandingProtocol.scala:275`.
- [x] Inspected `ContentUnderstandingSubmissionSuite.scala:211-236`: recovery-wording tests cover final analyzer retrieval and analysis polling, but not interrupted or exhausted creation-operation polling.
- [x] Reviewed Python parameter lifecycle/code-generation and JVM persistence changes, plus credential/autologging warnings in both changed documentation pages.
- [x] Assessed driver-supplied validation: current-snapshot compilation, style, 47 Admission/Submission tests, and codegen passed; the earlier 161-test run covers the preceding snapshot. No tests were executed by this reviewer.
- [ ] Current publication/runtime, live-service, latest-target integration, and external CI evidence were not supplied; this is not a merge-readiness verdict.

## Issues

### Issue 1: Creation-operation diagnostics still prescribe unsupported polling recovery
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 198-200, 621-626, 778-780
- **Description**: `createAnalyzer` uses the shared polling loop for `/analyzers/{id}/operations/{operation}` handles. A clean `Running` response at the attempt limit receives “Retain the handle and resume polling.” Creation-operation transport failures likewise receive that advice from `ContentUnderstandingTransport.scala:38`, because the exchange at line 621 uses the default request context. However, public poll mode validates only `/analyzerResults/` handles (`ContentUnderstandingProtocol.scala:275`); it cannot perform the recommended recovery. Documentation at `docs\Explore Algorithms\AI Services\Content Understanding.md:416-422` also gives unqualified poll-mode recovery advice.
- **Risk**: Following the diagnostic fails local validation instead of recovering creation. Users lack supported next steps and may mistakenly repeat the administrative PUT.
- **Suggested Fix**: Use the existing creation context (`analyzer.isDefined`) to select management-specific exhaustion and transport wording. Advise inspecting the analyzer with `getAnalyzer` or service tooling without automatically repeating PUT, and explicitly restrict documented poll-mode recovery to analysis handles. Add focused creation-operation tests for a final clean `Running` poll and a disconnected poll; preserve existing timing and response-retention behavior.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Verified by driver; unresolved.
- **What changed**: No source changes during this review.
- **Why**: Recovery guidance must describe supported operations without introducing new public APIs.
- **How verified**: Static inspection confirms createAnalyzer passes Some(analyzerId) into the shared poll loop, but exhaustion uses the unconditional pollAttemptsError and exchange leaves analyzerRetrieval false. The TransportError text recommends resume polling; the guide similarly gives unqualified poll-mode advice. Public analysis poll mode does not accept management operation URLs. No new runtime reproduction was run.

## Third-pass driver disposition

- **Effective decision:** High/direct, stage6, review attempt2, stage attempt3. The human's explicit "Start the third review" authorizes this one extra pass after the closed two-attempt cycle. It does not restart six stages, reopen deferred work, authorize a fourth pass, or change security/trust/permission controls.
- **Decision sources:** High tier and GPT/Opus high reasoning, Grok low reasoning are retained human selections. Exact model slots were resolved from the current task-tool catalog: gpt-6.1-sol, grok-4.7, claude-opus-5.5. Primary: gpt-6-astra. Broad38-path contribution and qualitatively high concurrency/deadline/acceptance/credential/persistence complexity justify independent multi-model inspection.
- **Results:** GPT one Medium (113.335 seconds); Grok one Medium (125.875 seconds), duplicating this creation-recovery finding; Opus one Low (154.760 seconds), repeating the previously deferred exception-message diagnostic-code UX recommendation. All three exited0 and persisted ISSUES_FOUND. This is one newly identified Medium finding plus one existing deferred Low item, not three new defects.
- **Disposition:** Complete with unresolved findings. This review request did not initiate another implementation or review loop. No source changes, commit, push, rebase, provisioning, ADO write, CI trigger or live CU call. The earlier architecture/coverage backlog remains separate.
- **Snapshot:** `cu-stage6-attempt5.patch`; SHA256 `ce1451e0b2bb20af83054257fdc8b2e2d72049c9ea65926d9a1290fff1479394`; 488269 bytes,38 source paths. Source hashes and original HEAD `f3a7963c217677e9b6564cc6736495e0f1bcdce4` verified unchanged after collection.
- **Validation:** Existing exact-snapshot evidence remains cognitive main/test style/compile,47/47 AdmissionSuite/SubmissionSuite tests, codegen, Black205 unchanged and whitespace checks. Earlier161/161 full-CU evidence covers the preceding snapshot. Reviewers did not execute tests; no current publication/runtime, live-service, latest-target integration or external CI claim.
- **Independence audit:** All three input hashes match original artifact frontmatter and finalized receipts. Actual tool arguments and results were audited: GPT40, Grok10 and Opus15 completed read/search calls. No prior/sibling feedback access or returned feedback markers. Some searches were broader than the prompt's explicit-directory preference, but code-only globs excluded feedback; known-guide searches remained confined to that document. GPT's empty guessed REVIEW-PROMPTS lookup is retained, not treated as evidence of source absence.
- **Caller context:** `cu-polish-r2a3-context.txt`, SHA256 `770bc8c961ffc9cd3a5ff5d7ef407bd57e2237b33d68c691133db361b6f37777`. Each skill-owned Execute invocation retained its own input, response, events and finalization receipt in the session files directory.
- **Relationship to prior fixes:** The immediate poll-only timing description and neutral OperationTimedOut text remain corrected. The new finding is another recovery path: creation-operation attempt exhaustion and non-timeout transport failure, not a reversal of those fixes.

## Subsequent authorized remediation

- The human requested fixing findings and rerunning until clean, after first explaining causality. Before source edits, the driver confirmed that committed HEAD already gave creation-GET transport failures analysis-poll advice. Earlier local changes introduced the same advice for PollAttemptsExhausted; both existed in pre-polish snapshot2. The most recent wording fix did not introduce them but missed these sibling paths.
- Creation-operation GETs now reuse the management context already used by direct/final analyzer retrieval. Exhaustion and transport diagnostics advise inspecting with getAnalyzer, not repeating creation. Analysis recovery wording, timing, HTTP status, raw response, handle and precedence are unchanged. Parameter and guide docs explicitly restrict public poll mode to analysis handles.
- The exception-message recommendation was also authorized for this loop. A bounded fixed-code allowlist and neutral message replace generic troubleshooting advice; the public exception signature and structured response remain unchanged. Its class is in its own file to preserve the protocol file-size limit.
- `cu-management-recovery-red.log` retains four expected pre-fix failures. `cu-management-recovery-green.log` records main/test style/compile,165/165 tests across7CU suites and codegen passing. `cu-management-recovery-black.log` records205 unchanged files; whitespace passed.
- Snapshot6: `0ac70e3a320cd70af0a37ff10c5882ddfb08ea147bc21cce01f0ff3d5e538807`,495573 bytes,39 paths. These fixes are locally validated and pending the next independent review. The original third-pass verdict remains ISSUES_FOUND.