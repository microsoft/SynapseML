---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 8
round: 1
timestamp: "2026-11-04T07:29:43Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "b2ef09f34279912aa8445874c8b74396ddab4299605edd8701f460d5c2413b07"
---

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: copilot-cli
attempt: 1
stage_attempt: 8
round: 1
timestamp: "2026-11-04T07:22:01.309Z"
outcome: issues_found
reviewer_model: gpt-6.1-sol
input_sha256: 34b924b432dccae7c0fc607581440338281c1ac4efecbceaf6b49d8915032654
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-8-review-1-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the supplied inline diff and CU implementation, including submission, polling, authentication, deadlines, DNS, provisioning, and response parsing.
- [x] Traced column validation through `ContentUnderstanding.scala:85,336–341` and the inherited transformation entry point in `CognitiveServiceBase.scala:765–768`.
- [x] Inspected generated Python parameter handling, persistence tests, shared schema helpers, documentation, and pipeline selection.
- [x] Inspected case-resolution coverage in `ContentUnderstandingSuite.scala:150–180` and `VerifyDatasetExtensions.scala:28–38`; both exercise the current session rather than differing active/input sessions.
- [x] Considered the caller-reported offline validation results as supplied evidence, not independently executed results.
- [ ] No commands, tests, or live CU requests were executed during this read-only review.

## Issues

### Issue 1: Column collision checks use the active session instead of the input dataset's session
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala
- **Line(s)**: 85, 98–100, 338–340
- **Description**: The new comparisons use `SQLConf.get.resolver`, but the inherited `transform(dataset)` does not activate `dataset.sparkSession` before constructing the internal transformer. These sessions can differ when a DataFrame belongs to `spark.newSession()`. For example, with a case-insensitive active session and a case-sensitive input session, distinct output/error names such as `analysis` and `ANALYSIS` are incorrectly rejected. Likewise, an output name differing only by case from an input column is rejected despite being valid in that dataset's session. The previous exact comparisons allowed these valid case-sensitive configurations.
- **Risk**: Valid multi-session transformations now fail based on an unrelated active session's configuration. Temporary-name allocation has the same session dependency through `DatasetExtensions.findUnusedColumnName`.
- **Suggested Fix**: Perform transformation planning and name resolution using the input dataset's Spark session configuration. Make the dataset-taking schema-helper overload honor that dataset's resolver. Add a regression using separate active and input sessions with different `spark.sql.caseSensitive` settings.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Name validation must follow the session that analyzes the input DataFrame.
- **How verified**: Static call-path inspection; the multi-session regression has not been executed.

## Driver Decision

Tier: Max
Source: Human request to continue the Max review-fix loop until clean.
Change size: Broad cross-module CU connector work spanning Scala implementation and tests, Python tests, docs, shared code generation/schema helpers, and pipeline selection (25 diff sections; frozen patch 247,781 bytes).
Cyclomatic complexity: High; request, polling, deadline, DNS, authentication, analyzer provisioning/retrieval, persistence/codegen, and failure paths interact, with concurrency and recovery cases.
Rationale: Max is explicitly requested and justified by broad change surface and interacting asynchronous/network failure paths.
Primary model: Copilot CLI configured identity not relied on for review assignment; reviewers are explicit.
Reviewers: gpt-6.1-sol (latest available GPT flagship in current task catalog), grok-4.7 (latest Grok frontier), claude-opus-5.5 (latest Opus). Pairwise distinct.
Reasoning: xhigh for GPT and Opus; low for Grok per the user's prior explicit lower-reasoning preference.
Override source: Human instruction: continue review-fix loop until clean; use Grok 4.7 with lower reasoning; if Grok is not connecting, consider review clean when GPT and Opus are clean. The Grok availability exception applies only to a demonstrated connection failure; a Grok finding still blocks advancement.
Branch: work/cu-integration-2698 targeting master; live master baseline is Spark 3.5.0 / Scala 2.12.17 (build.sbt:31,34).
Frozen input: C:\Users\boraelkin\.copilot\session-state\653e4257-0554-408f-9e0e-b23750249edb\files\cu-max-review-diff-20261104-stage1-attempt8.patch
Input SHA-256: 34b924b432dccae7c0fc607581440338281c1ac4efecbceaf6b49d8915032654
Input includes tracked source changes plus the three untracked Scala source files, excludes reviews/cu-connector artifacts.
