---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 1
round: 1
timestamp: "2026-11-06T17:33:19Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "5b460abcd84f7cb833b35d4ea08b62148d2813edc9a6b70a350b89104cadb4a7"
---

---
producer: review-code
schema_version: 1
tier: High
contract: direct
execution_backend: copilot-cli
attempt: 3
stage_attempt: 1
round: 1
timestamp: "2026-11-06T17:29:38.112Z"
outcome: issues_found
reviewer_model: gpt-6.1-sol
input_sha256: 3f656dd61c5312777fd356262a0eb023eb45fb2b07d423403c62433379499c8d
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-1-review-1-gpt-6.1-sol.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed the complete supplied 37-path diff against master `d3f3af6bd3e4b7d191f51c9eb29f9aeffe788193`, for reviewed commit `085928a9b1ee73473ea339c5a3b28bc2d5e5b1e0`.
- [x] Traced submission/polling transitions, accepted-handle retention, authentication provenance, DNS admission and transport deadlines through `ContentUnderstanding.scala`, `ContentUnderstandingProtocol.scala`, `ContentUnderstandingDnsResolver.scala`, and shared HTTP clients.
- [x] Examined Python parameter precedence, byte conversion, generated accessors and persistence in `core\src\main\python\synapse\ml\core\schema\Utils.py`, `Wrappable.scala`, and `ComplexParamsSerializer.scala`.
- [x] Compared CU wrapper generation with existing validation helpers and regression assertions in `test_ServiceParamPythonBridge.py:121–143`; examined supplied CU Scala/Python tests and documentation.
- [ ] Tests were not executed. Supplied validation results were considered as caller-provided evidence, not independently reproduced. No review artifacts or run directories were inspected.

## Issues

### Issue 1: CU setParams bypasses existing ServiceParam argument validation
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingParams.scala
- **Line(s)**: 205–214
- **Description**: The generated CU-specific `setParams` directly loops over setters and skips `None`, bypassing `_validate_service_param_arguments`. Consequently, `stage.setParams(documentBytes=b"new", documentBytesCol="body")` succeeds and silently selects the column; reversing keyword order selects the scalar. Likewise, `stage.setParams(documentBytes=None)` succeeds while retaining any previously configured document. Master's generated implementation delegates to `_set_params_via_setters`, which rejects simultaneous scalar/column aliases and explicit service-parameter `None` before applying updates (`Utils.py:89–105, 141–147`; `Wrappable.scala:530–543`). The inherited CU constructor still uses that validation, making constructor and `setParams` behavior inconsistent with the required contract.
- **Risk**: Invalid configuration appears successful and can analyze the wrong document or retain a stale credential/input. Conflicting aliases can also consume pending Python values instead of leaving the rejected update unchanged.
- **Suggested Fix**: Delegate CU `setParams` to `_set_params_via_setters(kwargs)` or retain the inherited generator implementation. Keep constructor-only `skip_none=True` behavior separate. Add focused CU tests asserting conflicting aliases and explicit service-parameter `None` raise without changing existing JVM bindings or pending Python values.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Preserve master's ServiceParam validation and prevent silently ambiguous updates.
- **How verified**: Static tracing against the existing helper and bridge tests; fix verification pending.

## Driver resolution and effective decision

The original response above is preserved. Issue 1 is confirmed and fixed
locally: CU's generated `setParams` now delegates to the existing
`_set_params_via_setters(kwargs)` helper. This validates conflicting scalar/column
aliases and explicit service-parameter None before any setters run, while
retaining the public kwargs signature and dedicated setter implementations.
No new validation framework, transactional state or shared-helper changes were
introduced.

Two new regression methods produced five failures before the fix: both alias
orders and explicit None for document scalars, document columns and credentials.
After regeneration they pass, asserting that rejected calls preserve pending
Python entries, JVM bindings/credentials and unrelated settings. The existing
request/copy/persistence test now configures valid native options through
setParams, preserving its request and response assertions.

Fresh evidence: cognitive main/test style and compilation, core/cognitive
codegen, and the focused Scala generated-wrapper test passed. All 57 Python
tests passed together (34 CU and 23 upstream bridge tests). Pinned Black passed
for all 219 files; whitespace checks passed. The first Python full-suite
launcher selected mismatched driver/worker interpreters; that run was stopped
and is not counted. The successful rerun pinned both to the existing Python
3.11 environment. No live-service calls or Azure pipeline trigger occurred.

The reviewed correction is a two-file working-tree delta on
085928a9b1ee73473ea339c5a3b28bc2d5e5b1e0, not yet committed or pushed.
Full-diff snapshot 3 contains 37 paths / 441538 bytes, SHA256
e5b3150bbae9ee013ea0c08a94a194bbcf11701419ff548a19588d7e8ff5dbcd.
Session evidence: cu-native-setparams-red-r2.log,
cu-native-setparams-green.log and cu-native-setparams-python-r2.log.

Effective policy remains human-selected High/direct over the complete feature:
six ordered stages, GPT gpt-6.1-sol/high, Grok grok-4.7/low and Anthropic
claude-opus-5.5/high concurrently within each batch. The reasoning override is
the recorded driving-human agreement. Primary model is gpt-6-astra. The large
37-file change remains high-complexity across asynchronous state, deadlines,
authentication and Python/JVM lifecycle. Existing bounds are unchanged;
invalid invocations cannot satisfy advancement and unresolved findings remain
visible. Repository AGENTS.md requires first committed evidence under pr-2815.

Attempt-1 transport disposition: all input hashes, runtime identities and
finalization receipts were retained. No feedback-pattern matches were observed
in tool results, but GPT skill-directory searches and Grok searches without
explicit source paths violated the prescribed scope. Those slots do not count
as compliant independent signoff. Opus complied; its pipeline approval concern
has historical authorization, while Azure execution remains outstanding.
The corrected tree proceeds to a fresh three-reviewer stage-1 attempt 2 with
explicit path instructions; no attempt-1 feedback is supplied to reviewers.

The inner model-authored frontmatter in the original GPT response is not
authoritative: it uses the patch hash rather than the input hash. The outer
skill-owned header and retained receipt establish input identity.

## Driver consolidated disposition (2026-11-06)

Post-review driver assessment; this does not rewrite the model verdict,
clear unresolved findings, or validate an invocation rejected by its audit.
Duplicate reports are not independent confirmations.

### s1t1-gpt-6.1-sol-i1: Explicit setParams validation

- Disposition: **fixed-locally**.
- Evidence: Shared helper delegation and focused red/green regressions; 57 Python tests passed. Not yet committed.
- Consolidated topic: `setparams`.
