---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 7
stage_attempt: 1
round: 5
timestamp: "2026-10-23T19:09:58Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "495139de306c4ee5494022ac2e34f7a037ed0c2b79dc52376bb495c586816c1e"
---

## Review Summary
- **Round**: 5
- **Theme**: Testing & coverage
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: `reviews\pr-2781\task-unknown-attempt-7-stage-attempt-1-review-5-gpt-6.1-sol.md`
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Cross-checked parameter forwarding and normalization against `cognitive\src\test\scala\com\microsoft\azure\synapse\ml\services\openai\OpenAIPromptCacheSuite.scala`: unset/null omission, explicit empty maps, Foundry inheritance, nested values and all five supported breakpoint content types.
- [x] Reviewed real Spark/loopback assertions in `cognitive\src\test\scala\com\microsoft\azure\synapse\ml\services\openai\OpenAIPromptMultimodalRequestSuite.scala:146-245`: both APIs, four public routes, Boolean struct values, provider HTTP 400 propagation and marked/unmarked content.
- [x] Inspected generated-wrapper, copy and persistence coverage in `cognitive\src\test\python\synapsemltest\services\test_ServiceParamPythonBridge.py:23-68`; identified the Boolean assertion weakness below.
- [x] Reviewed `core\src\test\scala\com\microsoft\azure\synapse\ml\fabric\OpenAIFabricSettingSuite.scala` alongside production status handling, and all four notebook diffs. The cache tutorial distinguishes raw/normalized usage and does not require positive cache counts.
- [x] Considered driver-supplied Scala, installed Python and notebook validation; none was executed or independently reproduced during this review. Retained limitations: local Python 3.12.3 versus target 3.11.8, and bundled Scala 2.12.18 versus build 2.12.17.
- [ ] Current-revision PR-triggered CI, coverage publication and the distinct final review pass remain pending; this verdict covers precommit review only.

## Issues

### Issue 1: Boolean-preservation assertions also accept numeric values
- **Severity**: Low
- **File**: `cognitive\src\test\python\synapsemltest\services\test_ServiceParamPythonBridge.py`
- **Line(s)**: 37-46
- **Description**: Dictionary equality does not distinguish Python `True` from `1`. Consequently, every new assertion here would pass if the scalar bridge returned `{"mode": "explicit", "ttl": "12h", "prewarm": 1}`. The Scala HTTP regression asserts `JsBoolean`, but exercises Spark struct columns rather than Python scalar setter conversion.
- **Risk**: A Boolean-to-number regression in the Python scalar path could escape this type-preservation test despite violating the required JSON contract.
- **Suggested Fix**: Add type-sensitive checks, such as `assertIs(actual["prewarm"], True)`, for original and copied stages. Include a Boolean in the scalar save/load fixture and verify its type after restoration.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending.
- **Why**: Scalar bridge assertions must distinguish JSON booleans from numbers.
- **How verified**: Static inspection; no changes made or tests executed in this round.

## Driver triage and pass disposition

The original reviewer response above is preserved unchanged.

Issue 1 is **confirmed and remains open**. A targeted installed-wrapper
experiment verified that the current scalar setter and copy paths actually
return Boolean `True` for all four wrappers. It then injected integer `1`
only into scalar getter readback and ran the two existing cache test methods.
Both methods still passed, including twelve corrupted readbacks across the
four wrappers. This demonstrates an assertion-sensitivity gap, **not an
observed production Boolean-conversion defect**. The scalar persistence
fixture also contains no Boolean to verify after loading.

The proposed narrow correction is to use type-sensitive Boolean assertions
for original, copied and restored scalar options, and include a Boolean in
the persistence fixture. No production or test fix was applied during this
pass. Such a test change requires a fresh full precommit pass.

### Effective decision and aggregate outcome

- Tier: human-selected Medium. Exact human reviewer override:
  `gpt-6.1-sol/max`, advertised by the live harness catalog.
- Scheduling: six independent sequential lenses under trusted repository
  policy, overriding the installed concurrent Medium default.
- Primary runtime model identity was unavailable; none was invented.
- Scope: complete 29-path patch, not merely the formatting correction.
  The incremental source change is four line wraps in two notebook cells;
  the complete patch includes cross-language and nested serialization paths.
- Source manifest:
  `213d4b7178c74f5c7da2f7c67ba6112630e0a78b4c56e89a127f947685c9f022`.
- Diff:
  `83b4442d184f4e530d67d0abef9097f9de74d18fb0c324f8b7474e4de9cd8dfa`.
  The entire 96,428-byte diff independently reconstructed all manifest
  entries and was embedded intact in each reviewer input.
- Lenses 1–4 are clean. Lens 5 reports this confirmed low-severity test gap.
  All five transport/provenance/privacy audits passed. Lens 6 was not
  dispatched because the unresolved issue requires a source-test fix.
- Notebook-aware Black 22.3.0 passed without skipping. Both affected cell
  ASTs, all other cells and notebook metadata were preserved. The exact five
  tutorial cells passed seven loopback requests with SDK/SynapseML parity.
  Unchanged production artifacts were reused; no additional live calls or
  rebuilds were performed for formatting.
- The explicitly authorized seventh precommit pass is consumed. No further
  precommit allowance remains; final allowance is unchanged at zero of two.
  This is a blocked partial pass, not a qualifying review or a CI waiver.

## Subsequent resolution: authorized pass 8

Issue 1 is now **resolved** by the narrow test-only correction in
`cognitive/src/test/python/synapsemltest/services/test_ServiceParamPythonBridge.py`.
Original and copied scalar options use `assertIs(..., True)`, including the
original stage after modifying its copy. The scalar save/load fixture now
contains `prewarm=True`, and the restored value receives the same identity
check. Existing equality and column-binding assertions remain intact.

The two focused installed-wrapper tests passed with six subtests. Separate
readback-only integer injections were rejected for original options across
four wrappers, copied options across four wrappers, and one restored scalar
stage: nine expected assertion failures and zero unexpected errors.
No production code or generated binaries changed.

The human explicitly authorized one **Low-tier, single-reviewer full-patch
pass** for attempt 8, retaining `gpt-6.1-sol/max`. Its
[script-owned harness review](task-unknown-attempt-8-review-0-harness.md)
is clean and its complete-input, model, independence and privacy audit passed.
This exception does not change the distinct final Medium six-lens requirement.
All earlier feedback and the blocked pass-7 disposition above remain historical.