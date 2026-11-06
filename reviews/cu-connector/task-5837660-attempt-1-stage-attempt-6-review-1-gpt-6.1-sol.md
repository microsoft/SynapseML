---
producer: "review-code"
schema_version: 1
tier: "Max"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 6
round: 1
timestamp: "2026-11-04T04:23:05Z"
outcome: "issues_found"
reviewer_model: "gpt-6.1-sol"
input_sha256: "fca76031075fc22ba082c400ace03d962d10295e150719760ae5b45065caf6ad"
---

---
producer: review-code
schema_version: 1
tier: Max
contract: direct
execution_backend: copilot-cli
attempt: 1
stage_attempt: 6
round: 1
timestamp: "2026-11-04T04:14:24.711Z"
outcome: issues_found
reviewer_model: gpt-6.1-sol
input_sha256: a6880db5fda399b888cc8b028781490bf67ae7ee159eb68d8b6d1ea592285a0a
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: gpt-6.1-sol
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-6-review-1-gpt-6.1-sol.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Reviewed all 19 files in the supplied immutable diff, including submission/polling, management, bounds, persistence, documentation, and pipeline wiring.
- [x] Traced authentication resolution through `CognitiveServiceBase.scala`, `ContentUnderstanding.scala`, and `ContentUnderstandingProtocol.scala`.
- [x] Cross-checked generated Python accessor targets in `Wrappable.scala` against the connector's JVM class hierarchy.
- [x] Inspected `ContentUnderstandingSuite.scala`, `ContentUnderstandingRecoverySuite.scala`, Python tests, and both fuzzing suites; the cases below are not covered.
- [ ] No commands or tests executed: this review was read-only. Caller-reported validation was considered supplied evidence, not independently reproduced.

## Issues

### Issue 1: Generated custom-header column setter has no JVM target
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingParams.scala
- **Line(s)**: 192-193
- **Description**: Delegating header parameters to the generic generator produces `setCustomHeadersCol`, which invokes `_java_obj.setCustomHeadersCol(value)`. Neither `ContentUnderstanding` nor its ancestors implements that JVM method; `HasCustomHeaders` only provides scalar setters. Consequently, both `.setCustomHeadersCol("headers")` and the generated `customHeadersCol` constructor argument fail with a Py4J method-resolution error. The generated zero-argument `getCustomHeaders()` also lacks a matching JVM accessor.
- **Risk**: Python callers cannot use the advertised per-row custom-header authentication path, although the Scala recovery tests exercise it through `setVectorParam`.
- **Suggested Fix**: Provide the missing JVM accessors locally or specialize generation to use the generic service-parameter accessors. Add an offline Python test configuring and transforming a custom-header column.

### Issue 2: Equal-valued scalar fallbacks incorrectly make row authentication failures shared
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala
- **Line(s)**: 486-494
- **Description**: Matching a transmitted credential's fingerprint does not establish its configuration source. For example, configure `setAADTokenCol("token")` and the lower-priority scalar fallback `setCustomAuthHeader("******")`, then supply rows containing `expired-token` and a valid token. The first request uses the **column** token, but its header equals the scalar fallback. The fingerprint match therefore classifies its HTTP 401 as shared and throws `ContentUnderstandingException`.
- **Risk**: One rejected row aborts a batch containing valid per-row credentials, contrary to the documented row-failure isolation.
- **Suggested Fix**: Track the credential source selected by authentication resolution rather than infer provenance from header equality. Add a mixed-row regression where the rejected column token equals a scalar fallback.

### Issue 3: Default scalar credentials are omitted from shared-authentication detection
- **Severity**: Medium
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstanding.scala
- **Line(s)**: 105-125
- **Description**: `scalarValue` reads only `get(param)`, whereas request preparation reads `get(param).orElse(getDefault(param))`. Inherited public methods such as `setDefaultAADToken` and `setDefaultCustomAuthHeader` therefore configure credentials that are sent but never fingerprinted. With a non-authentication `customHeaders` column configured through `setVectorParam`, `rowAuthColumn` becomes true and rejection of the effective default scalar token is incorrectly returned as row-level `Rejected`.
- **Risk**: A shared expired or invalid credential can reject the entire dataset while the Spark action succeeds instead of surfacing the shared configuration failure.
- **Suggested Fix**: Use effective parameter values, including defaults, consistently for scalar extraction and column detection. Add coverage for a default scalar token combined with a non-authentication header column.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Resolved
- **What changed**: `HasCustomHeaders` now exposes JVM scalar getters and column getter/setter methods.
- **Why**: Generated PySpark wrappers invoke these methods directly.
- **How verified**: Added a Python setter/getter regression; `cognitive/pyCodegen` passed and all 12 offline Content Understanding Python tests passed.

### Issue 2
- **Status**: Resolved
- **What changed**: The request carries an internal marker only when a row-bound credential actually wins auth resolution; shared-auth classification checks that provenance instead of inferring it from header equality.
- **Why**: Equal header values do not identify whether the selected credential came from a row or scalar fallback.
- **How verified**: Added an equal-valued row/scalar credential regression; the full recovery suite passed (29 tests).

### Issue 3
- **Status**: Resolved
- **What changed**: Scalar-auth fingerprinting now resolves configured defaults as well as explicit scalar values.
- **Why**: Failure classification must use the same effective credential as request preparation.
- **How verified**: Extended the default-token regression with a non-auth custom-header column; the full recovery suite passed (29 tests).