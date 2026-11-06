---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 4
stage_attempt: 1
round: 4
timestamp: "2026-11-06T21:17:55Z"
outcome: "issues_found"
reviewer_model: "grok-4.7"
input_sha256: "5045c291f7524cbd30b8349b20495583e2ea747dddae916aeec5062482d079d6"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-4-stage-attempt-1-review-4-grok-4.7.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Submission assessment predicates compared with `safeSubmission` overwrite in `ContentUnderstandingResponseParsing.scala` and `ContentUnderstandingProtocol.scala`
- [x] DNS permit acquire/release, snapshot `waitRemaining`, and cancellation polling checked in `ContentUnderstandingDnsResolver.scala` and `ContentUnderstandingBudget.scala`
- [x] Task-owner propagation, deadline abort, and 404/410 creation retention checked in `Clients.scala` and `ContentUnderstandingProtocol.scala`
- [x] Python `setParams` validation-before-mutation and JVM default transfer checked in `Utils.py` and `ContentUnderstandingParams.scala`
- [x] Shared HTTP `errorSchema` nullability checked against the preparation-error path in `SimpleHTTPTransformer.scala`

## Issues

### Issue 1: Non-accepted handle diagnostics replace definitive submission failures
- **Severity**: Medium
- **File**: C:\Users\boraelkin\SynapseML-cu-native\cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingResponseParsing.scala
- **Line(s)**: 136-141
- **Description**: `assess` now treats every non-accepted response with an `Operation-Location` as an uncertain discarded handle. Both the invalid-location arm (`requiresLocation || !accepted`) and the unaccepted arm (`!accepted`) no longer require a successful nonstandard submission. `safeSubmission` then forces `status = "Unknown"` and replaces `response.error` with that assessment. A 4xx/5xx body that merely echoes a location, including terminal `failed`, loses `HttpError` / service failure details. Handles are still not followed.
- **Risk**: Definitive rejection, throttling, or auth failure is reported as an uncertain discarded handle. Callers can retry or retain the wrong recovery state, and the 203/206 case does not need this broader predicate.
- **Suggested Fix**: Emit `InvalidOperationLocation` / `UnacceptedOperationLocation` only for successful non-accepted submissions (`successful && !accepted`, or `requiresLocation`). Leave unsuccessful responses on the decoded failure error. If both apply, nest the discarded-handle diagnostic instead of replacing the service error.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: pending
- **Why**: pending
- **How verified**: pending

## Driver audit and resolution history: targeted iteration 2

The report and its original verdict are preserved, but this invocation is
**scope-invalid and not independent signoff**. Two searches omitted the required
explicit source path. The four malformed encoded-path searches failed explicitly;
no feedback read was observed. The complete transport audit retains the original
violations and tool outcomes. The next invocation must supply a literal path even
when its glob appears restrictive.

The diagnostic loss overlaps Opus issue 1 and was independently reproduced:
an unsuccessful response carrying a location lost `ServiceDetail`.
The broader claim of a newly changed rejection status is not established:
the existing `uncertainSubmission` already maps nonaccepted responses carrying
locations to Unknown. That pre-existing state behavior is deliberately unchanged.

The correction restricts discarded/unsafe-handle diagnostics to successful
nonaccepted submissions or successful ongoing responses requiring a location.
Unsuccessful submissions retain their decoded service/HTTP diagnostic, still
discard the handle and do not send a GET.

The expected failure is retained in `cu-focused-t2-red-protocol-dns-r2.log`.
Status at this entry: **implemented locally; green validation and fresh targeted
review pending**. An invalid invocation is not retroactively made valid by a fix.

### Compatibility refinement

An existing regression additionally requires unsafe-location diagnostics on
throttled submissions. Rather than weakening that assertion, the final correction
retains the service error as primary and adds the unsafe-location warning to its
details, preserving prior details/innererror and nonobject service-error content.
The complete CU and Python validation rerun is pending. The invocation's scope
violation remains unchanged.
