---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 1
stage_attempt: 1
round: 3
timestamp: "2026-11-05T19:48:57Z"
outcome: "clean"
reviewer_model: "grok-4.7"
input_sha256: "f1d5d08d00f7ca0c0af0918d8663a2b3500633ed8434fd20dadd9a1601dbbd51"
---

## Review Summary
- **Round**: 3
- **Theme**: Edge cases & robustness
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-1-stage-attempt-1-review-3-grok-4.7.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Shared-stop and admission paths in `ContentUnderstandingProtocol.scala` (`rowHandler`, `checkSharedFailure`, `decodeSubmission`, `safeSubmission`): pre-acceptance 401/403/404/shared codes stop new submissions; poll and `Unknown` do not; 401/403/429 without a safe handle stay `Rejected` when the body read fails; contradictory or unreadable 404 handles stay `Unknown`; marker header is stripped in `HTTPRequestData.toHTTPCore`.
- [x] Deadline, DNS, and poll retention in `ContentUnderstandingBudget.scala`, `ContentUnderstandingDnsResolver.scala`, `ContentUnderstandingTransport.scala`, and `pollWithClient`/`retainPollState`/`finishCreation`: unsent expiry keeps prior HTTP status, body, and handle; clean retained state gains `OperationTimedOut`; DNS failure and immediate worker rejection are not rewritten as deadline expiry; permits are released on unsent expiry and held until a submitted lookup finishes.
- [x] URL-less and located creation in `createAnalyzer`: explicit `analyzerId` required before HTTP; `CreationStatusNames` is not applied to analysis polls; ready returns the resource; missing/unknown stays `InvalidResponse`; failed without a service error stays `OperationFailed`; service/transport diagnostics outrank `MissingOperationLocation`; a succeeded creation that expires before the final GET keeps that state plus `OperationTimedOut`.
- [x] Row validation, session resolver, and temp-column isolation in `ContentUnderstanding.scala`, `DatasetExtensions.scala`, and `SimpleHTTPTransformer.scala`: dataset session resolver used for direct transform; exact column match plus resolver ambiguity check; invalid rows become `InvalidInput` without dispatch; internal HTTP names are case-insensitive and cannot overwrite inputs.
- [x] JSON/header bounds in `ContentUnderstandingResponseParsing.scala` and `ContentUnderstandingHeaderValidation.scala`: depth 1000, numeric-token 100, iterative error-code walk, 64KiB/200 header constraints, Retry-After digit cap with saturation and HTTP-date support, no secret echo on header rejection.
- [x] Persistence and Python lifecycle in `ComplexParamsSerializer.scala`, `Utils.py`, and `Wrappable.scala`: CU `saveDefaultParamsAsExplicit` is false; column bindings omitted from scalar maps; `getOrDefault` on a binding raises; bytes are not passed through pickle conversion; dedicated setters drop stale pending values.