---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 3
stage_attempt: 2
round: 4
timestamp: "2026-11-06T19:52:39Z"
outcome: "clean"
reviewer_model: "grok-4.7"
input_sha256: "3346bf150e0c0cc654ff25d639ef4be30ccd33fbcc7b0c2bce37db9178c57b07"
---

## Review Summary
- **Round**: 4
- **Theme**: Detailed correctness
- **Mode**: resolved-plan
- **Model**: grok-4.7
- **Artifact**: C:\Users\boraelkin\SynapseML-cu-native\reviews\pr-2815\task-5837660-attempt-3-stage-attempt-2-review-4-grok-4.7.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Poll attempt accounting, retain-on-retry, and pre-dispatch deadline retention in `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingProtocol.scala` (`pollWithClient`, `retainPollState`, `safeSubmission`): 120 attempts include the last GET; `delay >= remaining` skips a poll that cannot finish; terminal statuses do not replace a known handle.
- [x] Response bound in `readBounded`: `remaining = limit - size + 1` reads one extra byte only to detect overflow; exact `limit` bodies are kept; `getContentLength == -1` falls through to the bounded read.
- [x] DNS permit lifecycle in `ContentUnderstandingDnsResolver.scala`: acquire failure, post-acquire deadline, and `RejectedExecutionException` all release; the worker `finally` releases after submit; timeout does not cancel an in-flight lookup (documented, not a leak of the permit beyond that lookup).
- [x] Auth provenance vs `ServiceAuthHeaders.resolve` in `CognitiveServiceBase.scala` and `ContentUnderstanding.rowUsesColumnAuth`: blank values cannot win; marker is stored on `HTTPRequestData` and stripped only in `toHTTPCore` (`HTTPSchema.scala`), so shared 401/403 classification still sees row credentials.
- [x] Python scalar/column lifecycle in `core\src\main\python\synapse\ml\core\schema\Utils.py`: pending `_paramMap` wins; column `Right` is excluded from `isSet`/`hasDefault`/`extractParamMap`; `getOrDefault` raises `KeyError`; bytes skip `_java2py` pickle decoding; failed setters pop only after success.
- [x] Operation-location query check is a full-string `api-version` match (`validApiVersion` via `String.matches`); extra parameters fail closed. JSON bounds reject depth 1001 and 101-digit numbers while ignoring quoted text.
