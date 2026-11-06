---
producer: "review-code"
schema_version: 1
tier: "High"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 2
stage_attempt: 2
round: 6
timestamp: "2026-11-05T22:46:27Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "dc6bdd749bf51c6e5a7a850d485a45e9171f2be74c1a6a6288251d4d750b730e"
---

## Review Summary
- **Round**: 6
- **Theme**: Polish & hardening
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: C:\Users\boraelkin\SynapseML\reviews\cu-connector\task-5837660-attempt-2-stage-attempt-2-review-6-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Compared the `pollingDelay` contract with the code. The param doc in `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingParams.scala` (lines 113-116) and the docs table row at `docs\Explore Algorithms\AI Services\Content Understanding.md` line 387 both say "including the first". The execution paths in `ContentUnderstandingProtocol.scala` disagree: poll mode at line 644 starts with `firstDelay = 0L`, while analyze at lines 653-654 and creation at about lines 778-780 start with `retryDelay(raw.retryAfter, settings.pollingDelay)`.
- [x] Checked the per-task shared-stop handler in `ContentUnderstandingProtocol.rowHandler` (lines 672-698). The stop flag is a per-closure `AtomicBoolean`. Poll mode never trips the stop. The first stop is logged once through `compareAndSet`. Nested diagnostics only copy connector codes (TransportError, OperationTimedOut, ResponseTooLarge, ResponseBodyReadFailed) and only when no raw response body exists. This matches the contract.
- [x] Checked that the new `isSet` stub from `Wrappable.pyAdditionalMethods` (`core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala:83`) reaches cognitive services. Every override under `cognitive\src\main\scala` calls `super.pyAdditionalMethods`: CognitiveServiceBase.scala:284/636/679, ContentUnderstandingParams.scala:215, AzureMapsTraits.scala:23 and OpenAIPrompt.scala:168. `core\src\main\scala` has no other override.
- [x] Reviewed the diff's performance and observability changes. The class now logs under `FeatureNames.AiServices.ContentUnderstanding` instead of `Form`. `transform` wraps the snapshot pipeline in `logTransform`. Each operation creates its own HTTP client, which is documented. The DNS resolver and deadline executor have fixed sizes. `Content-Type` deduplication is case-insensitive. I found no unbounded allocation or hidden retry loop.
- [ ] I did not run any tests, builds or live CU calls; this review was read-only by mandate. Validation claims from the supplied logs were not independently reproduced.

## Issues

### Issue 1: `pollingDelay` docs say the first poll is delayed, but poll mode sends it immediately
- **Severity**: Low
- **File**: cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\contentunderstanding\ContentUnderstandingParams.scala; docs\Explore Algorithms\AI Services\Content Understanding.md; generated ContentUnderstanding.py (derived from the Scala param doc)
- **Line(s)**: ContentUnderstandingParams.scala 113-116; Content Understanding.md 387; ContentUnderstandingProtocol.scala 644 (compare 653-654)
- **Description**: The param doc says pollingDelay is "Milliseconds before each operation poll, including the first". The docs table says the same. The supplied validation evidence also says pollingDelay "covers every operation poll, including the first". That holds for `analyze` and `createAnalyzer`, which wait Retry-After or pollingDelay before the first GET. In `operationMode="poll"`, `execute` calls `pollWithClient(..., None, 0L, budget)`, so the first GET on a saved operation handle goes out immediately. pollingDelay only applies between later polls in that mode.
- **Risk**: The public description is inaccurate for one of the three operation modes. A user resuming many saved handles may expect a pause before the first request, set pollingDelay to pace requests, and instead get an immediate burst of GETs, which can raise 429 rates. The validation claim that the generated docs were "verified" is also not quite right.
- **Suggested Fix**: Prefer a docs-only fix, because changing behavior would alter poll-mode timing. Reword the Scala param doc, and through codegen the Python wrapper, along these lines: "Milliseconds between operation polls. After an analyze or create submission, also the wait before the first poll when Retry-After is absent or invalid. Poll mode sends its first GET immediately. Also the wait between management GET retries." Update the docs table row to match. Optionally add a fixture test asserting zero initial delay in poll mode so the documented behavior is locked in.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Addressed locally after final review; not independently re-reviewed.
- **What changed**: Corrected the Scala parameter description and Markdown guide, then regenerated Python documentation. Poll-only firstDelay remains zero; execution timing is unchanged.
- **Why**: Adopted the docs-only recommendation to preserve immediate poll-only entry and accurately describe waits after submission and between later polls.
- **How verified**: The driver ran all 47 tests in AdmissionSuite and SubmissionSuite, including a new public-transformer timing-contract regression. Main/test style and compile, codegen, Black, and whitespace passed. All four generated Python descriptions contain the corrected wording. Earlier regeneration evidence did not establish the semantic accuracy of the old description.
- **Evidence**: Final local snapshot5 and validation logs are recorded in the sibling GPT artifact's Final driver disposition. This is the same documentation finding reported by GPT and Grok. The original Low severity and ISSUES_FOUND verdict remain unchanged; no third model review was run.