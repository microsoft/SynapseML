---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 5
stage_attempt: 1
round: 1
timestamp: "2026-11-15T05:27:40Z"
outcome: "issues_found"
reviewer_model: "claude-opus-5.5"
input_sha256: "274fdb2d35a6a27cffc23a2aab25104d1fa0aa1e95277d802bf8b2b87cdac4b9"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: task-unknown-attempt-5-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 1
- **Verdict**: ISSUES_FOUND

## Evidence Checklist
- [x] Traced the main code path in `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala`. The `OfficeMimeTypes` map (L126–133) has unique values. Merging the reversed map into `MimeTypeExtensions` (L152) doesn't overwrite any existing key. `inferMimeType` (L293–300) still runs the JVM stream sniffer and `inferStructuredTextMimeType` before the extension map. `categorizeFileType` (L320) admits a file only when its extension and MIME type form one of the six approved pairs. For data URLs, the extension comes from the declared MIME type. ZIP, OLE, macro-enabled and template MIME types therefore map to `bin` and keep the unchanged message "Unsupported file type: X."
- [x] Checked the JDK `URLConnection.guessContentTypeFromStream` logic against Office file headers. It returns null for ZIP (`PK`) headers. For OLE (`D0 CF 11 E0`) headers it also returns null unless the root CLSID is FlashPix. The 64-byte structured-text check can't match either header. Real Office files therefore reach the extension map, while content starting with `<html`, `<head`, `<body` or `<!` stays `text/html` and is sent as text.
- [x] Checked when the new alias message in `responsesMessage` (L477–479) can fire. An approved extension either gets its exact MIME type (classified as `file`) or content sniffing replaces it with a non-Office type. So the message fires only when the JVM name map gives a non-approved extension an approved Office MIME type. Data-URL messages and Chat Completions messages are unchanged, and both Chat functions still reject the `file` category.
- [x] Checked the callers:
  - In `OpenAIPrompt.scala`, `wrapFileToMessagesList` (L631–653) and `createMessagesUDF` (L499–514) turn failures into row-local errors via `Try`.
  - `validateResponsesApiCompatibility` (L450–456) blocks Responses with AI Foundry models, so `input_file` parts can't reach a chat-only endpoint.
  - `AIPrompt` uses the same pipeline and reports `invalid_request` row errors. `AIRequestAdapter.GenerativeApis` contains only chat and responses.
  - `OpenAIResponses.validateInputFilePart` (L506–520) doesn't restrict MIME types.
  - Template filename placeholders now show `attachment.<ext>` for Office data URLs, which matches the filename sent to the provider.
- [x] Reviewed the tests:
  - The per-extension tests run through the transformer. They assert the exact `input_file` JSON (filename, MIME type, base64 bytes) for uppercase-extension paths and for data URLs, plus Chat rejection and the size limit.
  - Other tests cover HTML staying text, ZIP/OLE/macro data-URL rejection, provider-delegated content checks and the alias messages.
  - The alias tests restore the `URLConnection` file-name map in `finally`.
  - `build.sbt` L282 (applied to `cognitive`) runs suites serially, and `pipeline.yaml` L980–981 runs `testOnly` per package.
  - No other module's source uses the JVM file-name map, and no existing Scala or Python test relied on `attachment.bin` or on Office files being rejected.
- [x] Docs and fixtures:
  - The quickstart text (notebook L16–24 and L35) matches the implemented behavior.
  - The new provider link uses the same domain style as existing `OpenAI.ipynb` links, and a lychee dead-link workflow exists.
  - The `columnTypes` and `fileSizeLimitMB` parameter docs and the AIPrompt docs don't list attachment types, so nothing there went stale.
  - Under `.gitattributes` `text=auto`, all six fixtures are stored as git binary patches.
  - Fixture loading follows the existing `getResource(...).toURI` pattern.
  - A count-only ASCII search of the legacy DOC/XLS/PPT fixtures found no drive-letter, `/home/`, `/Users/`, `file:///`, `/tmp/` or `AppData` path fragments.
- [ ] Not checked: UTF-16LE metadata strings inside the legacy fixtures. The search tool can't match NUL bytes, and the plan says to rely on the fixture documentation rather than decoding binaries.
- [ ] Not run: tests, builds and live provider calls. This was a read-only review; the supplied validation evidence was used as context only.

## Issues

### Issue 1: Quickstart size-limit wording suggests SynapseML enforces a per-file limit by default
- **Severity**: Low
- **File**: docs/Explore Algorithms/OpenAI/Quickstart - Multimodal OpenAI Prompter with Responses API.ipynb
- **Line(s)**: 24 (markdown cell `2bd4cab2`, last paragraph)
- **Description**: The new paragraph says direct file inputs "must be under 50 MB each and within the 50 MB combined request limit; the combined limit is enforced by the service." Because only the combined limit is called service-enforced, readers will assume SynapseML enforces the per-file limit. It doesn't by default:
  - `OpenAIPrompt.attachmentSizeLimit` (`OpenAIPrompt.scala` L626–627) returns `None` unless `fileSizeLimitMB` is set.
  - The parameter has no default, and `AIPrompt` doesn't add one.
  - So an oversized Office file is fully read, base64-encoded and uploaded before the provider rejects it.
  - When the limit is set, it is measured in MiB (`validateFileSize` multiplies by 1024 × 1024), so `setFileSizeLimitMB(50)` would allow files up to 52,428,800 bytes.
- **Risk**: Users sending large Office documents may count on a limit check that doesn't exist. That means wasted executor memory, upload traffic and time, ending in a provider rejection instead of a fast local row error. There's no security impact, and errors stay row-local.
- **Suggested Fix**: Say that SynapseML checks attachment size only when `fileSizeLimitMB` is set (as the configuration cell later in the notebook does), and that otherwise the service enforces both limits. Recommend a `fileSizeLimitMB` value well below the provider's per-file limit, and note that it is measured in MiB.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Open
- **What changed**: Pending
- **Why**: Pending
- **How verified**: Pending

## Driver Decision

{
  "tier": "Medium",
  "source": {
    "tier": "primary estimate",
    "themes": "repository six-round policy",
    "execution": "repository sequential override",
    "model": "installed capability-first complementary-family rule"
  },
  "changeSize": "Bounded production MIME classification plus fixtures, request regressions and documentation.",
  "cyclomaticComplexity": "Low added branching; moderate API, platform MIME and package-integration interaction.",
  "rationale": "Six independent complementary-reviewer lenses cover the bounded patch. Publication and dependency sequencing are tracked separately.",
  "primaryModel": "gpt-6-astra",
  "reviewers": ["claude-opus-5.5"],
  "reasoning": "max",
  "contextTier": "long_context",
  "capabilitySource": "https://platform.claude.com/docs/en/models/overview",
  "capabilityEvidence": "Anthropic recommends Opus 5.5 for long-running agentic coding. It is the strongest eligible Anthropic coding option in the current task model catalog.",
  "catalogEvidence": "Current task schema includes claude-opus-5.5 with max reasoning and long_context. Actual runtime identity must match.",
  "overrideSource": "SynapseML repository sequential six-round PR-loop policy",
  "stageGate": "All six lenses on one frozen patch; source fixes consume the pass.",
  "bounds": "Existing per-PR checkpoint counters remain in force; 20-minute per-review process-tree deadline.",
  "artifactPolicy": "Public-safe session drafts before a PR exists; numbered PR review directory after publication."
}

## Driver disposition after round 1

The original review above is preserved. Issue 1 is resolved by explicitly stating
that the service enforces both provider limits, that the optional client check
only applies when configured, and that `fileSizeLimitMB` uses MiB. The quickstart
recommends its existing conservative `10.0` example. No defaults, size arithmetic,
executable notebook cells, production source, or tests changed.

The notebook structure and unchanged code cells were checked. The 115-test,
compile, style, and package evidence remains applicable to the byte-identical
Scala sources and tests. All 57 reviewer tool calls were audited; none opened
earlier feedback or checkpoints. This documentation change consumes the pass,
so no complete clean review is claimed.


## Publication handoff

- Pull request: https://github.com/microsoft/SynapseML/pull/2849
- Published product commit: `b314bb3828db6d45e7c131d6841aaee66f97b50e`.
- Target: `45578f08e6bacf8475de0e92f1717e761d40f795`.
- Final product manifest SHA-256: `02cc48c833c3f5fa80a7b9fd03942f7aa41190aff6a72575672a41f1c0207379`.
- Original review text and driver dispositions above are preserved.
- Superseded review retained for history; it does not qualify the final product manifest.
- Remote CI and the final CI-qualified review are separate pending gates.
