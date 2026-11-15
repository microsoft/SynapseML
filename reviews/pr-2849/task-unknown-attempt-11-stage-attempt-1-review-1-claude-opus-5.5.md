---
producer: "review-code"
schema_version: 1
tier: "Medium"
contract: "direct"
execution_backend: "copilot-cli"
attempt: 11
stage_attempt: 1
round: 1
timestamp: "2026-11-15T08:43:31Z"
outcome: "clean"
reviewer_model: "claude-opus-5.5"
input_sha256: "8de59979696f2edfba702d3b9f1966a02184eb57ef6e25e35f0c36cfda8b60d3"
---

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: resolved-plan
- **Model**: claude-opus-5.5
- **Artifact**: task-unknown-attempt-11-stage-attempt-1-review-1-claude-opus-5.5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] **Office MIME map** (`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIAttachmentUtils.scala` L126–155):
  - `OfficeMimeTypes` is declared before `MimeTypeExtensions`, so object initialization order is safe.
  - The reverse map is one-to-one and doesn't collide with existing keys.
  - A data URL goes from its MIME type to `attachment.<ext>` to `"file"` only for the six approved MIME types.
  - ZIP, OLE, alias, macro-enabled and template MIME types fall back to `bin` and stay `unsupported`.
- [x] **Detection order** (same file, L293–305, `inferMimeType`/`fileExtension`):
  - The order is: content sniff from the bytes, then the structured-text check, then the Office extension, then the JVM filename map.
  - The JDK content sniffer has no ZIP check. On the OLE signature it returns a type only for FlashPix files. The structured-text check doesn't match ZIP or OLE headers, so genuine OOXML and OLE files reach the Office lookup.
  - Content starting with `<html` or `<!` stays `text/html` and is sent as text.
  - Content starting with `<table`, `<?xml ` or `{\rtf` is detected as XML or JSON and stays unsupported.
  - For non-Office extensions the Office lookup returns `None`, so the old chain runs unchanged.
- [x] **Admission check** (L320): a file is admitted only if `OfficeMimeTypes.get(extension).contains(mimeType)`.
  - An Office MIME type that the filename map returns for `.docm`, `.dot`, `.zip` or `.bin` is rejected.
  - The PDF, image, audio and text branches are unchanged.
  - Before this patch, an Office-extension path whose content wasn't sniffed could only be `unsupported`, so no previously accepted input changes how it is routed.
- [x] **Extension helper** (L408): the new `fileExtension` behaves the same as the inline code it replaces (`lastIndexOf('.')`, lowercased with `Locale.ROOT`). The existing Turkish-locale test (`OpenAIPromptMultimodalRequestSuite.scala` L168–189) still covers it.
- [x] **Chat path and new error message** (L442–500):
  - Chat (`chatCompletionsDataUrlMessage`, `chatCompletionsMessage`) still rejects Office files and suggests `apiType='responses'`.
  - The new Responses message (L477–479) can only appear when the JVM filename map gives an Office MIME type for a non-matching extension.
  - It shows only the MIME type and extension, never the path.
- [x] **Row-local errors** (`OpenAIPrompt.scala` L498–514): any exception other than a task kill becomes a row-level error, so the new `IllegalArgumentException`s stay on their row.
  - The size limit applies only when `fileSizeLimitMB` is set (L626–627); no default exists anywhere in `cognitive/src/main/scala`.
  - `AIPrompt` inherits the same routing (`AIPrompt.scala` L170) and maps `IllegalArgumentException` to `invalid_request` (L436–448).
  - Attachments only reach the Chat and Responses APIs (`AIRequestAdapter.scala` L22).
- [x] **No downstream filter**: `OpenAIResponses.scala` L507–539 checks `input_file` parts by field name only, with no MIME allowlist. A grep of `cognitive/src/main/scala` found no other place that derives MIME types or extensions and could disagree.
- [x] **Public-transformer tests** (`OpenAIPromptMultimodalRequestSuite.scala` L191–277):
  - For all six fixtures, they assert the exact `input_file` JSON: filename, MIME type and base64 bytes.
  - Each fixture is sent both as a temp path with an uppercase extension and a space in the name, and as a data URL.
  - They also cover Chat and size rejection, HTML sent as text for both APIs, and the exact rejection message for ZIP, OLE and macro-enabled data URLs.
  - All parameterized test names are unique.
- [x] **Path suite and CI** (`OpenAIPromptOfficePathSuite.scala`):
  - The HTTP test (L18–56) closes each exchange and stops the server in `finally`, and asserts the exact URIs requested.
  - The alias (L67), provider-delegated content (L93), 12 generic-mapping (L121) and markup/RTF (L148) cases all restore the JVM `FileNameMap` in `finally`.
  - `build.sbt` L282 and L373 apply `Test / parallelExecution := false` to `cognitive`, so the global map change can't race other suites.
  - `pipeline.yaml` L836–837 runs the `services.openai` package in CI, so the new suite is included.
  - No existing test expected Office files to be unsupported (grep).
- [x] **Notebook** (L20–26, L37, L152): only markdown cells changed. Every claim matches the code:
  - Content sniffing runs first, and HTTP routing uses the URL path rather than `Content-Type`.
  - Hadoop's `Path` percent-encodes `?` into the path, so query strings aren't sent.
  - Data URLs are trusted as declared.
  - The size check uses MiB (`OpenAIAttachmentUtils.scala` L188–197) and has no default.
  - The example sets `10.0`.
  - `check-dead-links.yml` only scans the deployed sitemap, so the new provider link can't block this PR.
- [x] **Not filed as defects**:
  - Already-existing behavior that the contract keeps and documents: the query-string and encoded-URI transport limits, and RTF being sniffed as `application/json`.
  - Optional improvements:
    - Add a fix-it hint to the Office error message.
    - Add an explicit data-URL test for a template MIME type such as `.dotx`; the same `bin` fallback already rejects it.
    - Demote the `### Office documents` heading, which sits directly under the H1 title.
- [ ] **Binary fixtures not inspected**: the review instructions said not to decode them, so OLE streams, OOXML parts, metadata and the absence of macros were not checked. I relied on `cognitive/src/test/resources/office/README.md`, the byte-exact assertions, and the supplied container and metadata check results.
- [ ] **Nothing executed**: this was a read-only review, so no builds, tests or live provider calls were run. The 129 passing tests, style gates, mutation runs and 12 live extractions come from the supplied validation evidence.
- [ ] **Provider details unverifiable**: the 50 MB and 1,000-row limits and the external guide URL can't be checked from this repository.

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


## Publication handoff

- Pull request: https://github.com/microsoft/SynapseML/pull/2849
- Published product commit: `b314bb3828db6d45e7c131d6841aaee66f97b50e`.
- Target: `45578f08e6bacf8475de0e92f1717e761d40f795`.
- Final product manifest SHA-256: `02cc48c833c3f5fa80a7b9fd03942f7aa41190aff6a72575672a41f1c0207379`.
- Original review text and driver dispositions above are preserved.
- This is part of the six-round pre-commit review on that product manifest.
- Remote CI and the final CI-qualified review are separate pending gates.

## Driver pre-commit disposition

All six independent sequential lenses reviewed the same frozen product patch.
Rounds 1 and 4 returned CLEAN. Rounds 2, 3, 5, and 6 returned low-priority
suggestions; their original verdicts and individual evidence-backed driver
dispositions are retained in the corresponding artifacts. No required product
fix remains from this pass. This is not a claim of completed remote CI.

The directory also retains the public-safe superseded attempts 5-10. Three
earlier nonqualifying drafts were excluded from public history because they
contained private local-path information; the originals are retained privately.
They do not count as review of the final patch, and their substantive findings
were not used to suppress this independent complete pass.

Additional validation after round 5: `AIPromptAttachmentSuite` passed all 28
tests on the same product patch, with no canceled, ignored, or pending tests.
This supplements the 129 passing OpenAIPrompt regressions; it is not a dedicated
Office-provider AIPrompt E2E test.
