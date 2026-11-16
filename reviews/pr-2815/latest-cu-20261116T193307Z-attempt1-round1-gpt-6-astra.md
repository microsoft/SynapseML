# Latest CU changes: focused review and fresh live validation

## Review provenance

| Item | Value |
| --- | --- |
| PR | microsoft/SynapseML#2815 |
| Target | `master` |
| Reviewed HEAD | `e353dfceed7ce1f6fabd46e8aa07141cde9259db` |
| Reviewed scope | Nine tracked worktree changes, plus the local-only CU bootstrap |
| Diff SHA-256 | `92d38558f01f6705bee7d95fea8281b12b3c4e6dbb5b0a9f3690f5a3dc87a763` |
| Notebook SHA-256 | `5157cf710b0d348c269c91588f39f91ad99cec3fe7b2b27cce80c27693da5ae8` |
| Local bootstrap SHA-256 | `e8d01892cc7fdd3a78c5ecc7624b27d40fae8fbc2cdf56bdfaaf3c9dce047119` |
| Reviewer | `gpt-6-astra`, direct review |
| Complexity estimate | Low: bounded configuration/default substitutions, fail-closed parsing, notebook display fix, and regression coverage |
| Size | Nine tracked files; 142 insertions and 55 deletions |

This is a focused direct review, not an independent multi-model signoff,
script-owned review harness result, six-round gauntlet, or full PR-readiness
decision. The pre-existing untracked OpenAI setup notebook was not changed or
treated as part of this CU patch.

## Findings

No actionable blocking defects were identified in the reviewed changes.
No additional production-code fixes were necessary.

- Completion examples, model setup, and positive fixtures use `gpt-6.1-sol`.
  Older models remain in deliberate rejection cases. Embeddings remain
  `text-embedding-3-large`, which is not a GPT completion model.
- Removed API/polling settings match the connector defaults: API `2025-11-01`,
  120 polling attempts, 1,000 ms delay, and 600,000 ms duration. Explicit
  `codePoint` encoding remains for supporting-text slicing.
- Default analyzer overwrite refusal, parameter-preserving copying, and
  non-overwriting Parquet writes are preserved.
- Diagnostic helpers derive the default API from its Scala source; runtime
  evidence reads the analyzer's effective API version. The tests cover an
  unrecognized source default failing before remote execution.
- Separating closing schema brackets avoids IPython prompt stripping.
  Notebook tests now apply IPython's input transformation before execution
  and exercise saved-handle recovery without document resubmission.
- The local bootstrap keeps credentials out of notebook source and maps the
  existing Foundry resource without changing shared defaults or deployments.

Non-blocking validation observation: the tracked notebook now uses buyer
`Research`, whereas the local copy still uses `Fabrikam Research`. Both are valid
synthetic inputs, but they are not identical evidence. The fresh run therefore
executed the tracked notebook, injecting only the reviewed local bootstrap.
The user's title and sample changes were preserved.

## Local checks

The following ran through the installed SynapseML WSL environment:

```text
python -m pytest tools/ci/tests/test_content_understanding_notebook.py tools/ci/tests/test_diagnose_content_understanding.py cognitive/src/test/python/synapsemltest/services/test_ContentUnderstanding.py::TestContentUnderstanding::test_semantic_notebook_executes_in_ipython_and_recovers_saved_handles cognitive/src/test/python/synapsemltest/services/test_ContentUnderstanding.py::TestContentUnderstanding::test_semantic_notebook_sends_deployments_only_with_analysis cognitive/src/test/python/synapsemltest/services/test_ContentUnderstanding.py::TestContentUnderstanding::test_semantic_notebook_ci_runner_recovers_without_resubmitting cognitive/src/test/python/synapsemltest/services/test_ContentUnderstanding.py::TestContentUnderstanding::test_semantic_notebook_ci_runner_retains_analyzer_service_error cognitive/src/test/python/synapsemltest/services/test_ContentUnderstanding.py::TestContentUnderstanding::test_semantic_notebook_rejects_invalid_configuration_before_service_calls -q
```

Result: **76 passed; 22 subtests passed**. Existing MLflow/Pydantic
deprecation warnings did not affect the result.

- `black --check --extend-exclude 'docs/' .`: passed, 231 files unchanged.
- `git diff --check`: passed.
- The previously successful Scala compilation, targeted CU tests, and style
  checks remain earlier evidence; they were not rerun in this focused review.
  No Scala edits were made during this review.

## Fresh live validation

Run: `local-d1feedaba5f0`.
Analyzer: `synapseml_purchase_order_local_d1feedaba5f0`.

The actual tracked quickstart completed all seven original code cells plus
the local bootstrap with zero notebook cell errors. This was a fresh analysis,
not replay of the previous result. Submission handles and the successful
`poll-001` result were persisted. Additional semantic verification completed,
and Spark was stopped.

Analyzer readback confirmed completion `gpt-6.1-sol` and embedding
`text-embedding-3-large`; the effective API was `2025-11-01`.
This run did not modify resource defaults or existing deployments.

| Field | Expected | Actual | Confidence |
| --- | --- | --- | --- |
| Supplier | Contoso Office Supplies | Contoso Office Supplies | 0.984 |
| TotalAmount | 125.00 | 125.0 | 0.972 |
| RequestedDeliveryDate | 2026-02-01 | 2026-02-01 | 0.989 |
| OrderSummary | Research buys 10 notebooks from the supplier, requested by the delivery date | Research is buying 10 notebooks from Contoso Office Supplies, with delivery requested by 2026-02-01. | 0.817 |

All four fields have supporting text, spans, and page-1 source coordinates.
The summary matches the latest sample, including the changed buyer name.
Confidence is service-estimated, not measured accuracy.

Sanitized evidence is retained in the session's
`files/cu-validation-local-d1feedaba5f0/` directory:

- `Quickstart_semantic_extraction.executed.ipynb`
- `cu-local-result.json`
- `run-metadata.json`
- `reviewed.diff`

The executed notebook and reports passed credential/resource-address scans.
The notebook source hash and complete tracked diff were verified unchanged
after execution. The diff was reproduced with
`git diff --binary --abbrev=9 HEAD`; Windows and WSL Git otherwise choose
different default object-ID abbreviation lengths.

Outcome: **the reviewed quickstart scenario passes**. This single synthetic
document does not establish production accuracy, other modality coverage,
remote CI success, or full PR merge readiness.
