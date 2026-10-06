## Review Summary

**ISSUES_FOUND: 1 finding (P2).**

| Field | Value |
| --- | --- |
| Round | 1 - Broad Sweep |
| Theme | Correctness, security vulnerabilities, logic errors, specification conformance |
| Actual model | `gpt-6-astra` |
| Mode | Sequential direct-contract |
| Base / merge base | `9d51ad1acd765b3246bec15517abf6b62e1d5d70` |
| HEAD | `3ce916902329c20d5c37de43d7c43d805f28e748`, with supplied staged archive and verifier corrections |
| Frozen patch fingerprint | `5145256a6e96fcb5097ef87fc7964964b24bc945e067ed709273e0f1aa335a23` |
| Full diff SHA256 | `26eed9253b7fc7694c90ec4552eedcb6d686b9801e8f56f388ed61b962cb4382` |

Reviewed all 55 paths in the supplied merge-base-to-index patch, including the complete implementation and test changes, not only the pending five-file correction. This is an independent round-1 assessment.

## Evidence Checklist

- Read the public/private plan contracts, legacy identity handling, default runtime selection, source/tag guards, publication workflow dependencies, durable execution state, recovery, producer receipts and release-notes gates.
- Read DBC preparation, native round-trip, archive validation, immutable publication and anonymous verification, including the correction that validates directory member paths before skipping directory entries.
- Read the Blob Maven endpoint and historical public-only verification corrections, version resolution, ESRP staging, version-bump recovery, documentation and website changes, and their supplied tests.
- Traced the finding through strict inventory generation, producer-receipt validation and the release-notes workflow. Checked that neither the retained wheel receipt nor the DBC content guard supplies the missing live wheel-availability check.
- Static review only: no tests, services, cloud operations, source edits or Git mutations. The supplied regression and formatting results were not independently reproduced. Built-wheel compatibility across runtimes, production access, signing and publication remain separate release gates; native DBC round-trip does not establish notebook execution.

## Issues

### 1. [P2] Require the public wheel, not just its PyPI release metadata

**Location:** `scripts/release/verify_release.py:349-355` (`Checker.public_pypi`).

**Trigger:** The requested version has a PyPI JSON response with matching release metadata but no expected wheel, for example:

```json
{"info": {"version": "1.2.0"}, "urls": []}
```

The method returns `OK` solely because `info.version` matches; it never inspects the release files. The same helper is used by strict bound verification in `_check_plan`, not just historical diagnostics.

**Consequence:** If the expected wheel is removed after a successful publication, its retained producer receipt can still pass `_validate_manifests`, while this inventory row continues to report success. `release_ops._artifact_present` consumes that status, and the fresh inventory check in `release-notes.yml` uses the same helper. Thus the release can remain complete and pass the notes gate without a downloadable public wheel. `_validate_dbc_content` closes this gap only for DBC archives. This is an artifact-availability error, distinct from the explicitly manual cross-runtime wheel-compatibility gate.

**Minimal fix:** For strict bound verification, require `public_pypi_wheel_name(version)` to appear as a wheel distribution in the response's `urls` and confirm that its download is available before returning `OK`. Preserve the intentionally non-strict historical path. Add regression coverage for an empty file list, a list missing the expected wheel, and an unavailable wheel download despite matching version metadata and an otherwise valid retained producer receipt.

## Driver resolution notes

Six strict-inventory regressions failed before the correction. A historical
metadata-only regression passed and retains that behavior.

Bound verification now requires exactly one expected, non-yanked wheel
distribution and probes its public download. The download URL must identify the
expected file on the HTTPS Python package host. Invalid URLs fail before a
request. Tests also exercise missing live wheels after valid producer receipts
have already been recorded.

This source correction consumes the current pass. A new full review pass on
the final frozen patch is still required.
