# Round 2 review

## Review summary

- **Round:** 2 of 6, architecture, abstraction quality, conventions and cross-repository consistency.
- **Mode:** Sequential, direct, one bounded round.
- **Model:** `gemini-3.8-flash`.
- **Reasoning effort:** `high`.
- **Reviewed HEAD:** `9dea133c0820d9e27e68ed04661b18d948575e65`.
- **Target context:** `master`.
- **Artifact:** `reviews/pr-2628/pr-2628-attempt-3-review-2-gemini-3.8-flash.md`.
- **Issues found:** 0.
- **Verdict:** CLEAN.

The review inspected the current dirty delta and its surrounding contracts against architectural principles, abstraction boundaries, coding standards, and cross-repository consistency. No production source was edited.

## Reviewed delta

Before adding this report, `git diff HEAD` contained 10 files, 705 insertions and 55 deletions:

- `reviews/pr-2628/README.md`
- `scripts/bump-version.py`
- `scripts/release/README.md`
- `scripts/release/release_guard.py`
- `scripts/release/release_matrix.py`
- `scripts/release/release_ops.py`
- `scripts/release/test_release_guard.py`
- `scripts/release/test_release_ops.py`
- `scripts/release/test_release_plan.py`
- `scripts/test_bump_version.py`

SHA256 of `git diff --no-ext-diff --no-textconv --binary HEAD`:
`172cc6b877e17091a123ebbec0a1b976953cbd7baf6547ac0449c0c0c4fa5348`.

HEAD and the dirty file inventory were verified before writing. The single untracked file was the prior Round 1 report.

## Evidence checklist

- [x] Read `AGENTS.md`, the repository coding standards, `build.sbt` and `environment.yml`. The delta adheres strictly to repository conventions: no JVM signatures are modified, no runtime dependency pins are altered, and all tooling changes are confined to Python scripts, unit tests, and markdown documentation.
- [x] Verified authoritative plan generation architecture in `scripts/release/release_matrix.py:595-617,779-783,816-837`. The `--output` option guarantees non-destructive persistence using POSIX-compliant exclusive file creation (`os.O_CREAT | os.O_EXCL | os.O_WRONLY`) with explicit stream flushing and `os.fsync`. Existing plans and ledgers cannot be overwritten, and parent directories must already exist.
- [x] Verified abstraction reuse of `parse_plan_json` across admission boundaries. `scripts/release/release_matrix.py:604-605` centralized duplicate-rejecting JSON decoding (`_unique_object`), directly reused by `read_plan` (both file and stdin) and imported by `scripts/release/release_guard.py:18-25,71` for public Maven payload decoding. Eliminates parsing divergence and enforces duplicate rejection across all entry points.
- [x] Verified state machine architecture and ledger conventions in `scripts/release/release_ops.py:2973-3024,3129-3180`. `_reconcile` cleanly encapsulates a single evaluation pass within the `StateStore` context manager. Locks are acquired exclusively during reconciliation and released before `sleep`, preventing lock starvation during waiting.
- [x] Verified deadline enforcement and rollback architecture in `scripts/release/release_ops.py:2518-2563`. In `_queue`, a pre-intent snapshot is taken before state mutation. If durable persistence of the submission intent exhausts the deadline (`monotonic() >= deadline`), the state is rolled back in place with the current revision and persisted, avoiding unsubmitted ghost jobs while preserving caller state references.
- [x] Verified CLI abstraction consistency in `scripts/release/release_ops.py:3034-3049,3099-3121`. Clean separation between read-only monitoring (`status --wait`) and approved execution (`resume --apply --approve-plan ... --wait`). Contradictory options (`--wait` combined with `--retry`, `--adopt`, or `--inspect-lock`) are rejected early before probing or state acquisition.
- [x] Verified plan immutability enforcement during bounded polling in `scripts/release/release_ops.py:3141-3148`. Every polling pass re-derives the plan file and verifies that its computed `plan_id` matches the initially approved plan, aborting if the plan was modified.
- [x] Verified repo-relative path exclusion architecture in `scripts/bump-version.py:157-162,232-234,237-246,643-649`. `_denylisted_path` uses parent hierarchy traversal to exclude `scripts/release/` in both file discovery and post-write verification sweeps. Protects fixed release tooling and test fixtures without risking broad basename exclusions that could skip root README or live code paths like `core/release/runtime.py`.
- [x] Verified operator guidance alignment in `scripts/release/README.md:39-68,192-229,231-255`. Consistently prescribes separate release directories per version/track (`../release-runs/<version>/<track>`), `--output` exclusive plan creation, resuming only with original directories/ledgers, and bounded waiting constraints.
- [x] Verified privacy boundary: This public review contains strictly public code, file paths, and facts. No private paths, private commit SHAs, or private implementation details are included.

## Architectural evaluation

1. **Single Responsibility and Dependency Direction:**
   - `release_matrix.py` remains the authoritative source for plan schema definition, validation, and serialization.
   - `release_guard.py` acts strictly as an admission gate, depending directly on `release_matrix` abstractions (`parse_plan_json`, `load_plan`) rather than re-implementing or diverging from plan parsing logic.
   - `release_ops.py` serves as the execution coordinator and state store manager, operating against immutable `ReleasePlan` objects and managing directory-local state persistence.

2. **State and Concurrency Robustness:**
   - State transition boundaries (`planned` -> `unknown` -> `pending` -> `complete`/`failed`) are rigorously maintained.
   - The rollback mechanism for post-save deadline exhaustion restores the known pre-intent state in place, ensuring memory references held by callers remain synchronized with disk.
   - Lock management guarantees that file locks are never held across polling sleep intervals, enabling concurrent read-only status inspections while preventing deadlock.

3. **Blast-Radius Discipline:**
   - `bump-version.py` excludes only the intended tooling path (`scripts/release/`) using repo-relative POSIX path matching, preserving surrounding build and release infrastructure without unintended side-effects.

## Validation and scope limits

Existing offline test evidence covers the architectural boundaries:
- 354 public driver, guard, and plan test cases passed.
- 49 focused wait, deadline, duplicate-rejection, and payload admission cases passed.
- 228 version-bump tests passed, including successive bump integrations and scanner exclusion tests.
- Black 22.3.0 verified formatting across all changed Python files.
- `git diff --check HEAD` passed with zero whitespace or line-ending defects.

Scope limits:
- Review conducted via static analysis, code inspection, and offline contract verification.
- No live publication, tagging, network requests, CI mutations, or remote API calls were made.
- Remote CI checks remain old-head evidence and require re-validation upon final branch push.

## Verdict

**CLEAN_FOR_NEXT_ROUND.** Architecture, abstraction layers, repository conventions, and design patterns are clean and consistent. Zero issues found.

## Public derivative-tag recovery follow-up

**CLEAN for the reviewed delta.** Reviewer: `gemini-3.8-flash`, reasoning effort `high`.
Reviewed against `fcbe55b7875a4cc8e66b5870e93e01d26c510490`.
Scope was limited to `.github/workflows/release-tag.yml`,
`scripts/release/test_release_tag_recovery.py`,
`scripts/release/test_release_ops.py`, and `scripts/release/README.md`.

### Architecture and repository pattern evaluation

- [x] **Separation of concerns and encapsulation (`.github/workflows/release-tag.yml:173-225`):**
  `reconcile_target_tags` encapsulates target-to-runtime mapping (`spark4.0` -> `3.12`,
  `spark4.1` -> `3.13`), authorization checks, reachability validation, tag immutability,
  atomic push, and remote ref confirmation into a cohesive, reusable Bash helper.
  It cleanly decouples target tag reconciliation from branch creation and rebase orchestration.
- [x] **Authorization and approval boundaries (`.github/workflows/release-tag.yml:183-207,267-283`):**
  Follows core repository safety principles: reruns cannot mint tags at arbitrary moving branch tips.
  Missing tags are authorized solely by a recorded same-repo merge commit (`$MERGED_SHA`)
  verified to be an ancestor of `origin/$TARGET`. For legacy contained releases without a recorded
  PR, both tags must already exist, agree on the same commit, and remain on the target branch.
  Otherwise, the workflow aborts rather than guessing an unreviewed commit.
- [x] **Tag immutability and atomic publication (`.github/workflows/release-tag.yml:198-216`):**
  Existing tags are strictly verified against `$EXPECTED` and never moved (`Refusing to move a published release tag`).
  Missing tags in a pair are created and pushed together using `git push --atomic origin`, preventing
  partial or desynchronized tag pairs if concurrent updates or push rejections occur.
- [x] **Remote verification contract (`.github/workflows/release-tag.yml:217-224`):**
  The workflow does not conflate local tag existence with remote completion. It executes
  `git ls-remote --tags origin` and verifies dereferenced commit matches (`^{}`) before reporting success.
- [x] **Open PR preservation (`.github/workflows/release-tag.yml:239-251`):**
  Open PRs are detected and preserved first, ensuring active PRs and manual conflict resolutions
  are neither overwritten nor tagged prematurely.
- [x] **Test architecture and fidelity (`scripts/release/test_release_tag_recovery.py`):**
  The test suite parses `.github/workflows/release-tag.yml` via `yaml.safe_load` and executes the
  actual workflow step against isolated, temporary bare Git repositories. Mocks are restricted to
  a lightweight `gh` reader. No production code is duplicated into test scripts.
- [x] **Credential safety and token caching (`scripts/release/test_release_ops.py:2944-2961`):**
  The mocked request tests exercise `AzureRemote._get` across all three allowed Azure DevOps hosts
  using an in-memory `Opener`, confirming
  Authorization header transmission using the cached token without network requests, credential leaks, or hardcoded secrets.
- [x] **Documentation consistency (`scripts/release/README.md:76-84`):**
  Operator guidance precisely documents orchestrator rerun behavior: atomic missing tag creation
  at recorded merge SHAs, immutability of existing tags, legacy pair consistency, and aborting on discrepancies.

### Validation and evidence

- Extracted workflow step validated with `bash -n` (syntax check passed cleanly).
- Python formatting validated with pinned Black 22.3.0 across changed test files without modification.
- Whitespace validation (`git diff --check`) passed with zero defects.
- Offline regression suite:

```text
python -m pytest -q -rs -p no:cacheprovider scripts/release/test_release_tag_recovery.py scripts/release/test_release_ops.py::test_direct_azure_reads_send_the_cached_token
```

Result: **25 passed, no skips**. All 25 regression cases (covering linear and cherry-picked histories,
partial pairs, annotated tags, legacy branches, remote push rejections, open PR preservation,
mismatches, and remote ls-remote verification) pass cleanly in an isolated test environment.

### Verdict

**CLEAN for the reviewed delta.** Architecture, abstraction layers, authorization boundaries,
and repository patterns conform strictly to SynapseML standards. Zero issues found; no fixes required.
