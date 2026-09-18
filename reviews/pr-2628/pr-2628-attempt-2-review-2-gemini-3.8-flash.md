# PR 2628, attempt 2, round 2 (Architecture & Patterns)

## Review Summary
- **Round**: 2
- **Theme**: Architecture & Patterns
- **Mode**: sequential
- **Model**: gemini-3.8-flash
- **Artifact**: direct-pr-2628\reviews\pr-2628\pr-2628-attempt-2-review-2-gemini-3.8-flash.md
- **Issues Found**: 0
- **Verdict**: CLEAN

## Evidence Checklist
- [x] Read root `AGENTS.md` and verified repository convention adherence: no generated file edits under `target/`, no RDD-based implementations, no hardcoded credentials, and preservation of public JVM signatures.
- [x] Inspected the plan domain architecture in `scripts/release/release_matrix.py`: clean separation of concerns, immutable dataclass models (`ReleasePlan`, `TargetPlan`), canonical JSON hashing for `plan_id` (`sort_keys=True, separators=(',', ':')`), robust derived coordinate and parameter reconstruction, and fail-closed validation in `load_plan`.
- [x] Inspected module coupling and dependency direction across the public release toolset: `release_matrix.py` (domain model / serialization), `release_guard.py` (CI workflow and build guards), `release_ops.py` (driver / state machine / reconciliation), `verify_release.py` (verification / inventory), and `bump_bbcvhd.py` (BBC-VHD rollout manipulation). Lazy imports in `verify_release.py` (`from release_ops import ReleaseError, validate_producer_evidence, verified_evidence`) avoid circular dependencies between verification and driver operations.
- [x] Verified resolution of round-1 findings:
  - PUB-1: `scripts/release/release_guard.py:237` queries `target.branch` rather than `target.base_branch`, properly validating all release branches (`master`, `spark4.0`, `spark4.1`) without querying `refs/heads/None`.
  - PUB-2: `scripts/release/bump_bbcvhd.py:108-144, 207-248` implements paired plan composition (`--oss-plan`, `--oss-evidence`, `--approve-oss-plan`), enabling independently published staged full-release UPacks to update BBC-VHD components once with matching OSS base, runtime, counter, and destination.
  - PUB-3: `build.sbt:207-210` removed `--skip-existing` from `twine upload` and passes credentials via environment variables (`TWINE_USERNAME`, `TWINE_PASSWORD`); `release_guard.py:114-160, 214` and `release_ops.py:1621-1629` require and validate the PyPI wheel identity and hash in primary Maven provenance receipts.
  - PUB-4: `.github/workflows/release-prepare.yml:197-206` generated PR body correctly specifies staged OSS-first publication, directing maintainers to publish the approved OSS-only plan before requiring dependent Internal PR merges.
- [x] Verified atomic state management and failure containment in `scripts/release/release_ops.py`: `StateStore` implements atomic compare-before-replace writes via unique temporary files and `os.replace` with fsync; `_probes` and `_dependency_present` support staged and Internal-only releases by querying existing OSS Maven artifacts read-only without initiating OSS builds.
- [x] Verified build versioning and storage safety in `project/ReleaseVersion.scala`, `project/build.scala`, and `pipeline.yaml`: snapshot coordinates default for ordinary CI runs; release coordinates require explicit release environment hints and clean git checkouts matching approved tags and commits; Azure blob upload verifies `ReleaseVersion.mayOverwrite` and checks remote absence before non-snapshot uploads.
- [x] Verified ESRP staging in `tools/esrp/prepare_jar.py`: clean staging into a temporary directory outside the Ivy cache without mutating source directories, validating module POMs, JAR classes, and version consistency.
- [x] Verified workflow orchestration and verification guards in `.github/workflows/release-*.yml`: workflows enforce `release_guard.py full-release` and atomic tag pushes with remote confirmation, maintaining strict public-only boundaries for release notes.
- [x] Verified test suite collection (415 tests collected cleanly) covering all matrix, guard, driver, evidence, workflow, and BBC-VHD components without executing network or service tests.

Clean review round: zero issues found.

## Reviewed source state

The review covered the uncommitted implementation above head
`243694bccff1d8de0903d813993cdf838f8bd371`, targeting master
`8c7143875c843c649a817cf3e8ba9c7bee23689c`. Test collection is not test
execution or current-head CI approval.
