# PR 2628, attempt 2, round 5

## Review summary

- Round: 5 of the sequential six-round review.
- Theme: Testing and coverage, including test completeness, real command/public-path coverage, mock adequacy, assertion strength, meaningful error paths, test isolation, deterministic timing, mutation-sensitive tests, and producer/consumer contracts.
- Mode: Sequential, read-only source review.
- Actual model: `gemini-3.8-flash`, reasoning effort `high`.
- Work: <https://github.com/microsoft/SynapseML/pull/2628>.
- Target: `master` at `8c7143875c843c649a817cf3e8ba9c7bee23689c`.
- Committed head: `243694bccff1d8de0903d813993cdf838f8bd371`.
- Reviewed baseline: That head plus the frozen working-tree implementation, including untracked production and test files. The round-5 inventory contains 31 public source files, captured at `2026-09-05T10:35:10.0810800Z`.
- Issues: 0.
- Verdict: **CLEAN** for this scope and theme.

This report contains only public source and public contract details.

## Evidence checklist

- [x] Read this checkout's `AGENTS.md`, master branch guidance, and public release skill (`.github/skills/synapseml-release/SKILL.md` and references).
- [x] Verified SHA-256 and byte size for all 31 listed public source files before and after inspection against `review-5-source-manifest.json`. The committed head was unchanged. In particular, `scripts/release/release_ops.py` remained 124298 bytes with SHA-256 `23f7124dc82c7021cc4878fc398b26c9d31855963e4072b23768209ce4e402c5`.
- [x] Inspected the public driver test suite in `scripts/release/test_release_ops.py` (104 test functions, 3110 lines):
  - Verified real command/public-path coverage: all subcommands and parameters (`preflight`, `status`, `status --inspect-lock`, `resume`, `resume --apply --approve-plan`, `resume --adopt`, `resume --retry`) are invoked through `ops.main(argv)` across successful and rejected paths.
  - Verified mock adequacy: `FakeRemote` faithfully simulates Azure DevOps API endpoints (pipeline definitions, runs, timelines, feed packages, manifests, and blobs); `AzureRemote` unit tests directly exercise real `urllib` request construction, query parameter serialization, header parsing, HTTP error mapping, token redaction, and redirect security.
  - Verified round-4 regression coverage: inspected `test_r4_absence_reads_offset_pages_until_the_collection_ends`, `test_r4_absence_never_accepts_unfinished_offset_enumeration`, `test_r4_absence_freshness_uses_the_oldest_observation`, `test_r4_retry_rechecks_freshness_at_submission_and_preserves_failed_attempt`, and `test_r4_expiry_restore_failure_keeps_history_and_never_submits`. Tests thoroughly validate offset-only pagination (`$top=100`, `$skip` progression, empty page termination), pagination cycle/bound enforcement, collection start timestamps, freshness rechecks at the 300s/301s boundary, and atomic rollback with preserved attempt history upon save failure.
  - Verified assertion strength and mutation sensitivity: assertions check exact state schemas, attempt history arrays, SHA-256 digests, positive build IDs, and specific error substrings rather than truthiness or broad catch-alls.
  - Verified meaningful error paths: tests assert fail-closed handling for malformed manifests, unauthorized feeds, corrupt lock files, competing ledger claims, and unexpected adapter exceptions.
  - Verified test isolation and deterministic timing: tests utilize `tmp_path` fixtures for state/plan/lock isolation, `monkeypatch` for environment variables, and the `controlled_clock` fixture for exact simulated time advancement without wall-clock sleeps.
- [x] Inspected rollout tests in `scripts/release/test_bump_bbcvhd.py` (33 test functions, 821 lines):
  - Tests verify single and paired rollout modes (`--plan`, `--evidence`, `--approve-plan`, `--oss-plan`, `--oss-evidence`, `--approve-oss-plan`), ensuring independent UPack counters are maintained, mismatched approvals or invalid plans fail closed before writes, rollback preserves files on disk, and CRLF line endings are preserved.
- [x] Inspected inventory and verification tests in `scripts/release/test_verify_release.py` (32 test functions) and `scripts/release/test_plan_evidence.py` (11 test functions):
  - Verified coverage of `verify_release.py`: ADO token acquisition, JSON GET fallback handling, HEAD vs GET presence checks, exact feed package lookups, continuation token pagination, CDN/Maven Central/PyPI coordinate validation, and GitHub commit peeling.
  - Verified coverage of `test_plan_evidence.py`: bound report generation, required artifact family selection, draft plan failure before auth, rejection of inventory-only approval promotion, bounded compressed evidence export, and rejection of Internal bindings in GitHub export.
- [x] Inspected plan generator tests in `scripts/release/test_release_matrix.py` (30 test functions) and `scripts/release/test_release_plan.py` (12 test functions):
  - Verified coverage of plan generation, canonical JSON serialization, SHA-256 digest sealing, parameter flag derivation, draft plan rejection, rehash tampering detection, and coordinate validation.
- [x] Inspected CI guard and staging tests in `scripts/release/test_release_guard.py` (12 test functions), `scripts/release/test_esrp_staging.py` (2 test functions), and `tools/ci/tests/test_pipeline_yaml.py` (39 test functions):
  - Verified coverage of ESRP directory staging (avoiding cache pollution, validating flat output, requiring test JARs and POMs), release notes guard, PyPI collision refusal, and pipeline YAML contract validation.
- [x] Verified test execution boundaries and offline constraints: no service calls, credential access, pipeline queueing, package publication, Git mutations, or actual releases occurred during this review.

Local commands actually used included:

```powershell
$root='C:\Users\singhrana\.copilot\session-state\16e6d9b2-ce73-41f9-9d38-9386edc5c48d\files\direct-pr-2628'
git --no-pager -C $root status --short
git --no-pager -C $root rev-parse HEAD
```

Inline Python commands verified manifest SHA-256 digests and byte counts, parsed AST test definitions, and checked for unasserted or weak testing constructs.

## Testing and coverage evaluation

### 1. Test completeness and command coverage
The public test suite covers all public CLI entry points and lifecycle transitions across `scripts/release/release_ops.py`, `release_matrix.py`, `verify_release.py`, `release_guard.py`, and `bump_bbcvhd.py`:
- `release_ops.py` commands (`preflight`, `status`, `status --inspect-lock`, `resume`, `resume --apply --approve-plan`, `resume --adopt`, `resume --retry`) are directly invoked through CLI argument vectors.
- State machine transitions (schema-1 migration to schema-2, intent recording, build ID acknowledgment, completion, failure, adoption, retry history accumulation, and downgrade rejection) are comprehensively tested.
- Round-4 defect fixes are thoroughly covered: offset pagination is validated with active and deleted version collisions past the 100-row boundary, and collection freshness is validated at exact 300s and 301s intervals with state rollback tests.

### 2. Mock adequacy and assertion strength
- `FakeRemote` models the Azure DevOps packaging and build APIs with high structural fidelity, matching JSON responses and query parameters.
- `AzureRemote` includes isolated unit tests verifying real `urllib` interaction, status code handling (401, 404), header extraction (`x-ms-continuationtoken`), error redaction (ensuring tokens and signed URLs are stripped), and redirect protection.
- Assertions verify deep dictionary structures, exact state fields, revision numbers, and explicit error messages rather than weak non-null checks.

### 3. Test isolation and deterministic timing
- All test state files, lock files, temporary repos, and Ivy trees are created inside `tmp_path` fixtures, guaranteeing strict test isolation.
- Timing-sensitive tests (such as absence collection age and policy read delays) avoid `time.sleep()` by using an injected `controlled_clock` fixture that controls UTC timestamps deterministically.

### 4. Producer/consumer packaging contracts
- `release_guard.py` and `test_release_guard.py` enforce flat ESRP Maven layout validation and require the primary PyPI wheel in master receipts.
- `test_verify_cli_consumes_driver_state_as_producer_evidence` ensures the verification CLI accepts driver-produced state as valid producer evidence.
- `test_bump_bbcvhd.py` enforces paired plan evidence validation, ensuring BBC-VHD consumes matching OSS and Internal UPacks without desynchronization.

## Validation limits

This review was conducted as a read-only analysis of source code, test definitions, and offline fixtures. No live Azure DevOps APIs, GitHub Actions, ESRP signing processes, or package feeds were accessed. Supplied test results from earlier phases were noted but not counted as independent execution evidence for this round.

The explicit refusal of same-coordinate Maven retries remains an intentional design requirement.
