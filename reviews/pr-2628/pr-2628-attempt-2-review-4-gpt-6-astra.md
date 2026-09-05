# PR 2628, attempt 2, round 4

## Review summary

- Round: 4 of the sequential six-round review.
- Theme: Detailed correctness, including data flow, exact types, source identity, return handling, and producer/consumer contracts.
- Mode: Sequential, read-only source review.
- Actual model: `gpt-6-astra`, reasoning effort `max`.
- Work: <https://github.com/microsoft/SynapseML/pull/2628>.
- Target: `master` at `8c7143875c843c649a817cf3e8ba9c7bee23689c`.
- Committed head: `243694bccff1d8de0903d813993cdf838f8bd371`.
- Reviewed baseline: That head plus the frozen working-tree implementation, including untracked production and test files. The round-4 inventory contains 31 public source files, captured at `2026-09-05T09:54:14.0876089Z`.
- Issues: 2, both Medium severity and high confidence.
- Verdict: **ISSUES_FOUND**.

This report contains only public source and public contract details. Neither finding challenges the deliberate refusal of same-coordinate Maven retries.

## Evidence checklist

- [x] Read this checkout's `AGENTS.md`, the master branch guidance, and the public release skill and command reference.
- [x] Verified SHA-256 and byte size for all 31 listed public source files before and after inspection. The committed head was unchanged. In particular, `scripts/release/release_ops.py` remained 122490 bytes with SHA-256 `4ace14b5824e4cfaac27865febea37fcf24243bd5aee4d33ee578b0ca0bdacb4`.
- [x] Read the complete `scripts/release/release_matrix.py`, `release_ops.py`, `release_guard.py`, `verify_release.py`, and `bump_bbcvhd.py`. Traced canonical schema-1 plans into operation parameters, schema-2 state, grouped retries, receipts, exported evidence, and paired rollout checks.
- [x] Read `build.sbt`, `project/ReleaseVersion.scala`, `project/build.scala`, the relevant existing `project/CodegenPlugin.scala` tasks, and `environment.yml`. Traced snapshot selection, explicit release hints, PyPI error propagation, and Maven staging.
- [x] Read the release-related `pipeline.yaml` jobs and their target-baseline diff, `tools/esrp/prepare_jar.py`, and `.github/workflows/release-{prepare,tag,tag-spark,notes}.yml`. Inspected the `pr-validation.yml` diff. Matched the ESRP directory producer to the post-publication receipt argument and the primary wheel path.
- [x] Inspected actual tests in `scripts/release/test_release_ops.py`, including its inert adapter, retry/history/claim tests, queue acknowledgement and exception tests, actual Maven receipt fixture, source/request checks, and publication dependencies. Read the relevant source in `test_release_plan.py`, `test_release_matrix.py`, `test_plan_evidence.py`, `test_release_guard.py`, `test_esrp_staging.py`, `test_release_version.py`, `test_release_workflows.py`, `test_verify_release.py`, and `test_bump_bbcvhd.py`. Inspected the release-related assertions in `tools/ci/tests/test_pipeline_yaml.py`. These are source inspections, not claims that I ran those suites.
- [x] Read `scripts/release/README.md` and all four files under `.github/skills/synapseml-release` listed in the frozen inventory. Checked retry scope, ledger ownership, public-only notes evidence, and the original-plan/counter requirements for paired rollout.
- [x] Ran three small offline probes through `python -B -`: slow absence collection, offset-only package pagination, and a delayed policy read between retry authorization and submission. The final probe used the existing public `FakeRemote` and an in-memory store whose `save` called the real `_validate_state`. No probe created files, invoked a subprocess, contacted a service, or submitted a real build.
- [ ] Did not repeat the parent's full public release/pipeline suite or publisher-consumer integration. No SBT, service test, Azure preview, credential lookup, package publication, or release operation was performed in this round.

Local commands actually used included:

```powershell
$root='C:\Users\singhrana\.copilot\session-state\16e6d9b2-ce73-41f9-9d38-9386edc5c48d\files\direct-pr-2628'
git --no-pager -C $root diff 8c7143875c843c649a817cf3e8ba9c7bee23689c -- build.sbt project\build.scala pipeline.yaml .github\workflows\pr-validation.yml
```

The inventory pass also executed local `git rev-parse HEAD`, `git diff --stat 8c7143875c843c649a817cf3e8ba9c7bee23689c`, and `git status --short` with this exact checkout as `-C`. Inline `python -B -` scripts performed the hash checks, AST test-name inspection, and the probes below. Direct file reads and scoped line-number searches supplied the source evidence.

## Findings

### R4-PUB-1: A package page without a continuation header is accepted as complete absence

- Severity: Medium.
- Confidence: High.
- File: `scripts/release/release_ops.py`.
- Lines: 510-541 and 579-598. The result is consumed at 2458-2459.

**Failing condition.** The package-list API returns an offset-paginated result with a full page and no `x-ms-continuationtoken`. The exact selected package is on a later page. `packageNameQuery` is a search, so earlier results can be other packages sharing the requested name prefix.

The request supplies neither `$top` nor `$skip`. The `count == len(value)` check establishes only the returned page's cardinality. At lines 589-590, a missing continuation header nevertheless ends enumeration. The function then returns `status="absent"`, even though it has not inspected the later package page. `includeAllVersions=true` does not make package pagination disappear.

**Observed evidence.** An in-memory offset-paginated endpoint contained 100 `synapseml-extra-*` package rows followed by `synapseml` with the exact requested UPack version marked deleted. It returned at most 100 rows per request and honored `$skip`. I invoked the actual `AzureRemote.absence` and `_validate_absence`, replacing only `_get`. The output was:

```text
offset_pagination {"queries": 1, "queried_offsets": [null], "dataset_contains_requested_deleted_version": true, "reported_status": "absent", "proof_accepted": true}
```

The existing transport test at `test_release_ops.py:754` exercises continuation headers, invalid page counts, and HTTP failures, but its second page is reached only through a continuation header. It does not exercise this offset-only response.

**Impact.** The driver's retry gate can certify an occupied or deleted-reserved coordinate as definitively absent and authorize an unnecessary retry. A downstream producer rejecting that collision does not repair the driver's false absence proof. This is a conditional pagination defect, not a claim that the current production feed already contains this many matching packages.

**Minimal fix.** Request an explicit bounded page size and implement the package API's `$skip` pagination, while retaining supported continuation handling. Require an actual end-of-collection condition, detect repeated pages, and fail closed at the bound. Add an offline case with the exact live or deleted version beyond the first offset page.

### R4-PUB-2: Retry freshness can expire or be hidden before the request is submitted

- Severity: Medium.
- Confidence: High.
- File: `scripts/release/release_ops.py`.
- Lines: 507-605, 2214-2218, 2458-2466, and 2487-2505.

**Failing condition.** Absence collection takes more than five minutes across package queries, or the subsequent full-release policy read takes long enough for an already-checked absence proof to expire.

There are two observable paths to the same freshness error:

1. `AzureRemote.absence` timestamps the entire proof only after all artifact queries finish. A slow later query makes an earlier absence observation look newly collected.
2. `_retry` validates freshness before `_queue`. `_queue` then calls `_policy`, which can perform paginated remote reads, and saves intent before submission. It never rechecks the absence age after that work.

**Observed evidence.** The first probe advanced an injected UTC clock by 601 seconds between the two family queries. The second used a terminal failed grouped operation, the real retry/state validation logic, and a policy adapter that advanced the clock by 601 seconds. No real time delay or service call was needed:

```text
slow_collection {"queries": 2, "oldest_observation_age_seconds": 601.0, "reported_age_seconds": 0.0, "proof_accepted": true}
delayed_retry_policy {"inert_submissions_added": 1, "absence_age_at_submission_seconds": [601.0], "final_status": "pending", "validated_state_schema": 2}
```

The in-memory ledger still passed `_validate_state`. The demonstrated retry used the original approved pip/UPack group and preserved its failed attempt; the defect is specifically the age of the absence evidence, not grouping or history loss.

**Impact.** The driver submits a retry using observations older than its advertised five-minute limit. The recorded proof can conceal that age, weakening both the admission decision and the operator's subsequent audit.

**Minimal fix.** Timestamp the start or oldest constituent observation of the bounded absence scan. Revalidate that observation immediately before submission, after policy reads and other potentially slow work. Refuse or recollect when the interval exceeds five minutes without discarding the recorded failed attempt. Add offline tests for slow multi-family collection and a delay after the first freshness check.

## Resolution log

### R4-PUB-1

- Status: Fixed.
- What changed: Requests now specify a bounded page size and initial offset.
  Enumeration follows continuation tokens or advances `$skip` by the observed
  row count. Absence requires an empty final offset page, not merely a short
  page or missing header. Repeated pages, repeated tokens and the page bound
  fail closed.
- Why: The retry admission contract requires complete enumeration, not a page-local absence result.
- How verified: New regressions place the live or deleted selected version
  beyond 100 prefix matches, check successful complete absence, and reject
  repeated and bounded short-page enumeration. Together with the freshness
  regressions, nine new cases failed before the fixes. All 18 selected new
  and existing transport cases pass after pinned formatting.

### R4-PUB-2

- Status: Fixed.
- What changed: Absence records the start of collection. Retry rechecks that
  time after the policy read and immediately after persisting intent, before
  calling the queue transport. If persistence consumed the allowed interval,
  the driver restores the complete prior failed state, preserves earlier
  attempt history and advances the ledger revision without sending a request.
- Why: The five-minute limit must apply to the observations used for the actual submission.
- How verified: An injected clock tests slow multi-family collection and
  policy/save delays at exactly 300 seconds and at 301 seconds. The boundary
  succeeds, expiry queues nothing, the entire previous failed action remains
  unchanged, and a subsequent fresh retry works. No wall-clock wait or service
  call was used. All 18 selected transport/freshness cases pass.

### Final affected-code run

The complete driver, plan-evidence, verifier and BBC-VHD suites pass all 395
cases after the fixes. This includes an additional rollback-storage-failure
case: no request is sent, the prior failed attempt remains in history, and
ordinary resume cannot duplicate the unknown intent. All four real publisher
consumer cases pass against the updated driver. Pinned Black and whitespace
checks pass. Unchanged pipeline and producer code retains its earlier evidence.
