# Edge cases and robustness review

**PR:** microsoft/SynapseML#2628
**Head:** `7bc3019409803a3eaf33e9b68b4f84debb5f4846`
**Base:** `861c3a1e14a9511b5604563ff1e3976cefa82e90`
**Assignment:** lens 3, fleet attempt 1, stage attempt 1
**Model:** harness-selected; exact reviewer model ID was not supplied in the assignment
**Verdict:** changes requested. Two P1 and three P2 findings reproduced locally.

## Scope and method

Read the worktree's `AGENTS.md`, branch and release guidance, all of
`scripts\release\release_ops.py`, all of `test_release_ops.py`,
`test_release_warnings.py`, and `test_plan_evidence.py`. Traced the relevant
inventory, evidence, receipt-producer and publication-job contracts in
`verify_release.py`, `release_guard.py`, `pipeline.yaml`, related public-evidence
tests, and the operator guide.

This is an independent review of the assigned release-operations scope in the
full PR, not a review limited to the latest incremental commit. The operations
module is new relative to the supplied base. Prior and sibling review reports
were not consulted. No nested reviewers were launched.

Reproductions called the real `release_ops.main` entry point with the existing
`FakeRemote`, disposable ledgers and synthetic fixture configuration. The test
wrapper only captured CLI output and supplied plan/state arguments. Network
entry points and the real CLI transport were blocked during the additional
reproductions. No live Azure/GitHub calls, production submissions or credential
file reads were performed. Only this review artifact was edited.

## Findings

### R1. P1: a fresh ledger republishes a partially existing Maven release

**Location:** `scripts\release\release_ops.py:1745-1753,1776-1793,3109-3129`.

`_artifact_present` requires every required coordinate to be present.
`_observe` changes a never-submitted action to `existing` only when that
all-present predicate is true. A partially published Maven action therefore
remains `planned`, with no blocker, and `_execute` submits the whole immutable
publisher again. This also happens with `--wait`.

The reproduced case has all Maven CDN artifacts, the public wheel and the DBC
already present, but missing Maven Central coordinates. The public CLI submits
another Maven build rather than requiring adoption or investigation. The
pipeline does not publish only the missing inventory rows: it reruns publication
for the selected target.

**Reproduction using the existing `cli` fixture:**

```python
plan = release_plan(repositories=["oss"], families=["maven"])
cli.remote.missing = {("oss", "maven-central")}
code, report, _ = cli(
    plan=plan,
    apply=True,
    extra=["--wait", "--timeout-seconds", "1"],
)
assert code == 1
assert len(cli.remote.queued) == 1
assert report["actions"][0]["status"] == "pending"
```

Observed present artifact kinds were `dbc`, `maven`, and `pypi`; the only
missing artifact kind was `maven-central`. One new queue request was recorded.
`test_release_ops.py:2345-2361` currently asserts this queueing behavior rather
than protecting against it.

This defeats the same-coordinate Maven safety rule enforced by `_retry`.
It is relevant when adopting an interrupted release into the driver, or when
an earlier publisher has produced some outputs before the ledger's first
submission. A new ledger does not make existing immutable coordinates safe to
publish again.

**Fix direction:** distinguish absent, partially present and fully present
publication groups before the first submission. At minimum, any observed
partial Maven publication must block queueing and require recovery/adoption.
Do not treat an aggregate `MISSING` result as whole-namespace absence either;
the existing retry test already demonstrates that a missing POM can hide an
existing JAR. Add the initial-submission regression, not just another
`--retry` test.

### R2. P1: adoption can replace an ambiguous submission with an older run

**Location:** `scripts\release\release_ops.py:2615-2652,2677-2685`; related
timestamp validation at `1931-1934` and retry eligibility at `2922-3003`.

When an action already has a durable submission intent but no returned build
ID, `_adopt` checks the supplied build's plan, source and request parameters
without checking whether that build could have resulted from the recorded
intent. An older matching run that is not in the retired-attempt list is
accepted even when its queue and finish times are a full day before the intent.

This is not only an inaccurate audit record. Adopting that older failed run
changes the ambiguous action to `failed`, making `--retry` available while the
actual accepted submission remains queued.

**Executed reproduction:**

1. Create a synthetic publisher-only plan and run read-only `resume`.
2. Register matching build `90`, mark it failed, and set its queue/finish
   timestamps to a day before the current invocation.
3. Wrap `FakeRemote.queue` so it first executes the original queue function,
   creating build `101`, then raises `OSError` to simulate a lost response.
4. Run approved `resume`. The ledger correctly records an unknown submission
   without a build ID; build `101` exists remotely as `notStarted`.
5. Run approved `resume --adopt publisher.oss.master.upack=90`.
6. Restore the normal fake queue and run approved
   `resume --retry publisher.oss.master.upack`.

**Observed result:**

```text
adoption exit = 1
retry exit = 1
ledger current build = 102
original unresolved build = 101
original unresolved build status = notStarted
new queue requests = 2
```

The ledger retires build `90`, not the real ambiguous submission, and submits
build `102` for the same coordinates while `101` remains live. No ledger
contents were manually changed in this reproduction.

**Fix direction:** distinguish adoption of previously existing artifacts with
no submission intent from recovery of an already-recorded intent. For the
latter, reject runs predating that intent outside a defined clock-skew allowance
and retain the unresolved intent. Keep the retired-build rejection as well.
Where timestamps cannot disambiguate matching runs, require additional
submission correlation rather than allowing adoption to establish retry
eligibility.

### R3. P2: failure of the first state write permanently strands the plan claim

**Location:** `scripts\release\release_ops.py:1680-1686`, with the subsequent
refusal at `1566-1571`.

The first save durably replaces the claim with `initialized: true` before
writing the state file. If that state write fails, the claim asserts that a
ledger once existed even though none was ever committed. Every subsequent
preflight/resume fails with the lost-history refusal.

**Executed reproduction:** on a new public plan, wrap `StateStore._replace`
to delegate claim writes normally but raise `OSError` whenever
`path == store.path`. Run `preflight`, remove the fault injection, then run
`preflight` again with the same plan and state.

```text
first preflight exit = 2
next preflight exit = 2
state exists = false
claim initialized = true
queue requests = 0
next error includes "The claimed ledger is missing"
```

The locks are released, but retrying the public command cannot recover. The
documented recovery procedure forbids deleting the persistent claim, so even a
read-only first preflight can leave the approved directory unusable after a
disk-write failure or process interruption.

**Fix direction:** make initialization recoverable without weakening the
missing-ledger protection for a previously committed state. Persist the initial
state before marking its claim initialized, and finish claim initialization
before any submission can proceed. Test interruption on both sides of the
initial state/claim writes.

### R4. P2: transport errors do not stop ordinary approved resume

**Location:** `scripts\release\release_ops.py:2579-2583,3096-3106,3569-3575`.

`_refresh_group` turns an authoritative read failure into an `unknown` action.
The execution loop stops on that blocker only when `--wait` is set. Ordinary
`resume --apply --approve-plan ...` continues queueing the remaining targets,
then exits `1` rather than the documented transport-error exit `2`.

**Executed reproduction:** use a public plan selecting `master` and
`spark4.1`, with missing Maven artifacts. Wrap the fake queue so that, after
returning a valid response for build `101`, subsequent `build(101)` raises
`ReleaseError("Azure read request failed with HTTP 503")`. Invoke approved
`resume` without `--wait`.

```text
exit = 1
queue requests = 2
action statuses = ["unknown", "pending"]
```

The first post-submission read fails, but the second runtime's publication is
still submitted. This contradicts the operator guide's promise that a read
failure stops the command and the runbook's manual-action rule for unknown
submissions. `test_wait_stops_before_unrelated_queue_after_ambiguous_submission`
only covers the `--wait` variant.

**Fix direction:** preserve the first build ID and unknown outcome, then
propagate transport failure as a command-level blocker before further
submissions, regardless of polling mode. Keep any intended independent handling
of ordinary pre-existing artifacts separate from transport failures. Add the
non-wait public two-target regression and assert both the queue count and exit
status.

### R5. P2: warning evidence accepts job windows outside the producer build

**Location:** `scripts\release\release_ops.py:1931-1934,1987-2044,2544`;
the same evidence collection path is repeated at `3480`.

Task timestamps are checked against their containing job, but the job window
is never checked against the enclosing build's queue/finish window.
`_validate_build` validates those build timestamps individually and discards
them from the returned outcome. The timeline checker receives no enclosing
window to validate.

Consequently, consistently stale job and task records pass the current-run
proof. This is distinct from the covered case where only a task is stale
relative to an otherwise current job.

**Reproduction using the existing warning fixture:**

```python
plan, records, _ = partial_build(cli)
for record in records:
    record["startTime"] = "2020-01-01T00:00:00Z"
    record["finishTime"] = "2020-01-01T01:00:00Z"

code, report, _ = cli("status", plan=plan)
evidence = ops.verified_evidence(plan, cli.state, remote=cli.remote)
assert code == 0
assert report["complete"] is True
assert evidence["complete"] is True
```

The build's queue timestamp remained in 2026. Despite all executed jobs and
required publication tasks claiming completion in 2020, the CLI returned
success and exported `producer-verified` evidence.

**Fix direction:** validate ordered build times and require each executed
warning-proof job window to lie within its producer build window before
accepting its task proof. Apply the equivalent consistency check to exported
warning summaries. Preserve the independent task/job retry counters; requiring
their numeric equality would not solve this problem.

## Tested clean conclusions

The following protections were traced in the public CLI paths and exercised by
the targeted regression suites:

- Approval requires both mutation flags and the exact plan ID. Missing or
  mismatched approval does not queue work.
- State and plan locks prevent competing local writers. The persistent claim
  rejects a second state filename, deletion of an initialized ledger, and
  schema downgrade that would erase retry history. Lock inspection is bounded
  and read-only. These are directory-local guarantees, not distributed locks.
- Intent is saved before submission, and returned IDs are saved before
  follow-up queries. Unknown outcomes are not automatically resubmitted.
  Matching explicit adoption can recover a lost response without another queue
  request. R2 concerns admission of the wrong historical run, not this normal
  recovery path.
- Merely existing artifacts do not produce completion or producer evidence.
  Failed/pending producers, invalid receipts and absent required artifacts do
  not become complete. Known successful builds wait for artifact visibility
  without being queued again.
- Warning acceptance requires allowlisted advisory labels, task IDs and major
  versions, explained job outcomes, and successful required Maven publication
  tasks. Missing, skipped, failed or duplicated required publication tasks are
  rejected. Task retries are independent of job attempts, and task windows
  outside their current job are rejected. R5 concerns the next enclosing
  timestamp boundary.
- Publisher retries retain the failed group and proof, reject retired build
  reuse, require terminal job results and fresh namespace-absence evidence,
  honor the attempt limit, and handle continuation/offset pagination
  conservatively. Same-coordinate Maven `--retry` is refused.
- Polling validates bounds, releases locks between iterations, retains build
  IDs on timeout/interruption, and prevents new submissions after the deadline
  is reached during policy checks or intent persistence. Status polling never
  queues dependent work.
- Evidence export rechecks producer builds and timelines, rejects changes
  observed during collection, binds selected action coverage, preserves actual
  partial-success results, and excludes unrelated service diagnostics.

## Validation and limits

Targeted suite command:

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'
$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD='1'
python -B -m pytest scripts\release\test_release_ops.py scripts\release\test_release_warnings.py scripts\release\test_plan_evidence.py -q -p no:cacheprovider
```

Result: **406 passed**, zero failures or skips, in `860.48s`. The command exited
`0`. These passing regressions do not cover the five reproduced defects above.

The separate in-memory reproduction script completed successfully. It used
`python -B -`, the existing fixture implementations and real CLI calls, and
produced all five finding outputs above. Temporary reproduction directories
were removed after execution.

Formatting command:

```powershell
python -B -m black --check scripts\release\release_ops.py scripts\release\test_release_ops.py scripts\release\test_release_warnings.py scripts\release\test_plan_evidence.py
```

Result: exit `1`; the installed Black `26.5.1` would reformat `release_ops.py`
and leave the three test files unchanged. This is not a result from the
repository-pinned Black `22.3.0`, and is not promoted to a correctness finding.
No formatter writes or dependency installations were performed.

The local interpreter was Python `3.14.6`, not the branch's pinned Python
runtime. No Scala/Spark build, live publisher, network-failure timing exercise,
cross-machine locking test or physical power-loss test was run. Failure and
interruption cases used deterministic local injections; R5 demonstrates a
validator accepting contradictory input, not evidence that Azure has returned
that particular timeline in production. The polling checks establish queue
gating at deadline boundaries, not a hard wall-clock bound for all in-flight
service calls.

Completion here means the assigned review and its artifact, not production
authorization, release readiness, or approval of PR areas outside this scope.

## Implementation resolution notes

The review above is preserved as originally written. The subsequent instruction
authorized fixes for R1-R5, regression tests and this appended resolution record.
The fixes are uncommitted working-tree changes based on the recorded head.
No publication authority was granted or exercised.

### R1 resolved: require absence before the first Maven submission

`release_ops.py` now treats any observed partial Maven publication as existing,
not unsubmitted. That observation remains a blocker if the artifacts later
disappear. When aggregate inventory reports missing, the first submission also
checks every approved Maven filename, including supported classifiers,
signatures and checksums, in both public Maven destinations. The primary PyPI
version and required DBC archive are checked as well. This catches a JAR or
signature left behind when an aggregate POM check reports missing.

The namespace check stops at a 180-second boundary between requests. A failed
read or unconfirmed result creates no submission intent. Confirmed absence
expires after five minutes and is checked again after intent persistence; an
expired proof restores the unsubmitted state without sending a request. Optional
runtime policy is rechecked after the namespace reads, so a policy change during
that work cannot authorize a queue request. Same-coordinate Maven retry remains
forbidden.

Regression coverage includes partial inventory with and without waiting,
aggregate missing with hidden publication residue, JAR/signature/checksum/PyPI/DBC
residue, complete absence, read failure, timeout, policy changes during namespace
reads and expiration during state persistence. Tests call the public CLI and
assert queue counts, durable state and absence of an operation when blocked.

### R2 resolved: retain an ambiguous intent when adoption is too old

`_adopt` rejects a candidate queued more than five minutes before an unresolved
local submission intent. The allowance accommodates clock skew without accepting
the reproduced day-old failed run. Rejection retains the original unresolved
intent and cannot establish retry eligibility. Historical adoption remains
valid when there is no local submission intent.

`test_fleet_r2_old_run_cannot_retire_an_ambiguous_live_submission` reproduces an
accepted request with a lost response, rejects the older failed run, checks that
the ledger bytes are unchanged, rejects retry, and then adopts the actual live
run without another submission. The companion historical-adoption test preserves
the valid no-intent recovery path.

### R3 resolved: commit initial state before initializing its claim

`StateStore.save` now replaces the state before marking its persistent claim
initialized. Claim initialization and state-version maintenance still finish
before queueing. The initialized-claim protection against deletion of an existing
ledger remains in place.

The regression injects write failures before and after replacement of both the
state and claim. Each case recovers through another public preflight without
claim deletion, leaves no stale lock and sends no queue request.

### R4 resolved: read failures stop both resume modes

`ReleaseReadError` distinguishes authoritative-read failures from an ordinary
pending or rejected producer. Build, definition, timeline and provenance failures
record the unknown outcome, preserve a returned build ID, persist the ledger and
propagate a command-level error. Both waiting and non-waiting execution stop
before another action is submitted. The command exits 2 for these read failures.

`test_fleet_r4_read_failure_stops_before_another_public_submission` covers all
four read phases in both modes. Every case records exactly one queue request,
retains build 101 as unknown and leaves the second target without an operation.
Unexpected programming errors are not converted into successful results.

### R5 resolved: bind warning-proof jobs to their producer build

Build validation rejects finish-before-queue timestamps. The warning path
requires each executed job's ordered UTC interval to lie within its build's
queue/finish interval. The same check runs during live refresh, evidence
recollection and validation of exported warning summaries. Existing task-to-job
checks and independent task/job retry counters are unchanged.

The new tests reject jobs and tasks consistently dated before or after their
build, and reject reversed build intervals in exported evidence. Existing genuine
Azure seven-digit timestamps, legacy ISO timestamps, different valid job windows,
independent retry counters and production-sized warning evidence still pass.
No timestamp window was widened to accommodate fixtures.

### Coordinated public artifact contract

The owned operations code now consumes the verifier's
`collect_public_artifact_content`, `validate_public_artifact_content` and
`public_artifact_receipts` contracts. Public schema-2 producer manifests retain
the separate `blob_artifacts` receipt. Completion and public producer evidence
require destination/path-bound SHA-256 and size observations for Maven CDN,
Maven Central and the primary PyPI wheel. These observations are retained in the
ledger receipt and exported evidence.

The shared `FakeRemote` calls the real content collector with mocked byte-download
and PyPI metadata transports. Producer fixtures create the separate Blob receipt
required by `release_guard.maven_receipt`. Tests reject changed bytes at all three
destinations and missing, changed, duplicate or wrongly scoped observations.
Legacy public receipts remain readable but cannot establish fresh completion
without current content proof; retained build IDs prevent a repeat publication.
Existing plan IDs, plan documents and operation identities were not changed.

The three-runtime public-notes fixture now supplies its evidence through a JSON
file on Windows, rather than exceeding the operating system's environment
variable size limit. The GitHub payload-size assertion remains in place.

### Regression commands and results

Commands below ran from the worktree root with the existing interpreter and:

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'
$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD='1'
```

Before the production fixes, the new R1-R5 regressions demonstrated the failures:

```powershell
python -B -m pytest scripts\release\test_release_ops.py scripts\release\test_release_warnings.py -q -p no:cacheprovider -k 'fleet_r' --tb=short
```

Result: **18 failed, 3 passed, 389 deselected in 21.48s**, exit 1.

After the initial R1-R5 changes, including existing timestamp compatibility cases:

```powershell
python -B -m pytest scripts\release\test_release_ops.py scripts\release\test_release_warnings.py -q -p no:cacheprovider -k 'fleet_r or azure_timestamps or legacy_iso or task_retry_counter or production_sized_warning or clean_build_keeps' --tb=short
```

Result: **30 passed, 380 deselected in 34.51s**, exit 0.

The first complete owned-suite run encountered the concurrently introduced Blob
receipt contract before these fixtures and operations code were integrated:

```powershell
python -B -m pytest scripts\release\test_release_ops.py scripts\release\test_release_warnings.py scripts\release\test_plan_evidence.py -q -p no:cacheprovider --tb=short
```

Result: **29 failed, 398 passed in 327.93s**, exit 1. The failing public-producer
and notes fixtures lacked the new Blob receipt argument or provenance.

A further failing-before test established that aggregate missing is insufficient:

```powershell
python -B -m pytest scripts\release\test_release_ops.py -q -p no:cacheprovider -k 'fleet_r1_aggregate_missing' --tb=short
```

Result: **1 failed, 327 deselected in 3.72s**, exit 1.

After integrating the separate producer receipts and public content observations:

```powershell
python -B -m pytest scripts\release\test_release_ops.py scripts\release\test_release_warnings.py scripts\release\test_plan_evidence.py -q -p no:cacheprovider -k 'fleet_r or public_release_completes or actual_guard_producer or public_notes_export or production_sized_warning or status_requires_successful or compressed_evidence_round_trip' --tb=short
```

Result: **1 failed, 32 passed, 395 deselected in 42.03s**, exit 1. The only
remaining failure was the Windows evidence environment-variable limit described
above; it was fixed without reducing evidence coverage.

```powershell
python -B -m pytest scripts\release\test_release_ops.py scripts\release\test_release_warnings.py scripts\release\test_plan_evidence.py -q -p no:cacheprovider -k 'fleet_ or public_release_completes or actual_guard_producer or public_notes_export or production_sized_warning or azure_timestamps or task_retry_counter' --tb=short
```

Result: **51 passed, 393 deselected in 52.64s**, exit 0.

The complete owned suites then passed:

```powershell
python -B -m pytest scripts\release\test_release_ops.py scripts\release\test_release_warnings.py scripts\release\test_plan_evidence.py -q -p no:cacheprovider --tb=short
```

Result: **444 passed in 431.59s**, exit 0, with no failures or skips. This run
started before the final post-namespace policy recheck was added. That final
change and its regression were validated separately:

```powershell
python -B -m pytest scripts\release\test_release_ops.py -q -p no:cacheprovider -k 'fleet_r1_policy_is_rechecked_after_namespace_reads' --tb=short
```

Result: **1 passed, 336 deselected in 2.53s**, exit 0.

The final targeted run covered all new regressions and the related policy,
deadline and absence checks on the completed owned changes:

```powershell
python -B -m pytest scripts\release\test_release_ops.py scripts\release\test_release_warnings.py scripts\release\test_plan_evidence.py -q -p no:cacheprovider -k 'fleet_ or policy or deadline or absence' --tb=short
```

Result: **76 passed, 369 deselected in 38.86s**, exit 0.

```powershell
git --no-pager diff --check -- scripts\release\release_ops.py scripts\release\test_release_ops.py scripts\release\test_release_warnings.py scripts\release\test_plan_evidence.py reviews\pr-2628\task-pr2628-fleet-attempt-1-stage-attempt-1-review-3-harness.md
```

Result: exit 0, no whitespace errors. Git printed a line-ending normalization
warning for `test_release_warnings.py`; no normalization command was run.

### Changed files and remaining limits

Only these implementation files and this original review artifact were edited
by this reviewer:

- `scripts\release\release_ops.py`
- `scripts\release\test_release_ops.py`
- `scripts\release\test_release_warnings.py`
- `scripts\release\test_plan_evidence.py`
- `reviews\pr-2628\task-pr2628-fleet-attempt-1-stage-attempt-1-review-3-harness.md`

All five verified findings have regression-backed fixes. Validation used the
existing Python 3.14.6 environment, not a newly installed branch-pinned runtime.
No live Azure/GitHub or artifact-host calls, secret reads, submissions, commits,
pushes or full-file formatter writes were performed. Concurrent collaborator
changes were not reverted or edited.

The namespace check proves absence of approved filenames at observation time;
it is not a cross-machine reservation. Its request-boundary budget does not
interrupt an already-running HTTP request. The all-absent case performs
`len(PUBLIC_MAVEN_MODULES) * 180 + 2` public existence probes for the primary
schema-4 target. Real-service latency and suitability of the 180-second budget
were not measured under this no-network assignment. A timeout blocks publication
rather than weakening absence requirements.

The parent owns combined regression, pinned-runtime validation, coordinated
formatting and CI. The recorded helper integration passed locally; this record
does not claim production qualification or whole-PR readiness.

## Artifact contract follow-up

The artifact owner subsequently specified producer-evidence schema 3 and
destination-specific fixture digests. Public exports now use schema 3 for both
clean and warning producers and reject old public evidence envelopes. Legacy
ledger receipt versions remain readable, and private evidence retains its
existing version. The new schema does not change plan IDs, operation IDs,
manifest identity, warning requirements or timestamp validation.

`FakeRemote` now derives public Maven and wheel digest/size pairs from distinct
synthetic payloads containing the build, destination and artifact path. DBC keeps
its existing separate fixture and verification contract. The new tests first
showed the previous schema and repeated-hash gaps:

```powershell
python -B -m pytest scripts\release\test_plan_evidence.py -q -p no:cacheprovider -k 'uses_schema_three or rejects_legacy_envelopes or fixture_hashes_are_destination_specific' --tb=short
```

Result before these adjustments: **3 failed, 1 passed, 25 deselected in 7.42s**,
exit 1.

```powershell
python -B -m pytest scripts\release\test_plan_evidence.py scripts\release\test_release_warnings.py -q -p no:cacheprovider --tb=short
```

Result after the adjustments: **111 passed, 1 failed in 103.29s**, exit 1.
The remaining failure is the all-three-target, widespread-warning production
fixture's 60,000-character evidence budget. Distinct required-artifact hashes
exposed this additional compression limit; no fixture timestamps, observations
or budget checks were weakened.

Two in-memory measurements used the same failing public fixture without editing
the encoder or evidence. These are diagnostic runs, not passing validations:

```powershell
@'
import base64, copy, gzip, json, sys
sys.path.insert(0, 'scripts\\release')
import pytest, verify_release as verify
original = verify.encode_evidence

def size(report):
    raw = json.dumps(report, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode()
    return len(base64.b64encode(gzip.compress(raw, mtime=0)))

def measured(report):
    print('CURRENT_BASE64_CHARS', size(report))
    ordered = copy.deepcopy(report)
    for run in ordered['producer_evidence']['runs']:
        for doc in run['provenance']:
            for key in ('artifacts', 'blob_artifacts'):
                doc[key].sort(key=lambda item: item['path'])
    print('PATH_SORTED_BASE64_CHARS', size(ordered))
    for run in ordered['producer_evidence']['runs']:
        required = {item['path'] for item in run['public_artifacts']}
        for doc in run['provenance']:
            doc['artifacts'].sort(key=lambda item: (item['path'] in required, item['path']))
    print('REQUIRED_LAST_BASE64_CHARS', size(ordered))
    return original(report)

verify.encode_evidence = measured
raise SystemExit(pytest.main(['scripts\\release\\test_release_warnings.py::test_production_sized_warning_evidence_fits_and_passes_notes_guard[True-True]', '-q', '-s', '-p', 'no:cacheprovider', '--tb=short']))
'@ | python -B -
```

Result: current encoding **61,888** characters; path-sorted manifests **61,496**;
required entries last **61,432**. The unchanged encoder still rejects the report.
The fixture failed in `4.07s`, exit 1.

```powershell
@'
import base64, json, sys, zlib
sys.path.insert(0, 'scripts\\release')
import pytest, verify_release as verify
original = verify.encode_evidence

def measured(report):
    for sorted_keys in (True, False):
        raw = json.dumps(report, sort_keys=sorted_keys, separators=(',', ':'), ensure_ascii=True, allow_nan=False).encode()
        for level in (6, 9):
            for memory in (8, 9):
                for strategy in (zlib.Z_DEFAULT_STRATEGY, zlib.Z_FILTERED):
                    compressor = zlib.compressobj(level=level, wbits=31, memLevel=memory, strategy=strategy)
                    encoded = base64.b64encode(compressor.compress(raw) + compressor.flush())
                    print(f'ENCODED_CHARS sort={sorted_keys} level={level} memory={memory} strategy={strategy}: {len(encoded)}')
    return original(report)

verify.encode_evidence = measured
raise SystemExit(pytest.main(['scripts\\release\\test_release_warnings.py::test_production_sized_warning_evidence_fits_and_passes_notes_guard[True-True]', '-q', '-s', '-p', 'no:cacheprovider', '--tb=short']))
'@ | python -B -
```

Result: sorted canonical JSON with gzip-compatible `zlib` level 9, memory 8 and
`Z_FILTERED` produced **59,524** characters without removing any evidence.
Preserving JSON insertion order with that strategy produced **55,312**.
The unchanged encoder still failed in `3.65s`, exit 1. The artifact owner was
given these measurements to address encoding within the owned verifier.
Combined payload validation and a passing real encoder run remain required.

The schema-3 producer integration and ordinary notes export still pass:

```powershell
python -B -m pytest scripts\release\test_release_ops.py -q -p no:cacheprovider -k 'actual_guard_producer or public_notes_export or public_release_completes' --tb=short
git --no-pager diff --check -- scripts\release\release_ops.py scripts\release\test_release_ops.py scripts\release\test_release_warnings.py scripts\release\test_plan_evidence.py
```

Results: **5 passed, 332 deselected in 9.20s**, and whitespace check exit 0.
The warning-payload compression failure above remains unresolved at this handoff.

### Independent byte fixtures and separate Blob publication

Following the artifact owner's explicit fixture contract, the download fixture
no longer reads hashes or sizes from mutable producer receipts. `FakeRemote`
stores independent URL-to-bytes and PyPI metadata snapshots. Downloads hash the
stored bytes. Editing a producer receipt therefore cannot manufacture matching
observed bytes or metadata.

The actual-producer fixture builds distinct valid Blob POM/JAR content, writes
its intermediate receipt and passes `blob_receipt` to the final producer.
`produced_maven_receipt(..., remote=cli.remote)` explicitly publishes the staged
Central, Blob and wheel bytes into the fake transport. Every owned caller that
uses those receipts for public verification passes the remote. The helper's
default still permits receipt-only tests without claiming published bytes.
The simulated-publication caller in `test_release_rehearsal.py`, outside this
reviewer's ownership, needs the same `remote=cli.remote` argument; that exact
integration change was sent to the artifact owner.

The public-content mismatch test now corrupts stored fixture bytes directly,
without changing the declared receipt. Separate tests change only the receipt
and require rejection against unchanged published bytes at all three
destinations.

```powershell
python -B -m pytest scripts\release\test_plan_evidence.py -q -p no:cacheprovider -k 'download_fixture_does_not_follow_receipt_tampering' --tb=short
```

Before separating the fixture content: **3 failed, 29 deselected in 5.66s**,
exit 1. All three receipt-only mutations incorrectly returned CLI success.

```powershell
python -B -m pytest scripts\release\test_release_ops.py scripts\release\test_plan_evidence.py -q -p no:cacheprovider -k 'fleet_ or actual_guard_producer or public_release_completes or maven_receipt or public_notes_export' --tb=short
```

After the fixture and producer changes: **81 passed, 288 deselected in 78.51s**,
exit 0.

```powershell
python -B -m pytest scripts\release\test_plan_evidence.py -q -p no:cacheprovider -k 'content_mismatch_cannot_complete_or_resubmit or download_fixture_does_not_follow_receipt_tampering' --tb=short
git --no-pager diff --check -- scripts\release\release_ops.py scripts\release\test_release_ops.py scripts\release\test_release_warnings.py scripts\release\test_plan_evidence.py
```

After switching the corruption tests to actual stored bytes: **6 passed,
26 deselected in 11.09s**, exit 0. Whitespace check also exited 0. No verifier,
producer, pipeline or unowned test file was edited by this reviewer.

### Candidate encoder budget proof

The following process-local experiment exercised all four production-sized
warning fixtures, their real validation/decoding/notes paths, the 60,000-character
evidence cap and the 65,535-character combined input cap. Only the gzip strategy
was replaced in memory; the stored source encoder was not edited.

```powershell
@'
import sys, zlib
sys.path.insert(0, 'scripts\\release')
import pytest, verify_release as verify
original = verify.gzip.compress

def filtered_gzip(data, compresslevel=9, *, mtime=0):
    compressor = zlib.compressobj(level=compresslevel, wbits=31, memLevel=8, strategy=zlib.Z_FILTERED)
    return compressor.compress(data) + compressor.flush()

verify.gzip.compress = filtered_gzip
try:
    result = pytest.main(['scripts\\release\\test_release_warnings.py', '-q', '-p', 'no:cacheprovider', '-k', 'production_sized_warning_evidence', '--tb=short'])
finally:
    verify.gzip.compress = original
raise SystemExit(result)
'@ | python -B -
```

Result: **4 passed, 79 deselected in 11.04s**, exit 0. This includes all three
targets with widespread warnings, distinct published-content hashes and genuine
Azure timestamp precision. No proof fields, timestamps, fixtures or limits were
reduced. The artifact owner still must persist the encoder change and validate
the source version; this experiment does not resolve that outstanding gate.

## Final owned-suite verification

The shared verifier now persists the encoder correction by preserving producer
JSON field order before gzip. The complete owned suites were rerun against that
source implementation, without a process-local encoder replacement:

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'
$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD='1'
python -B -m pytest scripts\release\test_release_ops.py scripts\release\test_release_warnings.py scripts\release\test_plan_evidence.py -q -p no:cacheprovider --tb=short
```

Result: **452 passed in 253.10s**, exit 0, no failures or skips. This includes
all R1-R5 regressions, schema-3 public evidence, independent byte/receipt
tampering, actual separate Blob receipts, genuine Azure timestamp fixtures,
and all four production warning combinations with the unchanged 60,000 and
65,535 character budgets. The previously recorded encoding failure is resolved
in the shared source.

No namespace-listing dependency was added to the owned operations code. Its R1
guarantee remains absence of approved publication filenames, with the documented
request-boundary and cross-machine limitations.

The rehearsal owner also added the previously communicated `remote=cli.remote`
argument. Its two related integration cases were verified against the shared
source:

```powershell
python -B -m pytest scripts\release\test_release_rehearsal.py -q -p no:cacheprovider -k 'agent_simulated_publication_resumes_and_verifies_receipts' --tb=short
```

Result: **2 passed, 10 deselected in 12.97s**, exit 0. The rehearsal file was not
edited by this reviewer. The final owned-source/report whitespace check exited
0; only the previously noted line-ending warning was printed.

The owned implementation, fixture integration and regression work are complete.
The parent retains combined regression, pinned-runtime validation, formatting
and CI. No commit, push, live service call or production operation was performed.

### Download-count and freshness confirmation

Added a regression for `status`, non-waiting `resume` and direct
`verified_evidence`. Each runs twice with the same fake remote and instruments
the actual fixture downloader. Every required URL is read exactly once per
invocation, and every URL is read again on the next invocation. The three-target
fixture requires 91 downloads, comprising 31 for primary and 30 for each port.
Refresh-to-export copies already validated observations rather than downloading
them twice. Later wait-poll refreshes deliberately revalidate content; no
persistent remote-instance cache can hide a changed body.

```powershell
python -B -m pytest scripts\release\test_plan_evidence.py -q -p no:cacheprovider -k 'reads_once_and_rechecks_the_next_invocation or download_fixture_does_not_follow_receipt_tampering or content_mismatch_cannot_complete_or_resubmit' --tb=short
```

The first run had **3 failed, 6 passed, 26 deselected in 24.41s**, exit 1, because
the new count assertion mistakenly assumed a single target and expected 31.
The observed 91 URLs were already unique. After deriving the expected count from
the selected targets, the same command produced **9 passed, 26 deselected in
14.55s**, exit 0. This was a test expectation correction, not an implementation
or caching defect. No production code changed after the 452-test full run.

The artifact owner's exact receipt-independence regression was also executed
against the updated fixture, without modifying that test or its assertion:

```powershell
python -B -m pytest scripts\release\test_release_public.py::test_changed_receipt_cannot_redefine_published_fixture_bytes -q -p no:cacheprovider --tb=short
```

Result: **1 passed in 3.01s**, exit 0. Its earlier reported receipt-copy failure
does not reproduce with the current independent public-byte fixtures.
