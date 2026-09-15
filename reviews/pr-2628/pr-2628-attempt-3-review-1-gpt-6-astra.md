# Round 1 review

## Review summary

- **Round:** 1 of 6, broad correctness, security-conscious review and specification conformance.
- **Mode:** Sequential, direct, one bounded round.
- **Model:** `gpt-6-astra`.
- **Reasoning effort:** `max`.
- **Reviewed HEAD:** `9dea133c0820d9e27e68ed04661b18d948575e65`.
- **Target context:** `master`.
- **Artifact:** `reviews/pr-2628/pr-2628-attempt-3-review-1-gpt-6-astra.md`.
- **Issues found:** 2, both Medium.
- **Verdict:** ISSUES_FOUND.

The review inspected the current dirty changes and their coupled implementation, not earlier review conclusions. No production source was edited.

## Reviewed delta

Before adding this report, `git diff HEAD` contained 8 files, 621 insertions and 53 deletions:

- `reviews/pr-2628/README.md`
- `scripts/bump-version.py`
- `scripts/release/README.md`
- `scripts/release/release_matrix.py`
- `scripts/release/release_ops.py`
- `scripts/release/test_release_ops.py`
- `scripts/release/test_release_plan.py`
- `scripts/test_bump_version.py`

SHA256 of `git diff --no-ext-diff --no-textconv --binary HEAD`:
`707ed241df8ce2b4aecc982f1ce7a6c9518441c3608d2607784cec76077c5850`.

HEAD and the dirty file inventory were checked again before writing. There were no untracked files at that point. Coupled reads included `release_guard.py`, `verify_release.py`, `bump_bbcvhd.py`, the public pipeline admission calls, and the existing state, retry, request and receipt code in `release_ops.py`.

## Evidence checklist

- [x] Read `AGENTS.md`, the SynapseML code-review checklist, the `master` branch reference, `build.sbt` and `environment.yml`. The dirty delta changes Python tooling, tests and documentation, not JVM signatures or runtime dependency pins.
- [x] Checked approval and scope boundaries in `release_ops.py:917-1051,3064-3114`. File/stdin plans are re-derived before use; waiting requires status or an explicitly approved resume and rejects retry, adoption and lock-inspection combinations.
- [x] Checked state schema 2, plan identity, checksum validation, directory-local claims, compare-before-replace saves and lock cleanup in `release_ops.py:1071-1650`. Selected offline tests exercised competing ledger names, missing claimed state, retained retry history, durable intent and duplicate-queue prevention.
- [x] Checked read-only status, downstream progression, manual stops, timeout persistence, changed plans and interruption through the current wait regressions. These checks do not prove the final dispatch deadline requirement.
- [ ] No new queue after the wait deadline. A slow durable intent write permits a new request after expiry. See Issue 1.
- [x] Checked exclusive plan creation in `release_matrix.py:811-837`. Existing output is preserved, invalid inputs do not create output, and absent parents fail. File/stdin duplicate-key regressions pass.
- [ ] Reject duplicate JSON members at every plan admission boundary. The public Maven payload decoder still accepts root and nested duplicates. See Issue 2.
- [x] Checked both scanner passes in `bump-version.py:232-254,634-660`. The real two-bump regression preserved fixed release helpers and fixtures while updating `core/release/runtime.py`; similarly named live paths remain eligible.
- [x] Checked request argument construction, receipt/source identity requirements, manual recovery and the README/index delta. The driver still rejects same-coordinate Maven retries and does not treat inventory alone as producer success.

## Issues

### Issue 1: Recheck the deadline after saving submission intent

- **Severity:** Medium.
- **File:** `scripts/release/release_ops.py`.
- **Lines:** `2518-2552`, especially the deadline check at `2520`, durable save at `2539`, and dispatch at `2552`.
- **Description:** `_queue` checks the deadline before constructing and persisting the unknown submission intent. `StateStore.save()` can take the operation beyond the deadline, but the next step still calls `remote.queue`. This starts a new request after expiry, rather than merely allowing an already-started service call to finish.
- **Verified evidence:** An offline CLI probe used the existing `FakeRemote`, a bound OSS-only UPack plan and `--wait --timeout-seconds 30`. It called the real `StateStore.save()`, then advanced the fake monotonic clock to 31 when the first unknown intent had been persisted. The fake queue was called at second 31. The command returned 1, reported timeout, retained build ID 101 as pending, and released all locks. No real queue or service was contacted.
- **Risk:** The operator's bounded scheduling window is not enforced at the irreversible request boundary. A slow filesystem or a process pause can start publication work outside that window. Approval identity remains intact; this is a deadline violation, not a demonstrated approval bypass.
- **Suggested fix:** Recheck immediately after the durable intent save and before dispatch. If no request has been sent and the deadline has expired, durably restore the known pre-intent state under the same locks, preserving earlier history. Add a wait regression with a slow intent save. The existing retry freshness path at `2539-2550` already handles an analogous after-save expiry.

### Issue 2: Apply strict parsing to the public Maven payload decoder

- **Severity:** Medium.
- **File:** `scripts/release/release_guard.py`.
- **Lines:** `60-67`; compare `scripts/release/release_matrix.py:595-613`.
- **Description:** The refreshed file/stdin reader uses the duplicate-rejecting object hook, but `maven_plan` independently calls plain `json.loads` on the decoded payload. The public producer therefore still uses last-value-wins parsing.
- **Verified evidence:** A no-network probe generated a bound OSS-only Maven plan, then injected either a duplicate root `scope` or a duplicate target `oss_commit`, with the approved value last. `read_plan("-", require_bound=True)` rejected both inputs. Base64-encoding those same bytes and passing them to `maven_plan` returned the approved plan and target in both cases.
- **Reachability:** `release_guard.py:277-283` passes the pipeline environment payload directly to `maven_plan`. `pipeline.yaml:128-144,554-562` invokes this guard for public release admission. There is no raw-payload duplicate check in these callers.
- **Provenance:** `release_guard.py` was independently compared with its HEAD version and is unchanged. This is a tightly coupled gap in the current strict-parsing change, not a claim that the dirty delta added that decoder.
- **Risk:** A publication admission path accepts an ambiguous document that the refreshed plan reader rejects, contrary to the strict parsing requirement and README claim. Re-derivation still checks the resulting coordinates and approval ID; no unauthorized-coordinate publication was demonstrated.
- **Suggested fix:** Reuse a strict JSON decoder for the Maven payload and reject duplicate members before loading the plan. Cover root and nested duplicates through `maven_plan` and its CLI admission path, rather than testing only file/stdin loading.

## Tests and scope limits

Fresh offline runs used these repository-relative commands:

```text
python -m pytest -q -p no:cacheprovider scripts/release/test_release_ops.py scripts/release/test_release_plan.py -k "wait or duplicate_members or plan_output or invalid_plan"
python -m pytest -q -p no:cacheprovider scripts/release/test_release_ops.py -k "r3_retry or r3_plan_claim or r3_plan_guard or claimed_deleted or r3_inspect_lock or r3_new_locks or state_entry_failure or approval_requires_both_flags or intent_is_durable or pending_and_failed_builds or stale_conflicting_state or publisher_batches_missing"
python -m pytest -q -p no:cacheprovider scripts/release/test_release_ops.py -k "maven_missing_is_not_definitive_namespace_absence or r4_retry_rechecks_freshness_at_submission"
python -m pytest -q -p no:cacheprovider scripts/test_bump_version.py -k "release_tools_and_fixtures or release_exclusion_is_repo_relative or successive_bumps"
git diff --check HEAD
```

The pytest runs passed 37, 45, 5 and 7 cases respectively. The bump selection emitted one unknown `slow` mark warning. The selected tests and probes used fake services or temporary local files; the two finding probes were not added to the test suite.

Pinned Black 22.3.0 checked all six changed Python files without changes. The whitespace check passed. These passing checks do not cover the two demonstrated gaps.

No live publishing, tagging, approval, CI mutation, commit, push or production-source edit was performed. This was not a full build, Spark/runtime matrix, process-crash or power-loss test. Remote gates were not refreshed; earlier CI and PR reviews are not current-head proof. This round does not establish merge readiness or guarantee absence of other bugs.

## Resolution log

### Issue 1

- **Status:** Open.
- **What changed:** Nothing in this review round.
- **Why:** The requested scope was review and reporting only.
- **How verified:** Real CLI/state-store execution with a fake clock and fake remote reproduced dispatch at second 31 for a 30-second deadline.

### Issue 2

- **Status:** Open.
- **What changed:** Nothing in this review round.
- **Why:** The requested scope was review and reporting only.
- **How verified:** Root and nested duplicate payloads were rejected by the strict reader but accepted by the public Maven decoder. Future resolution notes should append to this evidence.

## Bounded follow-up: Boolean build metadata

The additional review request asked whether capitalized Boolean strings in Azure build `templateParameters` would prevent the public driver from matching an approved operation. No additional issue was found at this boundary.

- [x] Read `scripts/release/release_ops.py:1711-1818`. `_parameters` accepts an object or strictly decoded JSON string. `_parameter_equal` compares Boolean expectations against native booleans or case-insensitive `"true"`/`"false"` strings; it does not use the truthiness of a nonempty string. Non-Boolean expectations retain exact type/value matching.
- [x] Exercised the real `_validate_build` path using the public test module's `FakeRemote.register`, without queueing. Eight positive cases accepted `"True"`/`"False"` metadata across OSS/Internal Maven and OSS/Internal pip-publisher operation profiles, with both object and JSON-string `templateParameters`.
- [x] Sixty negative cases rejected opposite truth values, padded or empty strings, integer `0`/`1`, null, and an excluded build flag changed from `"False"` to `"True"`.

The reviewed HEAD remains `9dea133c0820d9e27e68ed04661b18d948575e65`; these metadata-matching functions are unchanged by the dirty delta. This is an existing response-comparison rule, not normalization of sealed plans or permission to widen an approved request.

The probe changed no source or test files, made no network or queue calls, and did not fetch live Azure build metadata. Its result is limited to the requested metadata representation. The two original findings and their resolution status are unchanged.

## Implementation resolution

### Issue 1: Fixed and locally verified

`release_ops._queue` now snapshots the pre-intent state for bounded execution
and rechecks the deadline immediately before calling the queue adapter. If
the intent write exhausts that window, it restores the original state in place
and persists the restoration with the current ledger revision. No request was
sent, so this preserves resumability without inventing an ambiguous Azure run.
The in-place update also preserves the caller's state reference.

Four regressions first reproduced the failure, then passed at the exact
deadline and after it, for one-family and grouped publisher operations.
They verify zero submissions, restored planned actions without intent or build
IDs, released locks, and a later approved resume that queues exactly once.

### Issue 2: Fixed and locally verified

`release_matrix.parse_plan_json` now owns duplicate-rejecting decoding for both
file/stdin plans and `release_guard.maven_plan`. Schema re-derivation and
approval checks are unchanged. Root and nested duplicate regressions first
failed, then passed through both the Maven helper and its actual CLI admission
path. The CLI rejects the payload before checkout and does not print its data.

The combined wait, payload, duplicate and retry-freshness selection passed
49 cases. Black 22.3.0 accepted all five affected public Python files. These
results establish the local fixes, not current-head remote CI or live release
approval. The reviewer has been asked to verify the bounded resolutions.

## Bounded fix verification disposition

**CLEAN_FOR_NEXT_ROUND.** Reviewer: `gpt-6-astra`, reasoning effort `max`.
Verified only the two recorded fixes and their regression boundaries, not a
new broad review. HEAD remains
`9dea133c0820d9e27e68ed04661b18d948575e65`, with the uncommitted fixes.

- [x] **Issue 1 verified.** In `scripts/release/release_ops.py:2518-2563`,
  the pre-intent snapshot precedes mutation. The post-save deadline check
  precedes dispatch, and expiry restores the state in place with the current
  revision before persisting it. This preserves the caller's state reference.
  All four boundary/grouping regressions passed, including restored planned
  actions, no submitted request, lock release and one later approved resume.
- [x] **Issue 2 verified.** `scripts/release/release_matrix.py:604-617` and
  `scripts/release/release_guard.py:67-81` use the same duplicate-rejecting
  decoder. Both root/nested regressions passed through the helper and CLI,
  rejecting before checkout without exposing the injected payload value.

Fresh bounded command from the repository root:

```text
python -m pytest -q -p no:cacheprovider scripts/release/test_release_ops.py::test_wait_restores_unsubmitted_intent_when_its_save_exhausts_deadline scripts/release/test_release_guard.py::test_maven_payload_rejects_duplicate_members_before_checkout
```

Result: **6 passed**. The larger 49-case result and red/green history above
are supplied implementation evidence; they were not rerun in this verification.
No remaining issue was confirmed within these two fixes. Their original Open
statuses are superseded by this verified local disposition, with history retained.
No remote API, full suite or production-source edit was performed. This permits
the next review round, not merge or publication approval.
