# Workflow orchestration review

## Verdict

**Request changes.** The normal publication path does not check that the approved
target's runtime matches its source before public uploads. The port-PR recovery
path also retains an unsafe overwrite case for an existing release branch.
Both findings are P2 correctness issues. The second is explicitly a retained
baseline defect, not a newly introduced regression.

This is an offline review, not release authorization or production qualification.
No live service calls, credentials, tags, commits, source edits, or nested agents
were used. The only persistent review output is this file.

## Review contract and revisions

| Field | Reviewed value |
| --- | --- |
| PR | microsoft/SynapseML#2628 |
| Head | `7bc3019409803a3eaf33e9b68b4f84debb5f4846` |
| Base, local `upstream/master` | `861c3a1e14a9511b5604563ff1e3976cefa82e90` |
| Merge base | `861c3a1e14a9511b5604563ff1e3976cefa82e90` |
| Rebase | Explicit no-op; neither revision nor source was changed |
| Assignment | Broad sweep, fleet attempt 1, stage attempt 1, reviewer 1 |
| Model selection | Harness-selected; no additional model identity inferred |
| Decision authority | Human-assigned scope and single-reviewer artifact contract |
| Change size | Large, spanning workflow entry points, Azure producers, guards, transport, and recovery tests |
| Complexity | High, with cross-job variables, immutable refs, separate approval phases, retries, and multiple services |

Read the worktree's `AGENTS.md` and branch/release guidance. Did not use sibling
or historical review reports as evidence. This review covers the assigned
orchestration scope, not every implementation line in the full PR.

## Findings

### F1. P2: Check the source runtime before authorizing Maven publication

**Location:** `scripts\release\release_guard.py:197-208`, called by the new
publication guard in `pipeline.yaml:227-241`.

`validate_checkout` checks HEAD, the local release tag, and cleanliness. It does
not check the Spark, Scala, or Python versions in the approved commit.
Nevertheless, `release_guard.py:636-647` emits the plan's runtime-dependent
variables, including `releaseScala`, as though the checkout matched them.

This differs from bootstrap, which performs that comparison in
`scripts\release\bootstrap_release.py:174-187` before creating tags. The normal
post-merge path has no equivalent check. A port merge that accidentally retains
master's runtime settings can compile and pass tests for that wrong runtime.
Its merge callback can still create the port tags, and a plan bound to those
exact tags and commits passes the Maven source guard.

The failure occurs too late to be harmless. `pipeline.yaml:304-308` uploads
stable Blob Maven artifacts using the actual build's Scala version.
`pipeline.yaml:723-725` only later invokes ESRP staging with the plan's Scala
version. For a Spark 4.1 plan bound to a Scala 2.12 checkout, the publication job
can write the wrong artifact family before ESRP staging rejects the missing
Scala 2.13 output. Schema-4 DBC preparation also lacks this runtime comparison.
The release cannot be treated as a clean pre-publication refusal.

**Offline reproduction:** Generated a normal public plan with the frozen master
source bound to its Spark 4.1 target. Mocked only the Git identity/tag/cleanliness
responses, representing a clean, consistently tagged but wrong-runtime source.
The runtime check read `build.sbt` and `environment.yml` from the actual frozen
commit through local `git show`. No tag was created.

```text
Maven guard, Spark 4.1 plan with master-runtime source: exit 0
Claims releaseScala=2.13: True
Bootstrap rejects same source: spark4.1 candidate has an unexpected runtime
```

The essential probe is:

```python
plan = matrix.build_plan(
    "1.2.0",
    oss_commits={key: reviewed_head for key in matrix.DEFAULT_TARGET_KEYS},
)
target = next(item for item in plan.targets if item.key == "spark4.1")
# Supply this plan and its ID, target.oss_maven_tag, and reviewed_head through
# the Maven guard's existing environment interface.
# Mock _git HEAD/tag queries to reviewed_head and status to empty.
assert guard.main(["maven"]) == 0
# With the same source's real build.sbt/environment.yml:
bootstrap.check_runtime(repo, target)  # raises ValueError
```

**Fix recommendation:** Share the existing runtime comparison with
`validate_checkout`, or validate equivalent authoritative build metadata before
any release upload. Enforce Spark, Scala binary version, and Python consistently
for normal publication and bootstrap. Do not merely postpone the check until
ESRP inventory validation.

**Regression coverage:** Add wrong-Spark, wrong-Scala, and wrong-Python source
cases to the Maven guard tests. Verify that producer execution stops before DBC
publication, `publishBlob`, or PyPI upload. The existing
`test_maven_payload_checks_plan_tag_source_and_family` validates identities, not
the runtime carried by those identities.

**Provenance:** This omission is in the PR's new Maven release guard and its new
producer wiring. The probe establishes acceptance of an inconsistent source
binding, not an observed failure of a production release.

### F2. P2: Do not rebuild an existing orphaned or closed-PR release branch

**Location:** `.github\workflows\release-tag.yml:367-376`, following the open and
merged PR lookups at lines 319-339.

The workflow preserves a branch when it finds an open PR or a recorded merged
PR. If neither lookup finds one, it rebuilds `release/v<version>-<target>` from
the shared target and force-pushes it. There is no intervening guard for an
already-existing remote release branch.

A reachable recovery sequence is:

1. A run pushes the release branch, then PR creation fails or is interrupted.
2. A maintainer changes that branch while recovering it. Alternatively, its PR
   is closed without merging and the branch retains reviewed fixes.
3. A fresh orchestration run fetches that branch but finds no open or merged PR.
4. The workflow discards its content by rebasing from `TARGET_COMMIT` instead.
   The ordinary `--force-with-lease` succeeds because the fresh checkout already
   fetched the remote branch's current SHA.

The lease protects against a ref update after fetch; it does not protect the
existing content fetched at the start of this recovery. A PR opened after the
initial lookup has the same problem: the later push can update it without ever
passing through the existing-PR preservation branch.

**Offline reproduction:** Executed the exact YAML `Create spark rebase PRs`
script through native Bash's stdin with `git` and `gh` replaced by shell
functions. Both PR lookups returned empty, the shared target was not already
based on the release, and all unexpected Git operations failed closed. The
script never queried whether the remote release branch existed and completed
this sequence:

```text
Orphan branch simulation exit: 0
PROBE_REBUILD_EXISTING_BRANCH
PROBE_REBUILD_COMPLETE
PROBE_FORCE_PUSH_EXISTING_BRANCH
PROBE_CREATE_PR_AFTER_REWRITE
```

No real Git mutation or GitHub command ran in this probe. The destructive
consequence follows from the actual `checkout -B` source and force-push refspec,
not from a simulated remote permission result.

**Fix recommendation:** Treat an existing release ref without a recognized PR
as an explicit recovery state. Preserve it and either create the missing PR
from that exact head or stop for reviewed reconciliation. For genuinely new
branches, use an explicit nonexistence lease so a concurrently created branch
cannot be overwritten. Do not interpret an empty PR list as permission to
replace an existing ref.

**Regression coverage:** Cover a pushed branch with failed PR creation, a closed
unmerged PR with retained fixes, and a release branch created after the lookup.
Require that every existing remote SHA remains unchanged. Existing recovery
tests exercise open and merged PRs but not these states.

**Provenance:** The base already contains the same unconditional
`git push --force-with-lease -u origin "$BRANCH"` recovery behavior. This is a
retained defect relevant to the requested reliable release-orchestration
remediation, not a regression attributed to this PR.

## End-to-end trace

**Normal path:** Preparation is master-only, validates version and existing
release refs, generates version/docs changes, and opens a PR with the scoped
App token. Its merged callback checks the recorded merge commit and docs,
creates the primary tag, and explicitly dispatches derivative orchestration.
The orchestrator captures immutable primary source, preserves recognized PRs,
and recovers tag pairs from recorded port merges. F2 affects the unrecognized
branch recovery path. Package submission is separate and requires an approved
source-bound plan. F1 affects the publication guard before its first upload.

**Pre-merge path:** Bootstrap binds candidates before tags, verifies canonical
origin, workflow identity, selected candidate refs, target-base ancestry,
current-head checks, runtimes, and primary docs. It rechecks source refs, pushes
missing tags atomically without forcing existing tags, then verifies remote
identities. Preview does not push tags; dispatch is not package publication.
The normal master's ancestry guard remains separate. Notes require later
primary integration plus approved public producer evidence and current artifact
checks.

The examined App token requests repository-scoped Contents read and Pull
requests write. Git writes and validation dispatches deliberately retain
`GITHUB_TOKEN`; no additional token permission defect was established offline.

The Azure guard runs separately in `BuildAndCacheSbt`, `Publish`, and `Release`,
so these jobs do not incorrectly rely on ordinary task variables crossing job
boundaries. The DBC artifact name uses an explicit task output and dependency
mapping. The consumer checks the handoff before download/publication. These
checks do not resolve F1.

The operator transport uses argv and bundled Azure CLI Python for Windows
batch installations. Generated two-target and three-target public plan commands
were 1,644 and 2,024 characters respectively in the offline probe; no Windows
command-length failure was established for those normal envelopes.

## Files inspected

Full workflow reads covered:

- `.github\workflows\release-prepare.yml`
- `.github\workflows\release-tag.yml`
- `.github\workflows\release-tag-spark.yml`
- `.github\workflows\release-notes.yml`
- `.github\workflows\release-notebook-validation.yml`
- `.github\workflows\pr-validation.yml`
- `.github\workflows\website-deploy.yml`

Read `pipeline.yaml`'s changed producers, their dependency/variable wiring, and
relevant test-job conditions; `scripts\release\bootstrap_release.py`; and
`scripts\release\release_guard.py`. Followed relevant interfaces in
`release_ops.py`, `release_dbc.py`, `verify_release.py`, `project\ReleaseVersion.scala`,
`project\build.scala`, `build.sbt`, `environment.yml`,
`templates\sbt_cache.yml`, `templates\publish.yml`, and
`tools\esrp\prepare_jar.py`.

Inspected relevant tests in `test_release_workflows.py`,
`test_release_bootstrap.py`, `test_release_guard.py`,
`test_release_tag_recovery.py`, `test_release_dbc_contract.py`,
`test_release_public.py`, `test_verify_release.py`, and
`tools\ci\tests\test_pipeline_yaml.py`. Checked the operator guide, release skill,
agent runbook, and master branch reference for the intended lifecycle.

## Validation and limitations

Selected existing tests passed, **45 passed with no selected test skipped**:

```text
python -B -m pytest scripts\release\test_release_workflows.py -q -p no:cacheprovider
11 passed

python -B -m pytest scripts\release\test_release_bootstrap.py -q -p no:cacheprovider -k "pins_public_host or nested_plan or does_not_echo or timeout_has"
7 passed, 45 deselected
```

One additional invocation selected the following exact nodes with
`python -B -m pytest -q -p no:cacheprovider`; **27 passed**:

```text
tools\ci\tests\test_pipeline_yaml.py::test_publish_jobs_resolve_and_preserve_package_versions
tools\ci\tests\test_pipeline_yaml.py::test_maven_receipt_follows_esrp_publication_and_uses_its_actual_directory
tools\ci\tests\test_pipeline_yaml.py::test_release_publication_waits_only_for_enabled_optional_test_jobs
tools\ci\tests\test_pipeline_yaml.py::test_release_job_dependencies_exist_for_every_publication_combination
scripts\release\test_release_dbc_contract.py::test_native_dbc_validation_precedes_public_maven_upload
scripts\release\test_release_dbc_contract.py::test_native_dbc_publication_reuses_the_validated_publish_artifact
scripts\release\test_release_guard.py::test_explicit_three_target_release_cannot_silently_skip_spark40
scripts\release\test_release_guard.py::test_notes_require_an_explicit_complete_public_plan
scripts\release\test_release_guard.py::test_maven_payload_checks_plan_tag_source_and_family
```

Python bytecode and pytest cache writing were disabled. Tests requiring local
Git commits/tags or temporary artifact creation were not run under this task's
artifact-only restriction. Scala builds, website builds, and full release
rehearsal were not run. The two finding probes used mocked effects and in-memory
capture rather than weakened tests or production operations.

No hosted GitHub/Azure YAML execution, live App installation, branch protection,
service permissions, signing approval, artifact publication, or consumer runtime
validation was performed. Passing static and mocked checks is not evidence that
those external prerequisites work. The verdict is therefore about the reviewed
source and the concrete guard/recovery defects above, not production readiness.

## Authorized remediation of F1 and F2

Both findings are now **resolved in the local working tree**, with their original
text and baseline provenance retained above. This update records the separately
authorized implementation, not a revision of the initial review evidence.
HEAD remains `7bc3019409803a3eaf33e9b68b4f84debb5f4846`; no commit, push, live
service operation, or production tag was made. Git integration tests used only
disposable local repositories.

### F1 resolution

Moved the existing bootstrap runtime comparison into
`release_guard.validate_runtime` and invoked it from `validate_checkout` after
the existing identity and cleanliness checks. Bootstrap imports the shared
implementation under its existing `check_runtime` name. Normal Maven
authorization, DBC operations using that checkout guard, and bootstrap now
validate the same approved commit's Spark, Scala, and Python versions.

This reuses the established version-matching semantics rather than introducing
a second parser. Runtime failures occur before the Maven guard emits publication
variables. No `pipeline.yaml` change is needed: its existing checkout guards
already precede the public upload steps.

Added 18 Maven runtime cases covering all three targets with valid source,
incorrect Spark/Scala/Python, a missing Spark declaration, and duplicate Scala
declarations. The tests assert that both source files are read from the approved
commit and invalid source emits no publication-variable logging commands.
Added six bootstrap rejection cases for primary and Spark 4.1 candidates,
checking that every remote fixture ref remains unchanged.

### F2 resolution

Before rebuilding a port branch, the workflow now queries the exact remote
release ref. An existing ref without a recognized open or merged PR stops with
an explicit reviewed-recovery instruction. It neither recreates the PR nor
discards that branch's content automatically.

New branch creation uses an explicit empty expected-value lease:

```text
git push --force-with-lease="$RELEASE_REF:" -u origin "refs/heads/$BRANCH:$RELEASE_REF"
```

Unlike the old implicit lease, this cannot adopt a concurrently created branch
merely because another fetch refreshed the local tracking ref. Regression tests
exercise orphaned and closed-PR branches containing a retained review fix. A
real local Git `post-rewrite` hook installs a competing remote ref during rebase
and fetches it into the workflow checkout; the new push still refuses it.
Existing open-PR preservation and successful new chained PR creation remain
covered. Workflow comments and recovery diagnostics document the new behavior.

### Test evidence

All commands below ran from the existing worktree. WSL supplies POSIX Python,
Bash, and Git so the actual shell/Git workflow tests execute rather than skip.
There were no live service calls and no weakened or bypassed test assertions.

**Fail-before:** After adding regressions but before implementation, this exact
command produced **21 failures**. Incorrect runtimes returned success, valid
runtime cases showed no runtime-source reads, and each preservation case
completed the destructive old path.

```powershell
wsl --exec python3 -B -c "import pytest; from pathlib import Path; root=Path('scripts')/'release'; names=[str(root/'test_release_guard.py')+'::test_maven_guard_validates_source_runtime_before_authorizing', str(root/'test_release_tag_recovery.py')+'::test_recovery_preserves_existing_release_branch_without_active_pr', str(root/'test_release_tag_recovery.py')+'::test_new_release_branch_push_rejects_concurrent_ref_even_after_fetch']; raise SystemExit(pytest.main(['-q','--tb=short','-p','no:cacheprovider']+names))"
```

**Affected suites after implementation:** **278 passed, no skips**, at that
point in the shared working tree.

```powershell
wsl --exec python3 -B -c "import pytest; from pathlib import Path; root=Path('scripts')/'release'; files=[str(root/name) for name in ['test_release_guard.py','test_release_bootstrap.py','test_release_tag_recovery.py','test_release_workflows.py']]; raise SystemExit(pytest.main(['-q','--tb=short','-p','no:cacheprovider']+files))"
```

**Final scoped rerun after formatting corrections:** **41 passed, no skips**.
This includes all 21 initial regression cases, the six bootstrap cases, both
existing positive branch paths, and the workflow contract suite.

```powershell
wsl --exec python3 -B -c "import pytest; from pathlib import Path; root=Path('scripts')/'release'; names=[str(root/'test_release_guard.py')+'::test_maven_guard_validates_source_runtime_before_authorizing',str(root/'test_release_bootstrap.py')+'::test_bootstrap_rejects_mismatched_runtime_before_tagging',str(root/'test_release_tag_recovery.py')+'::test_recovery_preserves_existing_release_branch_without_active_pr',str(root/'test_release_tag_recovery.py')+'::test_new_release_branch_push_rejects_concurrent_ref_even_after_fetch',str(root/'test_release_tag_recovery.py')+'::test_new_release_prs_keep_the_explicit_branch_chain',str(root/'test_release_tag_recovery.py')+'::test_open_release_prs_keep_their_reviewed_branches_and_do_not_mint_tags',str(root/'test_release_workflows.py')]; raise SystemExit(pytest.main(['-q','--tb=short','-p','no:cacheprovider']+names))"
```

Pinned Black **22.3.0** passed the four unshared Python files:

```powershell
wsl --exec python3 -B -c "import black; from pathlib import Path; root=Path('scripts')/'release'; files=[str(root/name) for name in ['bootstrap_release.py','test_release_bootstrap.py','test_release_tag_recovery.py','test_release_workflows.py']]; black.main(['--check']+files)"
```

The initial six-file Black check identified two formatting changes in these new
tests, which were corrected surgically, plus concurrent receipt-section changes
owned by the artifact reviewer. Those receipt-formatting results were sent to
their owner; no whole-file reformat or receipt-section edit was performed here.
The scoped `git diff --check` passed.

Shared guard/test sections were coordinated with the artifact reviewer.
`pipeline.yaml`, PR validation, CI selection, and other reviewers' implementation
sections were not edited. The 278-test result is not a claim about receipt
changes made concurrently afterward; the final 41-test run establishes this
assignment's runtime and branch-preservation behavior. Full integrated
validation and production prerequisites remain outside this remediation scope.
