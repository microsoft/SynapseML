# Release configuration and build simplification audit

Reviewed microsoft/SynapseML#2628 at `800589ac8dfe4bd23596cb174c9ec997e574b6ae`, against `861c3a1e14a9511b5604563ff1e3976cefa82e90`.
Target context is master, Spark 3.5.0, Scala 2.12.17, Python 3.11.8. Full owned files and their changes were read.
Human override: Medium, six disjoint reviews; this is review 2, attempt 1. Reviewer identity exposed by the runtime: `gpt-6-astra`.
Audit only. No source changes, commits, pushes, nested agents, service requests, or production writes.

## Ranked reductions

Line estimates are net deletions after replacement, not deletion-only counts. Keep all stated acceptance checks.

| Rank | Files and current lines | Proposed reduction | Net lines and evidence |
| --- | --- | --- | --- |
| 1 | `scripts\release\release_matrix.py:558-599` | Replace the twelve repeated `build_*` expressions with one small ordered target/suffix table and a comprehension over repository and family. Keep every parameter name, including explicit false values. Do not add a configurable mapping system. | About 20-25 fewer lines after formatting. An independent in-memory table produced exactly the same ordered parameter items for all 140 non-public repository/family/target selections. |
| 2 | `scripts\release\test_release_version.py:144-159,197-212` | Construct both local SBT fixtures before execution, then run `releaseVersionChecks` followed by the expected failing `publishPypi` in one SBT invocation. Require both success markers and the collision marker. | About 10-15 fewer lines and one fewer SBT startup. Retain compilation of the complete `BuildUtils`, real Git tag/dirty checks, and execution of the extracted publication task. Requires opt-in SBT confirmation. |
| 3 | `scripts\release\test_release_matrix.py:210-225,241-254` | Parameterize the rebuild-counter test over `["oss"]` and `["oss", "internal"]`; both tests assert the same OSS counter and unchanged internal dependency coordinate. | About 12-15 fewer lines. Preserve both repository selections and both assertions; do not remove counter independence coverage at 227-238 or pipeline-variable coverage at 322-339. |
| 4 | `tools\esrp\prepare_jar.py:27,53-61` | Use `filename in selected` for duplicate detection and remove the separate global `destinations` set. Each module has a unique destination prefix and its own `selected` map. | Two fewer lines and one fewer collection. Existing duplicate-output refusal must still fail before any staging or cache mutation. |
| 5 | `scripts\release\release_matrix.py:309-315,680-694,784-796` | In `require_public_plan`, keep `plan_to_dict`'s full object validation, require bindings, then return the validated plan. Within `load_plan`, compare `_plan_document(expected)` with input instead of revalidating the freshly derived object through `plan_to_dict`. | About two additional lines, but public validation drops from three derivations to one; public loading drops from two to one. An in-memory replacement passed 126 plan/public/default regressions. Preserve hash, allowlist, derived-coordinate, hidden-field and binding checks. All current repository callers ignore the returned copy. |
| 6 | `scripts\release\test_release_bootstrap.py:57-178,638-686` | Keep representative website failure and success tests through `execute`; run the remaining malformed/latest-run/provider cases directly against `check_website_ci` with literal metadata. Reuse only the small metadata fixture, not a new fake-service framework. | Roughly line-neutral. Avoids constructing and pushing three candidate branches for each metadata variant. Preserve explicit integration proof that rejected website evidence creates no tags. |

The first four proposals remove roughly 44-57 lines without removing supported release behavior.
Do not promise a large public-only reduction by relabeling private functionality as duplication.

## Private functionality needs a scope decision

`release_config.py:37-143` is not dead code. `release_matrix.py:341-644,697-799,822-1053` derives, reads and displays separate private plans, internal-only patches, feed rehearsal and UPack rebuilds.
`release_ops.py:801-835,851-900` consumes profile identities for feed checks and artifact inventory; its execution paths also compare the local profile with the approved plan.
`verify_release.py:483-510` loads private configuration only when the historical/private selection needs it. Public-only checking must remain profile-free.
The README at 551-556 explicitly promises readable schema-1 identities and optional private operations. Schema 3 binds the profile into the digest; schema 1 is deliberately read-only.

A public-only scope could remove private creation/execution/rehearsal, but requires coordinated changes to ops, verifier, CLI flags, tests and documentation.
Estimated owned-code opportunity is several hundred lines, not an approved safe cut. No exact net estimate is credible before deciding whether legacy private reading remains supported.
Keep public schemas 2 and 4 byte-for-byte identity-compatible. Do not migrate approved documents or silently add DBC obligations to schema 2.
Even if private support is retired, retain `release_config.py:15-34`'s strict JSON implementation or move it deliberately: public plan, bootstrap, guard, DBC and verifier inputs depend on duplicate-member and non-finite-number rejection.

## Rejected cuts and file coverage

| Owned file, full range reviewed | Disposition |
| --- | --- |
| `build.sbt:1-415` | Keep explicit release-version resolution at 10-13 and fail-closed PyPI upload at 201-211. Unchanged build tasks and dependencies are outside this reduction. |
| `project\build.scala:1-209` | Keep empty-destination probes, overwrite=false and no automatic release retries at 94-175. A prefix probe rejects partial Maven releases; upload collision handling alone is not equivalent. |
| `project\ReleaseVersion.scala:1-47` | Keep snapshot-by-default, tag/HEAD/clean-tree checks and container allowlist. Workflow validation and build validation protect different entry points. The plan-ID check is format validation, not cryptographic approval verification. |
| `project\sbt-launch.sha256:1` | Keep. Both release preparation and PR validation consume the pin for the launcher selected by `project\build.properties`. |
| `tools\esrp\prepare_jar.py:1-132` | Proposal 4 only. Keep explicit version, selected Scala line, required modules/test JAR, POM/JAR checks, path containment and atomic staging outside Ivy. |
| `scripts\release\release_matrix.py:1-1061` | Proposals 1 and 5; private scope decision above. Keep schema allowlists, re-derivation against rehashed input, full-object checks before public projection, exclusive plan output and durability refusal. |
| `scripts\release\release_config.py:1-143` | Existing small profile validators are justified if private operations stay. Do not replace exact field/type/feed/package checks with permissive configuration. |
| `scripts\release\bootstrap_release.py:1-423` | Keep workflow-only apply, exact candidate/current-base/runtime checks, CI provider/latest-run identity, docs checks, atomic push, race recheck and temp-ref cleanup. Similar JSON envelopes do not justify a new response-validation abstraction. |
| `scripts\release\test_release_matrix.py:1-393` | Proposal 3. Keep full catalog coordinates and explicit optional-target coverage even though default releases omit Spark 4.0. |
| `scripts\release\test_release_config.py:1-160` | Keep strict JSON and private profile tests while private operations remain. The exported synthetic profile fixture is imported by multiple suites; deleting the module breaks them. |
| `scripts\release\test_release_defaults.py:1-96` | Keep the fixed schema-2 plan digest, default selection, optional-target policy independence and notes coverage. This is compatibility evidence, not redundant default testing. |
| `scripts\release\test_release_plan.py:1-302` | Keep independently computed digests in `resign`, rehashed-tamper rejection and retained-file durability tests. Reusing production hashing would weaken the independent oracle. |
| `scripts\release\test_release_bootstrap.py:1-686` | Proposal 6. Keep real local Git tests for atomic rejection, annotation identity, branch races, idempotence and preview/no-write behavior. |
| `scripts\release\test_release_version.py:1-212` | Proposal 2. Do not replace executed Scala/command assertions with source-string tests; those would not prove release refusal. |
| `scripts\release\test_esrp_staging.py:1-263` | Keep malformed version/classifier, cache preservation and linked-output coverage. API/CLI Cartesian cases may be separated later, but retain representative CLI no-output/error-code assertions rather than deleting that boundary. |

## Validation and defects

Commands below ran from the dedicated worktree with bytecode/cache writes disabled and pytest plugin autoload disabled.
`SYNAPSEML_TEST_RELEASE_SBT=0` kept the opt-in SBT test from bootstrapping dependencies during this offline audit.

```powershell
python -B -m pytest -p no:cacheprovider scripts\release\test_release_matrix.py scripts\release\test_release_config.py scripts\release\test_release_defaults.py scripts\release\test_release_plan.py scripts\release\test_release_version.py scripts\release\test_esrp_staging.py -q -ra
```

Result: **212 passed, 1 skipped**. The skipped test is the opt-in real SBT regression.
Schema-2 and schema-4 documents round-tripped exactly with both two-target and explicit three-target selections.
The historical three-target schema-2 digest remains `2872f6280e4022bb5c86cf46c0eaae88b1fd4b3c96f231d31ed7a6bdc8f14443`.

```powershell
python -B -m pytest -p no:cacheprovider scripts\release\test_release_bootstrap.py::test_preview_then_atomic_bootstrap_and_idempotence -q -x --tb=short
```

Initial result: failed before production code ran, because the fixture invokes `git -C <bare repo> show-ref` while host Git requires explicit bare-repository selection.
With process-local `safe.bareRepository=all`, the same test passed. No persistent Git setting was changed.
**Test portability defect:** `test_release_bootstrap.py:47-54` should use explicit `--git-dir` for its known bare fixture rather than require weakening host policy.

```powershell
python -B -m pytest -p no:cacheprovider scripts\release\test_release_bootstrap.py -q -ra --tb=short
python -X utf8 -B -m pytest -p no:cacheprovider scripts\release\test_release_bootstrap.py::test_bootstrap_is_explicit_and_preserves_normal_master_guard -q --tb=short
```

Full bootstrap result with process-local bare-fixture configuration: **57 passed, 1 failed**.
**Second test portability defect:** `test_release_bootstrap.py:20` uses locale-default `read_text()` for UTF-8 workflow YAML. It raises `UnicodeDecodeError` on Windows CP1252; use `encoding="utf-8"`.
The isolated UTF-8-mode command above passed, confirming the cause. This is not a claim that the unmodified full suite is green on this host.
Proposal 5 was applied only to Python function definitions in memory, then `pytest.main` ran `test_release_plan.py`, `test_release_public.py` and `test_release_defaults.py` with `-p no:cacheprovider -q --tb=short`: **126 passed**. No repository file was patched for this experiment.
The original seven-file run was stopped after the bare-fixture failure; the completed split runs above supersede it. Git confirms no owned tracked source changes.
The earlier unpinned Black 26.5.1 result is superseded by the supplied WSL Python 3.12 environment's Black 22.3.0 check: **all 11 owned Python files unchanged**.

```powershell
python -m black --check scripts\release\release_matrix.py scripts\release\release_config.py scripts\release\bootstrap_release.py scripts\release\test_release_matrix.py scripts\release\test_release_config.py scripts\release\test_release_defaults.py scripts\release\test_release_plan.py scripts\release\test_release_bootstrap.py scripts\release\test_release_version.py scripts\release\test_esrp_staging.py tools\esrp\prepare_jar.py
```

The formatter command ran through WSL with converted paths; no private environment path is recorded here.
Shared current-head baseline supplied by the parent: 1,778 Linux regressions plus 11 native Git cases; 1,669 hosted release tests including SBT/history; Azure validation passed with 22 unchanged skips and cache-only warnings.
That is supplied evidence, not work repeated by this reviewer. Reviewer pytest results above used native Windows Python 3.14; hosted testing covers the actual Python 3.11 branch baseline.
No broad suite was rerun after receiving that baseline. No new Scala, scalastyle or publication claim is made. After authorized edits, run only the affected tests; proposal 2 specifically needs the opt-in SBT version test with the selected JDK and cached dependencies.

## Authorized implementation resolution
Contract correction: Medium was the primary agent's estimate; the human requested a six-review fleet, not a particular tier or reviewer model.
Implemented recommendations 1, 3, 4, 5 and 6 plus both bootstrap portability fixes in five Python files: 99 insertions, 127 deletions, net 28 lines removed.
Exact parameter-dictionary assertions and fixed schema-2/schema-4 identities now guard the table and validation reductions; public/private behavior and legacy read-only loading remain supported.
Affected WSL run passed 196 tests: `test_release_matrix.py`, `test_release_defaults.py`, `test_release_plan.py`, `test_esrp_staging.py`, and `test_website_validation_rejects_invalid_evidence`, with `-p no:cacheprovider -q --tb=short`.
Eight selected public-contract tests passed: hidden private mutation, rehashed extra fields, legacy read/export/execute boundaries, and missing private package identity.
Six native bootstrap selectors passed: workflow parsing, preview/apply/idempotence, failed website/no tags, atomic rejection, optional-target independence, and annotated tags; host `safe.bareRepository=explicit` remained unchanged.
Differential checks against the reviewed head passed for 343 complete outputs and ordered parameter maps, 14 public schema round trips and 273 read-only legacy round trips; public approval now derives once.
Pinned Black 22.3.0 passed all five changed files; scoped `git diff --check` passed. No broad suite, live publication, commit, push or nested agent was used in this implementation.
Recommendation 2 is deliberately deferred: the original stronger SBT fixture remains unchanged. No unresolved implementation failure remains; parent owns full validation and CI.
