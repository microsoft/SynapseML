# Review Summary

- **Round**: 3
- **Theme**: Edge cases and robustness (error handling, boundary conditions, concurrency, failure modes)
- **Mode**: sequential
- **Model**: claude-opus-5, reasoning effort `max`
- **Repository**: public — `microsoft/SynapseML` PR #2628
- **Baseline**: target `master` `8c7143875c843c649a817cf3e8ba9c7bee23689c`; committed head
  `243694bccff1d8de0903d813993cdf838f8bd371`
- **Uncommitted scope reviewed**: 22 modified tracked files plus the untracked
  `project/ReleaseVersion.scala`, `scripts/release/release_guard.py`,
  `scripts/release/release_ops.py`, `scripts/release/test_esrp_staging.py`,
  `scripts/release/test_plan_evidence.py`, `scripts/release/test_release_guard.py`,
  `scripts/release/test_release_ops.py`, `scripts/release/test_release_plan.py`,
  `scripts/release/test_release_version.py`
- **Artifact**: `reviews/pr-2628/pr-2628-attempt-2-review-3-claude-opus-5.md`
- **Issues Found**: 4
- **Verdict**: ISSUES_FOUND

Round 1 fixes and the round 2 architecture verdict were not treated as evidence.
Every claim below comes from tracing the current working-tree source and from
offline probes executed during this round.

## Evidence Checklist

- [x] `git -C <public-root> status --porcelain=v1` and `git --no-pager diff --stat HEAD`
      to establish the exact modified/untracked scope against `243694bc`.
- [x] Full read of `scripts/release/release_matrix.py` (schema, digest, `_selection`,
      `_feed`, `_commit_bindings`, `build_plan`, `load_plan`, `read_plan`, `render_text`, CLI).
- [x] Full read of `scripts/release/release_ops.py` (transport allow-list, redirect
      handler, `read_provenance_zip`, `AzureRemote`, `_policy`, `_feed_identity`,
      `_destinations`, `_required_rows`, `_inventory`, `_dependency_plan`, `_probes`,
      `_operation`, `build_actions`, `_new_state`, `_validate_state`, `StateStore`,
      `_observe`, `_validate_build`, `_jobs`, `_validate_manifests`, `_refresh_group`,
      `_refresh`, `_adopt`, `_queue`, `_execute`, `_report`, `validate_producer_evidence`,
      `verified_evidence`, `main`).
- [x] Full read of `scripts/release/verify_release.py` (`Checker`, `_check_plan`,
      `add_tag_family`, `_InventoryChecker`, `build_report`, `validate_inventory`,
      `validate_evidence`, `encode_evidence`, `decode_evidence`, CLI mutual exclusions).
- [x] Full read of `scripts/release/bump_bbcvhd.py`, `scripts/release/release_guard.py`,
      `project/ReleaseVersion.scala`, and the `build.sbt` / `project/build.scala` diffs.
- [x] Read of `pipeline.yaml` release surface (parameter defaults, `BuildAndCacheSbt`
      guard, `Publish` and `Release` `dependsOn`/`condition`, ESRP + receipt steps) and of
      `.github/workflows/release-notes.yml`, `release-tag.yml`, `release-tag-spark.yml`,
      `release-prepare.yml`, `pr-validation.yml`, `tools/esrp/prepare_jar.py`.
- [x] Generated six real plans with the current generator (full, internal-only,
      OSS-UPack recovery with counter 1, OSS-Maven-first, staged Internal patch 0,
      rehearsal) and round-tripped every one through
      `load_plan(..., require_bound=True)` and `plan_to_dict`.
- [x] Ledger probes against `release_ops.main(..., remote=<offline double>)`:
      interrupted submission (queue raises) leaves `status=unknown`, `build_id=None`,
      operation + `intent_at` persisted at revision 4; a second `resume --apply`
      queued **0** commands; `--adopt ACTION_ID=BUILD_ID` bound the run and produced
      `status=pending`; a held `<state>.lock` returned exit 2 for `resume`, `preflight`
      and `status` without touching the lock bytes; a one-field state tamper returned
      "Release state checksum is corrupt"; `--apply` without `--approve-plan`,
      `--approve-plan` without `--apply`, and a wrong approval each returned exit 2
      with zero queued commands.
- [x] Failed-build probe: a `result=failed` Azure outcome records `status=failed`
      with "automatic retry is forbidden"; repeated `resume --apply` queued 0 commands
      and the report stayed `complete=false`; a **new `--state` path with the same
      approved plan_id re-queued the operation** (see Issue 1).
- [x] Producer-evidence end-to-end probe on the OSS-Maven-first plan: adopted three
      succeeded runs with realistic receipts (183 artifacts incl. the PyPI wheel),
      `verified_evidence` returned `evidence_kind=producer-verified`,
      `validate_evidence` passed, `encode_evidence` produced **15,720 chars against the
      48,000 budget**, and `decode_evidence` + `validate_evidence` round-tripped.
      The bounded GitHub transport requirement holds with wide margin.
- [x] BBC-VHD probes on throwaway component fixtures: internal-only hotfix preserved
      the OSS pin; a counter-bearing OSS pin with an unbound plan was rejected; the
      bound-counter plan was accepted; OSS-only recovery preserved the Internal pin and
      rejected a stale one; the staged `--plan` + `--oss-plan` pairing previewed both
      pins; an `internal-only` plan was refused as a pairing primary; the
      already-applied case refused a second `version.txt` bump; legacy `--version`
      refused to write.
- [x] BBC-VHD **apply** probe with real producer evidence generated by the driver:
      CRLF bytes preserved exactly, `version.txt` `1.4.26 -> 1.4.27`, post-condition
      "verified on disk", replay refused, and stale / wrong-plan / inventory-only /
      empty-runs evidence each rejected before any file write.
- [x] `mayOverwrite` call sites traced through `BlobMavenPlugin`
      (`maven`), `CodegenPlugin` (`pip`, `rrr`) and `build.sbt` (`docs`, `icons`);
      confirmed a snapshot R zip is named `synapseml-<version>.zip` so
      `stripSuffix(".zip").endsWith("-snapshot")` is true for ordinary CI.
- [x] `pipeline.yaml` parameter defaults confirmed (`testStyle`, `testUnit`,
      `testPython`, `publishArtifacts` all default `true`), and every job named in the
      release-mode `dependsOn` lists (`BuildDocker`, `RTests`, `DatabricksCPUE2E`,
      `DatabricksGPUE2E`, `FabricE2E`, `WebsiteSamplesTests`) confirmed to be
      unconditionally *defined*, so the release-mode graph still compiles.
- [x] `git ls-remote --tags <url> refs/tags/T refs/tags/T^{}` verified against a local
      fixture repository for both a lightweight and an annotated tag: the peeled line
      is returned, so the workflows' remote-confirmation `grep` accepts both forms.
      All real `v1.1.*` tags in this checkout are lightweight (`cat-file -t` = `commit`).
- [ ] No full test suite, live service call, pipeline queue, publication or credential
      access was performed. The parent's supplied counts (463 public cases with 28
      explicit SBT/platform skips, pinned formatting) are recorded as supplied
      evidence, not as this round's execution.

## Issues

### Issue 1: A failed or unknown action has no supported retry, and a second state file re-queues the same approved plan with no cross-ledger guard

- **Severity**: Medium
- **Confidence**: High
- **File**: `scripts/release/release_ops.py`
- **Line(s)**: 1850–1876 (`_execute`), 1295–1312 (`_observe`), 1154–1188
  (`StateStore.__enter__`, new-state branch); `scripts/release/README.md` 160–182
- **Description**: `_execute` skips any action whose `operation is not None`
  (line 1857), and `_observe` only rewrites `status` for actions whose `operation`
  is `None` (line 1298). Once an action has recorded a submission, its terminal
  `failed` state is permanent for that ledger: no command clears the operation, and
  `--adopt` can only bind an *existing* run ID. My probe confirms it — after a
  `result=failed` Azure outcome the driver queued 0 commands on every subsequent
  `resume --apply`, and `report["complete"]` stayed `false` indefinitely.
  The only way to make progress is to point `--state` at a *new* file. A fresh path
  falls into the `_new_state(self.plan)` branch, so the same approved `plan_id`
  re-queues the identical operation with no knowledge of the earlier run
  (probe: `queued 1`, no record of the prior build ID). That is exactly the "blind
  duplicate submission" the ledger exists to prevent, and it also silently defeats
  README line 181 ("Do not edit the ledger to erase a failed or unknown action").
- **Risk**: A transient Azure agent failure on any release action becomes an
  operational dead end. The documented recovery is unusable, so the natural operator
  workaround is a second ledger — which submits a duplicate production build with no
  cross-check, discards the audit record of the failed attempt, and (for Maven and
  publisher runs) relies solely on downstream immutable-coordinate refusal to avoid
  damage. It also makes the exported producer evidence for that plan permanently
  unobtainable through the supported path.
- **Suggested Fix**: Add one approval-gated, explicit retry, symmetric with
  `--adopt`: `resume --retry ACTION_ID --apply --approve-plan <id>` that (a) requires
  the action's current `status` to be exactly `failed`, (b) re-runs `_inventory` and
  refuses when `_artifact_present` is true, (c) appends the previous
  `{operation, build_id, outcome, error}` to a new `attempts` list on the action
  rather than deleting it, and (d) clears only `operation`/`build_id` so `_execute`
  can queue again. Separately, in `StateStore.__enter__`'s new-state branch, refuse
  when a sibling `*.json` in the same directory already carries this `plan_id`
  (or require an explicit `--new-ledger` acknowledgement), so a second ledger for one
  approved plan cannot be created silently. Document the retry in
  `scripts/release/README.md` §3 next to the `--adopt` example.

### Issue 2: A stale state lock blocks the read-only `preflight` and `status` commands, and no repository document describes the recovery

- **Severity**: Medium
- **Confidence**: High
- **File**: `scripts/release/release_ops.py`
- **Line(s)**: 1154–1170 (`__enter__`, `O_CREAT | O_EXCL` acquisition and the
  "lock is already owned" error), 1252–1254 (`__exit__`);
  `scripts/release/README.md` 160–182;
  `.github/skills/synapseml-release/references/recovery-and-rollout.md`
- **Description**: `StateStore.__enter__` acquires `<state>.lock` exclusively and
  fails when it exists. `__exit__` removes it only when the current process still
  owns the exact lock bytes, so any hard termination (agent reclaim, SIGKILL,
  machine restart, container eviction) leaves the file behind permanently. My probe
  shows the stale file returns exit 2 not only for `resume` but also for
  `preflight --state` and `status --state`, i.e. the operator loses the *read-only*
  commands they are told to use to diagnose the interrupted submission. The lock
  body already records `owner`, `pid`, `plan_id` and `created_at`, but nothing
  surfaces it, and a repository-wide grep for `.lock` across
  `scripts/release/README.md`, `.github/skills/synapseml-release/SKILL.md`,
  `references/preflight.md` and `references/recovery-and-rollout.md` returns no
  match. The only guidance is the error string itself, which correctly says "never
  remove a stale lock until its submission outcome is understood" but never says
  what to do once it is understood.
- **Risk**: An interrupted release stalls with no documented way forward. The
  predictable operator response — deleting an unexplained `.lock` under time
  pressure, or starting a second ledger — is precisely the behaviour Issue 1 shows
  to be unsafe.
- **Suggested Fix**: (a) On `FileExistsError`, read the lock body and include its
  `plan_id`, `pid` and `created_at` in the `ReleaseError` message so the operator can
  tell a live process from a dead one; keep the raw `owner` out of the message if it
  is treated as a capability. (b) Add a `status --plan ... --state ... --inspect-lock`
  (or a `--break-lock <owner-hex>` gate that requires the owner value printed in (a))
  so a proven-dead lock has an explicit, auditable removal path. (c) Add a short
  "Interrupted driver run" subsection to `scripts/release/README.md` §3 and to
  `.github/skills/synapseml-release/references/recovery-and-rollout.md` covering:
  confirm the process is dead, read the lock, inspect Azure for the plan ID, remove
  the lock, run `status`, then `--adopt`.

### Issue 3: The public Maven receipt hashes the Ivy tree, not the ESRP-staged tree that is actually published

- **Severity**: Medium
- **Confidence**: High on the mechanism, Medium on impact
- **File**: `pipeline.yaml`, `scripts/release/release_guard.py`
- **Line(s)**: `pipeline.yaml` 623–625 (`prepare_jar.py ... --output
  $(Build.ArtifactStagingDirectory)/maven-release-$(System.JobAttempt)`), 643
  (`folderlocation:` = that staged directory), 648 (`ESRP Publish Package`),
  654–659 (`release_guard.py maven --artifact-root "$HOME/.ivy2/local/com.microsoft.azure"`);
  `scripts/release/release_guard.py` 150–195 (`maven_receipt`)
- **Description**: In the `Release` job the publishable artifact set is the staged
  copy that `tools/esrp/prepare_jar.py` writes to
  `$(Build.ArtifactStagingDirectory)/maven-release-<attempt>`; ESRP signs and publishes
  exactly that directory (`folderlocation`, line 643). The receipt step that runs
  afterwards points `--artifact-root` at `$HOME/.ivy2/local/com.microsoft.azure`
  instead, so every `artifacts[].path/sha256/size` entry in
  `release-provenance.json` describes the pre-staging, pre-signature Ivy layout
  (`<module>/<version>/jars/<module>.jar`) rather than the flat, signed, renamed files
  ESRP actually publishes (`<module>/<module>-<version>.jar` plus `.asc`). Nothing
  downstream reconciles the two: `release_ops._validate_manifests` only checks that
  the paths are well formed, that the family/tag/version match the plan, and that the
  primary target carries `pypi/<wheel>`. The receipt therefore cannot detect a
  staging defect — a module dropped by `collect_artifacts`, a wrong `--scala`, or a
  rename mismatch — even though the whole point of the receipt is that "producer
  receipts describe actual outputs".
- **Risk**: The strongest public artefact in the approval chain attests to bytes that
  were never published. A `prepare_jar.py` regression, or an ESRP staging directory
  that is incomplete for one target, passes approval, BBC-VHD write gates and the
  GitHub Notes evidence unchanged.
- **Suggested Fix**: Hash the staged tree. Note that simply repointing
  `--artifact-root` does **not** work: `maven_receipt` line 157 requires
  `target.oss_maven_version in relative.parts`, which is false for the flat
  `<module>/<module>-<version>.<ext>` staging layout, so every file would be skipped
  and line 181 would fail the step. The minimal correct change is to teach
  `maven_receipt` the staged layout (accept `relative.parts == (module, filename)`
  where `filename.startswith(f"{module}-{version}")`, and treat
  `f"{module}-{version}.jar"` as the module-present marker), then pass
  `--artifact-root "$(Build.ArtifactStagingDirectory)/maven-release-$(System.JobAttempt)"`.
  Keep the receipt step after ESRP so the recorded hashes include the signatures, and
  extend `scripts/release/test_release_guard.py` with a staged-layout fixture.

### Issue 4: A returned Azure build ID can be lost if the ledger write fails immediately after queueing

- **Severity**: Low
- **Confidence**: High
- **File**: `scripts/release/release_ops.py`
- **Line(s)**: 1826–1829
- **Description**: `_queue` assigns `action["build_id"] = returned["id"]` and then
  calls `store.save()` as the very next statement. `save()` can raise for reasons
  that are entirely external to the submission — the lock bytes changed, the file
  changed concurrently, the serialized state exceeded `MAX_JSON_BYTES`, or any
  `OSError`. When it does, the exception propagates out of `_execute` to `main`,
  which prints only the generic message; the freshly returned build ID exists
  nowhere but in the dead process's memory. The prior save at line 1809 correctly
  preserves the *intent*, so the situation is recoverable, but the operator must now
  find the run in Azure by hand before `--adopt` can be used, and the "do not retry"
  guidance offers no ID to work from.
- **Risk**: An avoidable manual reconciliation step at the exact moment the tooling
  is meant to be most reliable, on a path that has already performed a real remote
  write.
- **Suggested Fix**: Emit the ID to stderr the instant it is known, before the save:
  `print(f"Azure build {returned['id']} accepted for operation {operation['id']}; "
  f"reconcile with --adopt if the ledger write fails.", file=sys.stderr)`. Two lines,
  no behaviour change on the success path, and the ID survives any subsequent
  `save()` failure.

## Notes on scopes that were probed and found sound

These are recorded so the parent does not re-open them: plan digest and derived-field
re-derivation including boolean-vs-integer discrimination; `--families ""` /
`--targets ""` / zero and negative counters; rehearsal feed alias, qualified-name and
GUID rejection; `mode=rehearsal` refusing Maven; internal-only plans queueing zero OSS
actions; staged `scope=full, repositories=[internal]` plans queueing zero OSS actions;
subset publisher operations enabling only the missing family flags; approval-gate
matrix; state checksum, schema, revision and timestamp validation; concurrent lock
ownership; adoption conflict detection; evidence freshness, plan binding, envelope kind
and run-coverage rejection; BBC-VHD hotfix OSS-pin preservation, OSS-only recovery
Internal-pin preservation, staged pairing constraints, `version.txt` idempotency,
CRLF preservation and post-condition rollback; bounded GitHub evidence transport; and
snapshot-only ordinary CI via `ReleaseVersion.resolve` and `mayOverwrite`.

## Resolution Log

_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Resolved with an explicit Maven retry restriction.
- **What changed**: Added approval-gated `--retry ACTION_ID` for a recorded
  failed publisher group. Every group member needs fresh complete package
  absence, a matching authoritative failed run, terminal jobs and available
  dependencies. The driver preserves validated attempt history and queues
  only that group. A persistent plan claim and serialized directory-local
  guard prevent accidentally selecting a second state filename. Valid older
  ledgers migrate without losing their recorded operations.
- **Why**: The suggested `_artifact_present` check alone is unsafe. The real
  Maven checker short-circuits at a missing POM even if the corresponding JAR
  exists. It cannot prove absence of a whole namespace, including sidecars.
  Same-coordinate Maven retry is therefore intentionally unsupported, not
  granted through an inventory-only fallback. The command guide, skill and
  recovery reference require a new version or Internal patch and a new
  approved plan for failed Maven publication. They forbid deleting history or
  bypassing it through another directory or raw queue command.
- **How verified**: The driver suite passes 233 cases, including 53 new
  recovery cases. Coverage includes exact approval, original-group selection,
  partial/deleted/uncertain artifact refusal, terminal-job/source checks,
  repeated retries, history tampering, ambiguous intent, competing filenames,
  legacy migration and downgrade refusal. The real Maven-checker regression
  proves the short-circuit case and requires zero additional queues.

### Issue 2
- **Status**: Fixed.
- **What changed**: Added `status --inspect-lock`, bounded to the two derived
  local lock paths. It reports safe metadata and exact paths without acquiring
  locks, changing state, contacting Azure or exposing ownership capabilities.
  New locks record the host. The command guide and recovery reference now
  document exclusive operator recovery, confirmed owner death, Azure
  reconciliation, exact-file removal and subsequent status/adoption.
- **Why**: A diagnostic command must work while a stale lock blocks normal
  status. Metadata alone must never authorize stealing a lock.
- **How verified**: Offline cases cover absent, valid, old, malformed and
  oversized metadata, hidden ownership values, no service calls, no mutation
  and both lock paths. Actual CLI help exposes the documented option.

### Issue 3
- **Status**: Fixed.
- **What changed**: `pipeline.yaml` now passes the exact ESRP publish directory
  to `release_guard.py` after the ESRP step. `maven_receipt` requires the flat
  staged layout, records every selected output and present signature/checksum,
  requires each module's JAR/POM plus Core tests, and rejects unexpected layout,
  empty files, links and files changed during hashing. The PyPI wheel requirement
  is unchanged.
- **Why**: The receipt must describe the published staging tree, not an earlier
  source copy. An Ivy-root fallback would hide the wiring error, so it is rejected.
- **How verified**: Five regressions failed before the fix. The formatted source
  passes 29 targeted guard/staging/pipeline cases. CLI cases use the real stager
  for all three targets, modify only staged bytes, add a signature fixture, and
  compare every receipt size/hash with the staged files rather than Ivy.
  Missing POM/Core-tests, wrong coordinates, unexpected/nested/empty output and
  in-hash mutation are rejected. Ordinary and release Azure no-write previews
  compiled; the release preview contains the exact staged receipt path.
  No release build or publication was executed.

### Issue 4
- **Status**: Fixed.
- **What changed**: A flushed stderr acknowledgement records a returned
  positive build ID, operation ID and adoption hint before the next ledger
  save or producer read.
- **Why**: A later local disk failure must not hide the ID needed to reconcile
  a possibly accepted remote submission. This acknowledgement is not a
  successful producer receipt.
- **How verified**: The post-queue save-failure regression retains the unknown
  intent, captures build 101, and adopts it after storage recovery without
  queueing a second build. The current flat-ESRP producer fixture also passes
  all 32 targeted receipt cases.

### Parent error-handling refinement

- Replaced blanket service catches with explicit operational exception types.
  Unexpected adapter programming errors propagate while persisted intent and
  returned build IDs remain intact. Entry cleanup uses `finally`, including
  interrupts. Lock cleanup failures now report a safe path and inspection
  command instead of disappearing silently.
- Five new regressions failed before the correction. All 11 selected
  adapter-error, interrupt, lock-cleanup, ambiguous-submission and post-save
  recovery cases now pass. Black 22.3.0 also passes.
