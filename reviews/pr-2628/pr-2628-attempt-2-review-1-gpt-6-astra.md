# PR 2628, attempt 2, round 1

## Review summary

- Work: [microsoft/SynapseML#2628](https://github.com/microsoft/SynapseML/pull/2628).
- Round and theme: 1, broad sweep covering correctness, defensive checks, logic, and requirements conformance.
- Mode: sequential, single reviewer, read-only source review.
- Model: `gpt-6-astra`, maximum reasoning.
- Reviewed baseline: committed head `243694bccff1d8de0903d813993cdf838f8bd371`, target `8c7143875c843c649a817cf3e8ba9c7bee23689c`, plus the current uncommitted implementation and untracked production files.
- Issues: 4, comprising three P1 findings and one P2 finding.
- Verdict: request changes.

This report contains only public source and public release-interface contracts. I did not change implementation files, run builds or tests, contact release services, or submit publication requests. Supplied validation results are not represented as independently rerun evidence.

## Evidence checklist

- [x] Read `AGENTS.md`, branch guidance, `build.sbt`, and `environment.yml`. Inspected tracked diffs and the untracked-file inventory separately.
- [x] Traced plan derivation and digest validation in `scripts\release\release_matrix.py:178-594`, including repository selection, source bindings, counters, and rehearsal restrictions.
- [x] Traced submission intent, atomic state writes, reconciliation, and adoption in `scripts\release\release_ops.py:835-1230` and `1609-1842`. Pending, failed, and unknown actions are not automatically requeued.
- [x] Traced producer request/source validation, root receipt retrieval, and the exported evidence allowlist in `release_ops.py:369-466`, `1328-1606`, and `1895-2156`.
- [x] Reviewed inventory versus approval separation and bounded GitHub evidence in `scripts\release\verify_release.py:607-762`; reviewed BBC write gates and rollback in `scripts\release\bump_bbcvhd.py`.
- [x] Read the release workflows, `pipeline.yaml`, `project\ReleaseVersion.scala`, the Blob upload changes, and `tools\esrp\prepare_jar.py`.
- [x] Reviewed test source in `test_release_guard.py`, `test_release_ops.py`, `test_plan_evidence.py`, `test_bump_bbcvhd.py`, and the release-related assertions in `tools\ci\tests\test_pipeline_yaml.py`. In particular, staged tests at `test_release_ops.py:506-594` stop at Maven publication, while BBC positive fixtures at `test_bump_bbcvhd.py:19-76` use one combined plan.
- [ ] No independent runtime or service validation was performed in this read-only round. Missing live evidence is not itself reported as a code defect.

Reviewed source hashes:

| File | SHA-256 |
| --- | --- |
| `scripts\release\release_guard.py` | `aaffb708c12e4342145e5da1c041c0b69dbb20ae051864e4b19fdc4f1b666b40` |
| `scripts\release\bump_bbcvhd.py` | `34ba2ec6a66e060fdd4fc883cd6add0c757b22e91265d5e73ff64aa4f8a7caf1` |
| `scripts\release\release_ops.py` | `f3c42d33e56e4f27f40f483e352606492dfb1a92e9aef741667cea05779468a5` |

## Findings

### PUB-1: P1, full-release preflight queries `refs/heads/None`

- File and lines: `scripts\release\release_guard.py:182-191`, specifically line 190.
- Confidence: high, 100%.

The branch-existence loop uses `target.base_branch`, not `target.branch`. The first target, `master`, has `base_branch=None` in `release_matrix.py:80-83`. A normal repository therefore fails the first `git ls-remote --exit-code` request for `refs/heads/None`. The remaining expressions would check `master` and `spark4.0`, never `spark4.1`.

This blocks Release Prepare before edits, primary tagging after merge, and derivative tag orchestration. All three paths invoke `full-release --repo .` in `release-prepare.yml:97-100`, `303-306`, and `release-tag.yml:91-94`.

Repair: check each target's actual release branch. Add a CLI-level test for `main(["full-release", ..., "--repo", ...])` that requires the three expected refs and rejects a missing real target. The existing `test_full_release_cannot_silently_skip_a_supported_target` only exercises plan construction.

### PUB-2: P1, independently published staged plans cannot authorize a new-base BBC update

- File and lines: `scripts\release\bump_bbcvhd.py:206-231`; related evidence gate at `149-157`.
- Confidence: high, 98%.

Consider the supported staged release where an OSS-only plan publishes a new base and a separate `scope=full,repositories=["internal"],patch=0` plan publishes its initial Internal packages. Once their UPacks are published, an existing BBC component still references the previous OSS/Internal base.

The Internal plan cannot update that component because lines 206-210 require its existing OSS pin to equal the new base. Applying the OSS plan first also fails: lines 219-229 require the existing Internal pin already to use the new base. The updater accepts only one plan and its matching evidence. Regenerating a combined plan after publication does not solve this: `verify_release.py:629-646` and `release_ops.py:1469-1483` reject producer evidence carrying either original plan ID.

The individual preservation checks are necessary, but their combination leaves independently completed staged releases without an automated, evidence-backed rollout path. A combined UPack plan chosen before publication is a different release procedure, not recovery for these completed plans.

Repair: add a reviewed rollout handoff that composes the complete, fresh producer evidence for the independently approved OSS and Internal plans and validates their shared base, target, and counters before updating both pins. Do not weaken the hotfix OSS-preservation rule or relabel existing receipts. Extend the staged test through BBC update from the previous base.

### PUB-3: P1, public PyPI collisions can be accepted without proving the existing wheel

- File and lines: `build.sbt:201-210`, specifically `--skip-existing` at line 207.
- Related paths: `pipeline.yaml:600-615`, `scripts\release\release_guard.py:105-155`, `scripts\release\verify_release.py:274-280`.
- Confidence: high, 98%.

The guarded public release job still invokes the inherited `publishPypi` task with `twine upload --skip-existing`. In a partial release where the PyPI wheel already exists but Maven work is missing, the task can succeed without uploading or comparing that existing wheel. The subsequent Maven receipt inventories JAR/POM files, not the aggregate PyPI wheel. The PyPI visibility check validates only the reported version.

Consequently, a successful new Maven producer run plus an existing same-version PyPI object can satisfy the new public approval gate even when the wheel was produced from different or incomplete inputs. Immutability is preserved, but the required source provenance is not. This is an inherited publication behavior that remains directly coupled to the new producer-approval path, not a claim that this diff introduced the flag.

Repair: reject an existing PyPI coordinate before publication, or permit a no-op only after proving the existing wheel's exact identity and hashes against the approved producer output. Include the wheel in the appropriate producer evidence. Add a partial-release regression with a conflicting existing wheel and otherwise successful Maven publication.

### PUB-4: P2, generated release PR instructions restore the dependency cycle

- File and lines: `.github\workflows\release-prepare.yml:201-206`.
- Confidence: high, 99%.

The generated PR body tells the operator to finish the port **and Internal** release PRs before binding a plan and starting publication. A new-base Internal PR can require the not-yet-published OSS dependencies to pass CI. These instructions therefore restore the merge-before-dependency-publication cycle that the staged OSS-first contract explicitly removes.

The checked-in command reference describes the intended separation, but the workflow-generated checklist is the instruction operators receive on the release PR.

Repair: make the generated checklist follow the staged procedure. Bind and publish the reviewed OSS-only plan first, publish the OSS dependencies needed by Internal CI, then prepare and merge the Internal changes and approve their separate plan. Reuse the appropriate public-only Maven plan and ledger for notes rather than implying that an unrelated new plan can adopt earlier receipts.

## Resolution log

### PUB-1

- Status: fixed.
- Changed `release_guard.py` to query each target's actual `branch`.
- Added a CLI-level regression that requires `master`, `spark4.0`, and `spark4.1` and rejects a missing final branch. Both cases reproduced `refs/heads/None` before the fix and pass afterward.

### PUB-3

- Status: fixed.
- Removed Twine's `--skip-existing` and moved its credential from arguments into the process environment. Upload failure now remains failure.
- Primary Maven receipts require the generated public wheel, validate its package name/version, and record its bytes. The pipeline passes the exact aggregate wheel path, and the driver rejects a primary receipt missing it.
- Targeted public guard/driver/evidence tests passed, including genuine fixture-file hash comparisons and a full three-target compressed GitHub handoff below 65,535 characters. The real SBT task was exercised with a controlled collision and propagated that failure.
- Recovery documentation explicitly rejects collision-as-success and distinguishes recorded service evidence from a cryptographic signature.

### PUB-2

- Status: fixed.
- Added a paired handoff using the original Internal plan and optional `--oss-plan`. Writes require both original evidence documents and both exact approval IDs.
- Both full-scope plans must match the selected base, source, runtime, OSS counter and destination. The command updates both pins once without mutating receipts or republishing artifacts; hotfix scope cannot bypass OSS preservation.
- The worker's 89-case BBC suite passed, including independently completed driver ledgers and producer reports with distinct build IDs, all supported targets, preview, malformed pairs, stale/missing evidence, and two-file rollback. Parent inspected the implementation and aligned the command guide, skill and canonical guide. Final aggregate validation follows after the remaining companion fixes.

### PUB-4

- Status: fixed.
- Generated release PR instructions now publish the approved OSS-only Maven plan before requiring the new-base Internal PR to pass CI or merge.
- Notes reuse that original public Maven plan, its ledger, and its producer evidence rather than creating a new identity for old receipts.
