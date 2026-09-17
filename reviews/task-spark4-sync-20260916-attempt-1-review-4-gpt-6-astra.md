# Spark 4.1 sync, attempt 1, round 4

## Review summary

- Round: **4 only**, Detailed Correctness, sequential, `gpt-6-astra`.
- Verdict: **CLEAN** for the bounded working-source snapshot. Issues found: **0**.
- Scope: four post-Round-1 CI files and their existing consumers, plus shared merge-resolution identity checks.
- No builds, tests, services, agents, source edits, staging, or commits were performed.
- Only this new review artifact was written; prior review artifacts remain intact.

## Snapshot

| Item | Value |
| --- | --- |
| Checkout | Repository root, branch `sync/spark4.1-master-20260916` |
| Branch | `sync/spark4.1-master-20260916` |
| Target / HEAD | `06897e5b27e28d84ce7ffa33e93d7f756992d0f2` |
| Master / MERGE_HEAD | `1305587a4afe92d27c8e28894b90e38020252e04` |
| Round 1 comparison tree | `0eabccd3a82c4ff1e50721b2fe60e4d051a2bc0e` |
| Source check | `2026-09-16T10:08:55Z` |
| Raw `ls-files --stage -z` SHA-256 | `6859de698e60ee5326c52905882bc806811a876719ec8e8acb978ed4107713ee` |

Initially deferred because the working file still duplicated `BUILD_SBT`.
The owner removed the later assignment before this inspection. The working file
now has one assignment at line 20. At the recorded snapshot, the index still had
assignments at lines 20 and 32, and `test_pipeline_yaml.py` had unstaged changes.
This verdict covers the working bytes below, not that older indexed copy.
No unresolved merge entries were present. The parent owns staging.

These are all four source changes from the Round 1 comparison tree:

| File | Working SHA-256 |
| --- | --- |
| `tools\ci\README.md` | `f6cccec1b1e03e80d2efac2c84eca22aa319b0d0c181933a658da77dfc3fb5f1` |
| `tools\ci\get_python_version.sh` | `50d07f2a4ca4bc5235c4008e1cdcee9193eba5cea5d4351ce7364b4f6dc0533d` |
| `tools\ci\tests\test_pipeline_yaml.py` | `1ae0892720a56eb3dfda9ff4df0f4ff837a573d9655aa2345d2d70c5db84cc2b` |
| `tools\ci\tests\test_python_version.py` | `f14646e71634063e07d9d96b5602e3d3dc81ac0736905cb3879d17c1bae2fd9e` |

## Evidence checklist

- [x] Read this worktree's Round 2 Gemini and Round 3 Opus artifacts as evidence
  pointers, not conclusions. Inspected the newer helper changes left outside Round 3.
- [x] `tools\ci\get_python_version.sh:5-25`: the only logic change makes the third
  numeric version component optional. Both anchors and the exactly-one-match check
  remain. The script still rejects wildcard, prerelease, range, major-only, and
  four-component values. It emits the selected version unchanged.
- [x] `environment.yml` selects `python=3.13`. The new accepted grammar therefore
  preserves the branch's minor-series selection without inventing a patch version.
  No runtime pin, dependency configuration, or pipeline trigger changed in this delta.
- [x] `pipeline.yaml:413-447`: the existing caller runs under `set -euo pipefail`,
  stores the helper output in `pythonVersion`, and passes it to both Docker builds.
  `tools\docker\demo\Dockerfile:46-50` and
  `tools\docker\minimal\Dockerfile:46-50` require a nonempty argument and pass it as
  quoted `python=${PYTHON_VERSION}` to Conda. No downstream patch-component parsing
  conflicts with the newly accepted minor-series value.
- [x] `tools\ci\tests\test_python_version.py`: the optional helper argument exercises
  the production no-argument path; explicit `cwd=REPO_ROOT` locates the actual
  environment file. Explicit temporary-file inputs remain absolute. Assertions
  check exit status and exact output, retain patch-version and duplicate rejection,
  and exercise minor-series acceptance and five malformed-value cases.
- [x] `tools\ci\tests\test_pipeline_yaml.py:19-33,447-485`: the first `BUILD_SBT`
  assignment remains after `REPO_ROOT`; only its identical later copy was deleted.
  The Fabric assertion now requires boolean false rather than accepting either
  disabled or enabled guarded execution. Key Vault template, filter, environment,
  subscription, and authentication assertions remain; no job or test was skipped.
- [x] `tools\ci\README.md:113-119` describes the helper's accepted numeric forms
  and unchanged output. This matches the code and the existing Docker consumer.
- [x] The combined `CognitiveServiceBase.scala`, `OpenAIChatCompletion.scala`,
  `OpenAIResponses.scala`, and `Wrappable.scala` are byte-identical to the Spark 4.0
  working files inspected in Round 4. They are unchanged from this candidate's
  Round 1 snapshot. No second whole-port audit was performed.
- [x] Native `git diff --check` against the Round 1 tree returned zero.

## Diff commands

All Git calls used process-local PATH, `GIT_OPTIONAL_LOCKS=0`, and the exact worktree.

```powershell
$git = (Get-Command git).Source
$wt = '.'
& $git --no-pager -C $wt diff --no-ext-diff --unified=6 0eabccd3a82c4ff1e50721b2fe60e4d051a2bc0e -- 'tools\ci\README.md' 'tools\ci\get_python_version.sh' 'tools\ci\tests\test_pipeline_yaml.py' 'tools\ci\tests\test_python_version.py'
& $git --no-pager -C $wt diff --name-only 0eabccd3a82c4ff1e50721b2fe60e4d051a2bc0e --
& $git --no-pager -C $wt diff --check 0eabccd3a82c4ff1e50721b2fe60e4d051a2bc0e --
```

## Findings and limitations

No concrete new correctness issue was found in the inspected working changes.
The Python-version fix repairs the incoming helper's mismatch with the retained
Spark 4.1 environment selection. The duplicate cleanup is complete in working source.

This is source-only evidence. No helper tests, Docker builds, native suites, or
candidate CI were run here; Spark 4.0 test results are not Spark 4.1 runtime proof.
The proven pre-existing R lookup and target's `Seq[Row]` SAR implementation remain
outside this round. The reported master build `236185691` failed Fabric provisioning
before running tests, so it is not candidate evidence. Ports retain Fabric disabled.
This verdict does not certify the older index or PR readiness. Rounds 5 and 6 were
not run. The parent owns staging, subsequent review, and validation.

### Post-write snapshot

At `2026-09-16T10:13:27Z`, the final check found the cleanup staged, one indexed
`BUILD_SBT` assignment at line 20, and no unstaged source. All four source hashes
above still matched. The indexed source now matches the reviewed working snapshot;
the index fingerprint above records the earlier pre-staging state.

### Final handoff

At `2026-09-16T10:15:24Z`, final tree `678e48f77876df8d982718513222314d4be52879`
had no source differences from the reviewed working snapshot. All four hashes above
still matched. The three Docker-helper files are included in this verdict;
`environment.yml` is byte-identical to the pinned Spark 4.1 target.
Read session `files\spark41\validation-report.json` and `validation-commands.json`.
They record the owner's two-test red case, 142 Linux CI tests, 10 parser tests,
77 pipeline tests, Black on 214 files, and 235 Scala/native tests across 26 suites.
These are owner-run results. Generated syntax checks used Python 3.14.6 only;
actual Python 3.13/PySpark 4.1 smoke and Docker image builds remain unrun.
