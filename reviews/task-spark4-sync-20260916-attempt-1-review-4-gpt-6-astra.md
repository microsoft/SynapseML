# Spark 4.0 sync, attempt 1, round 4

## Review summary

- Round: **4 only**, Detailed Correctness, sequential, `gpt-6-astra`.
- Verdict: **CLEAN** for the bounded working-source snapshot. Issues found: **0**.
- Scope: actual merge combinations and post-Round-1 fixes, not a new upstream or whole-port audit.
- No builds, tests, services, agents, staging, commits, or source edits were performed.
- Only this new review artifact was written; prior review artifacts remain intact.

## Snapshot

| Item | Value |
| --- | --- |
| Worktree | `C:\Users\singhrana\Documents\SynapseML\.worktrees\sync-spark40-20260916` |
| Branch | `sync/spark4.0-master-20260916` |
| Target / HEAD | `ecec8dd58b7a07ebc24d816e321a85ff5dc19d57` |
| Master / MERGE_HEAD | `1305587a4afe92d27c8e28894b90e38020252e04` |
| Round 1 comparison tree | `306e08e24ddb515ba14cabdab79fc6376836e62c` |
| Round 3 comparison tree | `cd6bd3b6eacb3062707f0f3247ec04f4f019d106` |
| Final source check | `2026-09-16T10:04:31Z` |
| Raw `ls-files --stage -z` SHA-256 | `2417352df69a5b081cf6f9766dd6499d390c359533e040b08cf9e9dad224bdb4` |

No unmerged entries were present. The owner staged the CI cleanup during review;
its reviewed bytes did not change. No tracked source remained unstaged at the final check.
Only the following source files differ from the Round 1 tree:

| File | Working SHA-256 |
| --- | --- |
| `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala` | `6c570eabc4a263e67fc106d6f394c213abef9f69c5a10b0a479f1fe1a23aa8f3` |
| `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\PyCodegenSuite.scala` | `4d6ffc896cd462f3cea155b9a73ca09ac4a756d62df1ef724f8d31bedfc6beba` |
| `tools\ci\tests\test_pipeline_yaml.py` | `ce6ba7c0f7dabc98f0cdd9ba11a60327ab1c5b0bb330c50dedbb581c3be98951` |

## Evidence checklist

- [x] Read the local Round 2 Gemini and Round 3 Opus artifacts and their parent notes
  as evidence pointers. Rechecked the actual source rather than adopting their verdicts.
- [x] `Wrappable.scala:109-115,149-168,315-358`: the new stub caller uses the existing
  `safeGetDefault(p)`. Valid defaults still flow through unchanged; the specific
  foreign-owner `IllegalArgumentException` becomes `None`, matching runtime generation.
  `None` yields an optional stub argument; service/complex-parameter branches remain intact.
  Both stub constructors and `setParams` consume this same argument-generation path.
- [x] `PyCodegenSuite.scala:20-63,253-270`: the fixture is nested and test-scope.
  The parent constructor does not read an overridden default before initialization.
  The regression first establishes Spark's rejection, then calls `makePyFile` outside
  the exception assertion and checks both `text=None` and `text: Optional[str] = ...`.
  A stub-generation exception therefore fails the test rather than satisfying it.
- [x] `tools\ci\tests\test_pipeline_yaml.py:19-33`: the sole Round 3 follow-up is
  deletion of the later identical `BUILD_SBT` assignment. AST inspection finds exactly
  one assignment at line 20, after `REPO_ROOT`; no test body or condition was removed.
- [x] `core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\PyCodegen.scala:92-130`:
  stub generation remains inside each package visit and before safe child traversal.
  Manual-initializer handling and runtime imports are not replaced by the stub path.
- [x] `cognitive\src\main\scala\com\microsoft\azure\synapse\ml\services\CognitiveServiceBase.scala:102-115,397-408`:
  retained collection normalization precedes the generic cast; explicit auth resolves
  before the by-name Fabric fallback is evaluated. These combined portions are unchanged
  from Round 1, as are the collection reads in
  `services\openai\OpenAIChatCompletion.scala:137` and `OpenAIResponses.scala:159`
  under the same Scala services directory.
- [x] `.github\workflows\pr-validation.yml:38-44`, `pipeline.yaml:312-318`,
  `templates\publish.yml:11-16`, and `tools\ci\tests\test_pipeline_yaml.py:370-414`:
  master action updates retain JDK 17; Fabric remains boolean false with the Key Vault
  assertions retained; retry preparation precedes package verification and publication
  under `set -e`. No changed ordering or weakened condition was found.
- [x] Verified the unguarded `RWrappable.rParamArg` body is identical at master and both
  pinned targets. It is deliberately outside this sync, not a new Round 4 issue.
- [x] `git diff --check` against the Round 3 tree returned zero; no source was changed.

## Diff commands

All calls used native Git with process-local PATH and `GIT_OPTIONAL_LOCKS=0`.

```powershell
$git = 'C:\Users\singhrana\AppData\Local\GitHubDesktop\app-3.6.4\resources\app\git\cmd\git.exe'
$wt = 'C:\Users\singhrana\Documents\SynapseML\.worktrees\sync-spark40-20260916'
& $git --no-pager -C $wt diff --no-ext-diff --unified=6 306e08e24ddb515ba14cabdab79fc6376836e62c -- 'core\src\main\scala\com\microsoft\azure\synapse\ml\codegen\Wrappable.scala' 'core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\PyCodegenSuite.scala' 'tools\ci\tests\test_pipeline_yaml.py'
& $git --no-pager -C $wt diff --name-only cd6bd3b6eacb3062707f0f3247ec04f4f019d106 --
& $git --no-pager -C $wt diff --check cd6bd3b6eacb3062707f0f3247ec04f4f019d106 --
```

## Findings and limitations

No concrete correctness issue remains in this round's scope. The Python stub fix
and duplicate-constant cleanup are consistent with the reviewed contracts.
The separate guide correctly limits the landed default-guard claim to Python.

This is source-only evidence, not a rerun of the parent's reported red/green
regression, 17 codegen tests, 137 Linux helpers, Black, codegen, or wrapper smoke.
It does not claim JVM-backed Python provenance or full candidate CI readiness.
The reported master build `236185691` ran zero Fabric tests before provisioning
failed; it is not candidate test evidence. No PR exists yet; ports keep Fabric disabled.
Rounds 5 and 6 were not run. The parent owns further fixes, validation, and staging.
