# Architecture and contracts review

**Verdict: changes required.** One P1 finding and one P2 finding.

## Review assignment

- PR: microsoft/SynapseML#2628.
- Base: `861c3a1e14a9511b5604563ff1e3976cefa82e90`.
- Head: `7bc3019409803a3eaf33e9b68b4f84debb5f4846`.
- Human-assigned fleet lens 2, review attempt 1, stage attempt 1.
- Harness model ID: not exposed; no ID inferred.
- Reviewed the complete base-to-head changes in the owned files, not only the latest commit. This is the assigned component review, not the aggregate fleet verdict.

Owned files reviewed in full:

- `scripts\release\release_matrix.py`
- `scripts\release\release_config.py`
- `project\ReleaseVersion.scala`
- `project\build.scala`
- `build.sbt`
- `tools\esrp\prepare_jar.py`

Read `AGENTS.md` and the master branch guidance. Traced the relevant contracts through `pipeline.yaml`, `release_guard.py`, `release_ops.py`, `verify_release.py`, `bootstrap_release.py`, the release-tag workflow, `BlobMavenPlugin.scala`, `CodegenPlugin.scala`, the SBT cache template, and release documentation. Inspected adjacent matrix, configuration, plan, defaults, public-plan, version, guard, ESRP, and DBC contract tests. No other review artifacts or historical reports were read.

## Findings

### F1. P1: publication rebuilds the wheel without binding its required qualification

**Locations:** `build.sbt:201-210`; `scripts\release\release_matrix.py:40-65,318-327`; consumers at `pipeline.yaml:701-707,750-763` and `scripts\release\release_guard.py:148-179,407-442`.

The required frozen primary-wheel qualification does not survive the handoff into publication. `publishPypi` unconditionally runs `packageSynapseML.value` and uploads the newly built wheel. It accepts neither the qualified wheel nor its approved digest. The public plan's exact allowlist contains source commits, runtime selections, and versions, but no wheel identity or qualification binding. `maven_plan` accepts that plan without any qualification evidence.

This is more than a missing automated test. Even if an operator follows the documented procedure and qualifies wheel A on every selected runtime, publication builds wheel B instead of promoting A. There is no pre-upload comparison with A and no requirement to qualify B. The producer receipt records B's metadata and digest after publication; it does not compare them with approved qualification evidence. Branch-specific Python CI installs the branch's own generated packages, so it does not close the same-primary-wheel cross-runtime gap.

Independent offline reproduction:

1. Generate a bound default schema-4 plan using the synthetic commits in the script below.
2. Pass it to `maven_plan` and `build_actions`. Both accept it without a qualification record.
3. Add a `wheel_qualification` member and recompute the digest. `load_plan` rejects it because the wire contract cannot carry that binding.
4. In a separate memory-only probe, supply `pypi_wheel_receipt` with two ZIP payloads having the expected wheel filename and identical `Name: synapseml` / `Version: 1.2.0` metadata, but different Python source contents. Both receive valid receipts with different SHA-256 values. Neither receipt is checked against a qualified-wheel digest.

**Impact:** a source-approved release can publish a wheel that never passed the required consumer qualification. This is a release-readiness blocker; no live publication was attempted to demonstrate it.

**Required correction:** retain a qualification binding to the frozen primary wheel and the selected runtime/source set, enforce it before tag creation and publication, and upload the qualified artifact without rebuilding it. If rebuilding is intentional, qualify that new artifact before upload. Preserve legacy approval identities; any new approved contract needs explicit regeneration and approval.

### F2. P2: the human-readable approval plan omits its public PyPI output

**Location:** `scripts\release\release_matrix.py:851-889,917-922`.

For the default public plan, `render_text` lists Maven coordinates and notebook archives, then reports `PIP: not selected`. It never mentions PyPI, `synapseml==1.2.0`, or the aggregate wheel filename. Nevertheless, `_required_rows` includes the primary public PyPI package, and the primary release job calls `sbt publishPypi`.

The private `pip` family and public PyPI publication are different contracts. The current renderer distinguishes neither in its output inventory. An operator reviewing the default CLI preview sees no indication that approving the Maven operation also authorizes an immutable public PyPI upload.

**Observed offline result:**

```text
Preview mentions PyPI: False
Preview reports PIP unselected: True
Required public wheel output:
[('pypi', 'master', 'pypi/synapseml', '1.2.0')]
```

**Required correction:** explicitly list the public PyPI package/version and wheel filename whenever the selected public Maven plan includes `master`. Label the unselected private-feed `pip` family separately. Test this against the same output inventory used by the publisher/verifier, including port-only plans that must not publish to PyPI.

## Minimal read-only reproduction

Run the following Python with `python -B` from the repository root. It reads source modules, generates plans in memory, and makes no service calls or filesystem writes.

```python
import base64
import copy
import json
import sys

sys.path.insert(0, r"scripts\release")
import release_guard as guard
import release_matrix as matrix
import release_ops as ops

plan = matrix.build_plan(
    "1.2.0",
    oss_commits={"master": "a" * 40, "spark4.1": "b" * 40},
)
document = matrix.plan_to_dict(plan)
payload = base64.b64encode(json.dumps(document).encode()).decode()
accepted, target = guard.maven_plan(
    payload, plan.plan_id, "refs/tags/v1.2.0", "a" * 40
)
assert accepted.plan_id == plan.plan_id
assert [action["target"] for action in ops.build_actions(plan)] == [
    "master", "spark4.1"
]

qualified = copy.deepcopy(document)
qualified["wheel_qualification"] = {
    "sha256": "f" * 64,
    "runtimes": ["master", "spark4.1"],
}
qualified["plan_id"] = matrix.plan_digest(qualified)
try:
    matrix.load_plan(qualified)
except ValueError as error:
    assert "allowlisted contract" in str(error)
else:
    raise AssertionError("Unexpected qualification-field acceptance")

preview = matrix.render_text(plan)
assert "pypi" not in preview.lower()
assert "PIP: not selected" in preview
assert ("pypi", "master", "pypi/synapseml", "1.2.0") in ops._required_rows(plan)
print("Both contract findings reproduced without publication.")
```

## Independent checks and limitations

Executed 86 existing pure test cases from `test_release_matrix.py`, `test_release_plan.py`, and `test_release_defaults.py`, including parameterized cases, by direct function invocation. All passed. This was not a full pytest-suite run: cases needing filesystem fixtures were excluded, and profile loading used only the repository's synthetic test profile. Python bytecode writing was disabled; socket connections and child processes were blocked during these probes.

Additional memory-only checks passed for complete 15-entry minimum Maven inventories across `1.2.0` / Scala 2.12, `1.2.0-spark4.0` / Scala 2.13, and `1.2.0-spark4.1` / Scala 2.13. Off-version artifact names were rejected. Synthetic profile checks rejected Boolean pipeline IDs, overlapping pipeline/feed identities, and noncanonical private Python package names.

No additional concrete finding was established in the reviewed target defaults, optional Spark 4.0 selection, retained historical lineage, public/private plan separation, independent rebuild counters, legacy schema identity preservation, exact-version versus snapshot logic, immutable blob policy, or ESRP module/coordinate validation. Schema-2 documents retained their separate approval identity and did not acquire schema-4 DBC obligations.

Scala/SBT execution, physical Ivy-cache staging, signing, consumer Spark qualification, and live service behavior were not run. The version and staging tests were inspected rather than executed because this assignment permits only the review artifact as a filesystem edit. The findings above do not claim that a candidate wheel has actually failed on Spark 4.1.

The reviewed head and owned source files remained unchanged at the final source check. No credentials, live services, publication operations, permission changes, commits, source edits, or nested agents were used. Only this requested review artifact was created.

## F1 clarification, 2026-10-08

**Updated disposition: F1 is withdrawn as a P1 code defect.** The original finding and reproduction remain above for audit, but this clarification supersedes their blocking verdict and proposed schema/promotion requirement. F2 remains the concrete P2 finding; its planned correction has not been reviewed here.

The existing policy already requires candidate-source consumer-wheel qualification before production tags and final wheel contents/hash verification after the production build. See `scripts\release\README.md:7-11,28-66,674-684`. The qualification procedure uses the same primary wheel across the selected runtimes. It does not require that the later production archive have an identical whole-file SHA-256. Treating the absence of a machine-readable qualification field as a defect imposed an additional automation requirement that the documented human gate does not promise.

I have not identified a concrete reachable source or installed-payload difference when the approved source, intended version, applicable build environment, and dependency inputs are held fixed. The earlier memory-only receipt probe supplied two deliberately different Python payloads. It established that the receipt records supplied contents without checking a qualification record, not that the real build can produce those different contents from the same approved inputs. Likewise, the plan's rejection of an invented `wheel_qualification` member demonstrates its intentional allowlist, not a broken persisted contract. The earlier statement that both findings were reproduced must be read with this limitation for F1.

Reinspection confirms that `packageSynapseML` cleans the aggregate Python source directory, packages and merges the module sources, and generates setup metadata using the selected version before building the aggregate wheel. I did not run two real candidate builds, so this is not a claim of reproducible builds or completed runtime qualification. General possibilities such as dependency changes are not evidence of a defect under the fixed-input assumptions.

A fresh memory-only ZIP check confirmed the relevant distinction: changing only ZIP entry timestamps produced different archive SHA-256 values while preserving every entry name and uncompressed byte. A different whole-wheel digest therefore does not establish changed Python code or packaging semantics.

### Narrow handling within the existing release process

No new qualification service, promotion service, schema member, or persisted approval-identity change is required by this review. Keep consumer qualification as an explicit human release gate, and do not describe plan validation, ordinary port CI, or producer receipts as proof that it ran.

For the existing final-wheel check, retain the qualification record and final archive hash, then compare the final wheel's entry names and uncompressed contents with the qualified candidate wheel. Differences confined to ZIP timestamps, compression, or archive ordering are not payload changes. Review package metadata and RECORD validity as well; do not silently ignore all metadata differences. This comparison is a narrow clarification of the guide's existing contents/hash verification, not a new approval document.

If an actual installed-payload or relevant packaging difference is found, require an explanation and appropriate consumer requalification before declaring the release complete. Source or target changes continue to follow the existing regeneration/reapproval rules. If immutable artifacts have already been published, follow the documented recovery policy rather than overwriting the same version.

This clarification changes only the review disposition. It does not attest that a production candidate has passed the human gate. No source files were modified, and no live services, credentials, or other reviewers' artifacts were accessed for this follow-up.

## Driver resolution of F2

The approval preview now names the public PyPI package, exact version and wheel
filename when the primary OSS Maven publisher is selected. It explicitly says
when public PyPI is not selected and labels the separate private-feed Python
family `PRIVATE PIP`. No plan wire format or approval digest changed.

Seven parameterized cases compare preview selection with the actual
`release_ops._required_rows` inventory. They cover the default, primary-only,
three-runtime, both port-only, private Python and internal Maven plans. All seven
failed before the correction. The targeted preview and CI-selector command
below then passed all 11 selected cases:

```text
python -m pytest scripts/release/test_release_matrix.py scripts/release/test_release_public.py tools/ci/tests/test_pipeline_yaml.py -q --tb=short -p no:cacheprovider -k "preview_discloses_exact_public_pypi_inventory or release_ci_includes_version_bump_regressions or text or render"
```

## Driver review decision

The user requested a full-PR fleet review. The coordinator selected Medium-scale
coverage with six concurrent, independent lifecycle assignments. The reviewed
patch had 100 files, 26,964 additions and 406 deletions. Complexity was high in
ledger state transitions, immutable publication, recovery, approvals and
cross-service evidence. The assignments covered workflows, version contracts,
recovery, artifact verification, tests and packaging, and operator documentation.

The primary model was `gpt-6-astra`. Higher-priority tool policy required default
reviewer model selection without explicit human model overrides. Reviewer models
were therefore harness-selected, and their exact identities were not exposed.
This is not a claim of three-model review or a uniform complementary-model fleet.
The coordinator, not the user, selected the six assignments and effective tier.
All six initial reports were retained before source remediation began.

Initial outcomes were changes requested in all six assignments. One proposed
P1 qualification finding was withdrawn by its original reviewer after a request
for concrete reachable payload drift. The documentation retains the human
qualification gate rather than claiming it has been automated. The other
confirmed findings are under remediation, with final outcomes recorded below
when combined validation is complete.

## Final driver outcomes

All six assignments completed. Confirmed findings were corrected in the shared
working tree and their original reports retain the resolution details:

| Assignment | Final source-review disposition |
| --- | --- |
| Workflow orchestration | Runtime binding shared with bootstrap; existing and concurrently created release branches preserved. |
| Version contracts | PyPI approval inventory corrected. The unproven payload-drift P1 was withdrawn by its reviewer; qualification remains a human gate. |
| Ledger recovery | All five findings corrected, including partial publication, stale adoption, first-save recovery, read-failure stopping and producer time bounds. |
| Artifact verification | Independent CDN/Central receipts and actual public-download proof required through completion, export and notes; realistic evidence budgets retained. |
| Tests and packaging | Version-bump suite runs in CI with its dependency and history; contradictory wheel evidence is rejected by the artifact fix. |
| Operator documentation | Primary API docs published and checked before Maven upload; runtime-specific Python pins and removed replay instructions corrected. |

No confirmed source-review finding remains unresolved in this offline scope.
This is not production authorization or a claim that external services were
qualified. The agents performed targeted correction checks, and the coordinator
owns combined validation and current-head CI.

The first combined Linux run passed 1,773 tests and found one public-artifact
privacy failure: a reviewer had included its operator's absolute worktree path.
That path was redacted without changing any finding, and all 51 public-document
checks passed afterward. Native Windows passed the ten historical Git replays
and committed-notebook case excluded under WSL. Both operating systems passed
all 12 fixed offline rehearsal scenarios, with no skipped scenario and explicit
denial of live-service validation and publication authority.

Six reports, including this driver record, are committed with the corrections.
The PR description records the final combined rerun and exact-head hosted checks.
Missing production access, final candidate qualification, exact-plan approval,
signing and maintainer decisions remain separate release gates.

The final combined Linux rerun passed **1,777 tests**, with one opt-in SBT skip
and the 11 native-covered Git cases deselected. The last fixture freshness
regressions are included in that count. Full pinned Black validation passed
with 238 files unchanged. The existing current-head hosted checks must still be
replaced by checks against the correction commit; the earlier build is not
evidence for these fixes.

## Current-head hosted review follow-up

Copilot review `5463822043` covered correction commit `bc16fe349f` and raised
two additional comments.

- [Rehearsal environment](https://github.com/microsoft/SynapseML/pull/2628#discussion_r4224954240):
  accepted. The child now inherits only an explicit allowlist of process, path,
  temporary-directory and locale settings. Release/auth variables, production
  plans, arbitrary future token names and Python/pytest overrides are excluded.
  The new synthetic-credential regression failed before this correction.
- [Homepage version identifier](https://github.com/microsoft/SynapseML/pull/2628#discussion_r4224954322):
  rebutted. `version.` at `website/src/pages/index.js:252` is literal JSX text in
  the sentence about the Scala binary version, not a JavaScript expression.
  There is no remaining `version` identifier reference. The actual
  [website build](https://github.com/microsoft/SynapseML/actions/runs/37855557257/job/113578741269)
  succeeded for `bc16fe349f`; no unused destructuring was restored.

The targeted command
`python -m pytest scripts/release/test_release_dry_run.py scripts/release/test_release_rehearsal.py scripts/release/test_public_release_docs.py -q --tb=short -p no:cacheprovider`
passed **76 tests on Windows and 76 on Linux**. Both actual sanitized runner
invocations passed all 12 scenarios with new report files and no publication
authority. A Windows-only test assertion was corrected to use the canonical
`SYSTEMROOT` key; the environment allowlist itself was unchanged.
