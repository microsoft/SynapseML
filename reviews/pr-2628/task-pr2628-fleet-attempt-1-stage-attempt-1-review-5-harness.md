# Lens 5: testing and coverage

## Review identity and scope

- PR: microsoft/SynapseML#2628, target `master`.
- Base: `861c3a1e14a9511b5604563ff1e3976cefa82e90`.
- Head: `7bc3019409803a3eaf33e9b68b4f84debb5f4846`.
- Assignment: user-selected lens 5, fleet attempt 1, stage attempt 1. Single reviewer; no nested agents. Model selected by the harness; exact reviewer model ID not established.
- Worktree: dedicated PR checkout, isolated from unrelated local changes.
- Owned scope: `scripts\release\release_dry_run.py`, its tests, `test_release_rehearsal.py`, release-test CI selection, `scripts\bump-version.py`, `scripts\test_bump_version.py`, OpenCV `ImageTransformer.py` and `test_image_conversion.py`. Followed the rehearsal fixtures into producer receipts and public-wheel verification to check whether the assertions match production behavior.
- Read the worktree's `AGENTS.md` and branch guidance. GitHub reported the supplied base and head SHAs. The worktree was clean before review and remained at the supplied head after probes.

## Verdict

**Two unresolved P2 findings.** The offline runner itself passed its negative cases and real entrypoint rehearsal. Those results do not establish correct public-wheel provenance or coverage of version preparation in CI.

### F1. P2: public-wheel tests accept a digest that contradicts producer evidence

Locations:

- `scripts\release\test_release_rehearsal.py:100-129`.
- `scripts\release\test_release_ops.py:109-110`, the imported `InventoryChecker.public_pypi` double.
- `scripts\release\test_release_public.py:287-327` and `scripts\release\test_verify_release.py:622-668`, the narrower real-checker tests.
- Production path: `scripts\release\verify_release.py:350-395,693-699`; `scripts\release\release_ops.py:2497-2517,3419-3421`.

The rehearsal maps PyPI presence to the Maven presence flag. Its success assertions therefore never compare the public wheel with the wheel hashed by the real receipt producer. The dedicated real-checker tests cover missing, wrongly named, yanked and unavailable wheels, but omit a correctly named, downloadable wheel with different bytes.

This is a verified behavior gap, not just a request for another test. I retained the real `Checker.public_pypi`, generated receipts through `produced_maven_receipt` and the real `release_guard.maven_receipt`, and supplied a synthetic PyPI response whose SHA-256 and size contradicted the producer receipt. Only the service response and availability check were stubbed for PyPI. `ops.verified_evidence` returned `complete: true`, and `verify.validate_evidence` accepted it. The returned PyPI row contained only identity and `PRESENT`, with no digest or size. The production evidence comparison binds public DBC bytes to their receipt, but does not perform the equivalent check for PyPI.

Observed output:

```json
{
  "published_digest_matches_receipt": false,
  "published_size_matches_receipt": false,
  "complete": true,
  "evidence_accepted": true,
  "pypi_row": {
    "kind": "pypi",
    "target": "master",
    "name": "pypi/synapseml",
    "identifier": "1.2.0",
    "status": "PRESENT"
  }
}
```

Impact: a conflicting public wheel at the expected name/version can be certified as producer-verified despite contradictory public metadata. This does not demonstrate a bad wheel on live PyPI; it demonstrates that the verifier and current tests fail to reject that condition.

Fix: give the PyPI double independent state, include authoritative public SHA-256 and size in strict verification, and compare them with the primary producer's wheel receipt. Add negative tests for mismatched and missing digests/sizes, plus a matching-content control, through `verified_evidence` and `validate_evidence`.

Offline reproduction, run from the review worktree with `python -B -`:

```python
import contextlib
import io
import json
import socket
import sys
import tempfile
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(r"scripts\release").resolve()))
import test_release_ops as fixtures
import release_ops as ops
import release_matrix as matrix
import verify_release as verify

with pytest.MonkeyPatch.context() as patch, tempfile.TemporaryDirectory() as folder:
    def forbidden(*args, **kwargs):
        raise AssertionError("network forbidden")

    patch.setattr(socket, "create_connection", forbidden)
    patch.setattr(socket.socket, "connect", forbidden)
    remote = fixtures.FakeRemote(patch)
    root = Path(folder)
    plan = matrix.build_plan(
        "1.2.0",
        oss_commits={key: fixtures.OSS_SHA for key in matrix.DEFAULT_TARGET_KEYS},
    )
    plan_file, state_file = root / "plan.json", root / "state.json"
    plan_file.write_text(json.dumps(matrix.plan_to_dict(plan)), encoding="utf-8")
    remote.missing = {("oss", "maven")}
    with contextlib.redirect_stdout(io.StringIO()):
        code = ops.main(
            ["resume", "--plan", str(plan_file), "--state", str(state_file),
             "--apply", "--approve-plan", plan.plan_id],
            remote=remote,
        )
    assert code == 1
    for item in json.loads(state_file.read_text())["actions"]:
        remote.succeed(
            item["build_id"], plan, "oss", ["maven"], target=item["target"]
        )
        remote.manifests[item["build_id"]] = [
            fixtures.produced_maven_receipt(
                plan, item["build_id"], root / item["target"] / "maven",
                target_key=item["target"],
            )
        ]
    wheel = verify.public_pypi_wheel_name(plan.oss_version)
    patch.setattr(verify, "_json_get", lambda *_: {
        "info": {"version": plan.oss_version},
        "urls": [{
            "filename": wheel, "packagetype": "bdist_wheel", "yanked": False,
            "url": "https://files.pythonhosted.org/packages/example/" + wheel,
            "digests": {"sha256": "0" * 64}, "size": 1,
        }],
    })
    patch.setattr(verify, "_url_exists", lambda *_: True)
    checker = fixtures.BASE_CHECKER(None, None, ["ado", "internal"])
    patch.setattr(
        fixtures.InventoryChecker, "public_pypi",
        lambda self, version, strict=False: checker.public_pypi(version, strict),
    )
    evidence = ops.verified_evidence(plan, state_file, remote=remote)
    receipt = next(
        artifact
        for run in evidence["producer_evidence"]["runs"]
        for document in run["provenance"]
        for artifact in document["artifacts"]
        if artifact["path"].startswith("pypi/")
    )
    assert receipt["sha256"] != "0" * 64 and receipt["size"] != 1
    verify.validate_evidence(plan, evidence)
    print(evidence["complete"])  # Actual: True despite the contradictory metadata.
```

The simulation writes only disposable local fixtures and never queues a real build.

### F2. P2: release CI omits the version-bump regression suite

Locations:

- `.github\workflows\pr-validation.yml:74-79`.
- `scripts\test_bump_version.py:43-126,604-759,1048-1426`.
- Cross-check: `pipeline.yaml:126-134,773-840`; `project\CodegenPlugin.scala:317-336`.

The new release-tooling step selects `scripts/release` and one CI test file, not the sibling `scripts/test_bump_version.py`. Azure's helper job selects only `tools/ci/tests`, and its Python matrix runs module-generated test packages, not this script suite. No other workflow selects the bump suite.

Reproduction: I parsed the committed workflow command and ran its exact selection with `--collect-only`, plugin autoload disabled, and pytest's cache disabled. It collected **1,229 cases across 21 files**, with **zero cases from `scripts\test_bump_version.py`**. Independently running the assigned three suites yielded 296 passing cases: 12 runner cases, 12 rehearsal cases and 272 bump cases.

The omitted suite contains the new repeated-release checks that preserve optional Spark 4.0 installation pins, generated-doc finalization, historical-snapshot protection and recovery-command integration. The release workflow executes the production bump and trusts its postconditions, but that is not a regression test for preserving unrelated or historical content. A change breaking these new safeguards can therefore pass the added release CI step.

Fix: add the bump suite to the release-tooling selector and install its `hypothesis` dependency, currently absent from that step. Account for `TestHistoricalReplay` needing historical commits: fetch the required history or explicitly separate historical replay from the mandatory current-code regression cases. Add a selector assertion so the suite cannot silently drop out again.

## Validation and coverage limits

All executions used native Windows Python 3.14.6, not master's pinned Python 3.11.8. No cloud resources, live publication, package installation, source edits or commits were performed.

The targeted suite command was:

```powershell
$env:PYTHONDONTWRITEBYTECODE='1'
$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD='1'
$env:GIT_CONFIG_GLOBAL='NUL'
$env:GIT_CONFIG_NOSYSTEM='1'
$env:GIT_TERMINAL_PROMPT='0'
$env:GIT_ALLOW_PROTOCOL='file'
python -B -c "import pytest; from hypothesis import settings; settings.register_profile('lens5', database=None); settings.load_profile('lens5'); raise SystemExit(pytest.main(['scripts/release/test_release_dry_run.py','scripts/release/test_release_rehearsal.py','scripts/test_bump_version.py','-q','-p','no:cacheprovider','-o','addopts=']))"
```

Result: **296 passed, 2 warnings, 440.58 seconds**. Warnings concerned the unregistered `slow` marker and a class-scoped fixture deprecation.

Additional evidence:

- `python -B scripts\release\release_dry_run.py` exited 0 and reported 12 passed, zero failures/skips, `live_services_validated: false` and `publication_authorized: false`.
- The runner tests verified failure/error/skipped/empty/malformed/missing JUnit results, nonzero pytest exit, timeout, launch failure, refusal of production flags, and preservation of an existing report without running tests. Inspection confirmed exclusive report creation at `release_dry_run.py:52` and fail-closed exit/count checks at lines 97-103. No concrete false-success or overwrite defect was found in that runner.
- Rehearsal isolation patches sockets and `Popen`, forbids non-Git direct subprocesses and `AzureRemote`, and restricts Git to the file protocol. Local bootstrap tests exercise real disposable repositories. This is test-level isolation, not an OS sandbox or evidence about live Azure/GitHub semantics.
- OpenCV's exact `toNDArray` function passed 13 additional NumPy-only probes: bytes, bytearray, list, array and memoryview for two-row grayscale/RGB images, plus three invalid sizes. No generated wrapper or Spark substitute was used to claim packaged compatibility.
- `python -B -m pytest opencv\src\test\python\synapsemltest\opencv\test_image_conversion.py -q -p no:cacheprovider -o addopts=` failed collection with `ModuleNotFoundError: No module named 'pyspark'`. Packaged Spark/JVM/persistence tests were therefore **not executed in this review**.
- The image suite does assert channel order, dtype, size rejection, Spark binary rows, transformation and persistence. Wheel-byte and JVM-origin assertions are conditional on `SYNAPSEML_CONSUMER_WHEEL` and `SYNAPSEML_CONSUMER_JAR`. Normal module CI does not set those bindings and cannot substitute for the documented same-primary-wheel qualification on each selected runtime.
- Historical and generated-asset bump assertions passed locally. The broad pre-existing 200-character anchoring behavior was not introduced by this PR and is not reported as a new regression.

I did not rerun Linux, SBT, formatting, live service checks or the full release suite. The supplied earlier 1,227-pass release result and prior Windows/Linux rehearsal results were not treated as independent proof of these contracts.

## Parent validation follow-up

The parent supplied these additional results on 2026-10-08 after this review:

- The broader WSL baseline reported 1,610 passing cases, one optional SBT skip and one notebook-admissibility deselection. Its ten `TestHistoricalReplay.test_historical_bump` cases failed with `No relevant files in <sha>`.
- Replaying those ten cases natively on Windows reported **10 passed, 262 deselected**. The parent isolated the WSL failures to Git reading the Windows-created worktree pointer. They require no product fix and are not additional review findings.
- For final Linux validation on this worktree, use `-k "not TestHistoricalReplay and not current_committed_notebooks_are_admissible"` and retain native Windows coverage for both excluded groups. The native historical-replay result is confirmed above; the parent owns the separate notebook-admissibility result.

These are parent-reported results, not duplicate reviewer executions. They do not resolve F1's contradictory-wheel evidence acceptance or F2's omitted CI selector.

## Driver resolution of F2

The PR-validation release-tooling step now runs
`scripts/test_bump_version.py` alongside the release and pipeline suites. It
installs the suite's existing Hypothesis dependency and checks out full history
so the ten historical replay cases run rather than being silently excluded.

`test_release_ci_includes_version_bump_regressions` asserts the complete selector,
dependency installation, absence of a `-k` exclusion, and full checkout depth.
It failed against the old workflow and passed after the change. Together with
the preview regressions, the targeted command recorded in review 2 passed
11 selected cases. Full combined validation follows after the remaining fixes.
F1 shares the artifact-verification defect tracked in review 4.

## Driver resolution of F1

Review 4 and the recovery integration now require independently measured public
artifact bytes and matching PyPI metadata. Their fixture stores downloaded
bytes separately from receipt fields; changing a receipt cannot redefine those
bytes. Changed CDN, Central and PyPI content is rejected through producer
completion, evidence export and notes. The original failing reproduction and
coverage gap above are preserved, with detailed correction evidence in reviews
3 and 4. No finding remains unresolved in this assignment's offline scope.

The combined public-document check caught an absolute local worktree path in
this report's original scope metadata. The driver redacted that path before
publication; no technical finding or reproduction was removed.
