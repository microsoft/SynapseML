# microsoft/SynapseML#2628: lens 4 detailed correctness

| Review identity | Value |
| --- | --- |
| Head | `7bc3019409803a3eaf33e9b68b4f84debb5f4846` |
| Base | `861c3a1e14a9511b5604563ff1e3976cefa82e90` |
| Lens | Detailed correctness of public artifact and DBC proof |
| Verdict | **Request changes: one P1 finding** |

The public Maven and PyPI checks establish availability, but do not establish that the available files match the producer's artifact hashes. An offline fixture passed complete producer-evidence validation and compressed evidence export with HTTP-200, zero-length public files that contradicted the retained receipts. This is an acceptance bug in the verifier, not evidence that any actual published release is corrupt.

## Inspected files

Repository-relative paths below identify the reviewed source, not local machine locations.

| Scope | Files and portions |
| --- | --- |
| Full owned implementation | `scripts/release/release_dbc.py`; `scripts/release/verify_release.py` |
| Owned guard logic | `scripts/release/release_guard.py`: notes plan and installation text, current DBC download verification, source binding, primary integration, Maven/PyPI receipts, and their CLI dispatch |
| Producer-proof trace | `scripts/release/release_ops.py`: operation construction, build identity validation, manifest validation, DBC content comparison, refresh, exported producer validation and evidence collection |
| Parsing support | `scripts/release/release_config.py`, including strict JSON handling |
| Associated tests | `scripts/release/test_release_dbc.py`; `test_release_dbc_contract.py`; `test_verify_release.py`; `test_plan_evidence.py`; `test_release_public.py`; artifact/notes/integration portions of `test_release_guard.py`; in-memory producer fixtures in `test_release_ops.py` |
| Publication wiring | `.github/workflows/release-notes.yml`; relevant Publish and Release jobs in `pipeline.yaml`; `tools/esrp/prepare_jar.py`; package/publication tasks in `build.sbt`; `project/BlobMavenPlugin.scala` |
| Rules and contracts | `AGENTS.md`; master branch reference; `environment.yml`; `pyproject.toml`; notebook publication and evidence sections of `scripts/release/README.md` |

No other agent reports, historical review reports, or session review artifacts were used.

## F1: P1, compare public Maven and PyPI content with producer receipts

**Primary locations**

- `scripts/release/verify_release.py:208-214`: `_url_exists` accepts the response without checking its body or length.
- `scripts/release/verify_release.py:299-324`: `_maven` reduces each required POM/JAR check to URL existence.
- `scripts/release/verify_release.py:350-395`: `public_pypi` checks version, exact wheel filename, package type, yanked state and URL, then uses the same existence-only probe. It never compares PyPI's digest or size with the receipt.

**Supporting trace**

`release_guard.py:407-527` records local wheel and ESRP staging hashes. `release_ops.py:2334-2494` validates the receipt's identifiers, hash syntax and required inventory, but does not compare these hashes with public Maven or wheel downloads. The public-content comparison at `release_ops.py:2497-2518`, called during refresh and exported-evidence validation, applies only to DBC files.

Consequently, fresh inventory can mark every Maven/PyPI row `PRESENT` while those URLs serve empty, corrupt, or different same-coordinate files. The positive build facts and retained local receipts do not resolve that contradiction. `validate_evidence`, `encode_evidence`, and `decode_evidence` all accept the resulting report. The notes workflow's later inventory-only check repeats the same existence-only checks, so it does not repair this gap.

The DBC path provides the useful contrast: its anonymous download is hashed, and a differing DBC hash is rejected against the producer receipt.

**Offline reproduction**

The fixture uses the repository's existing `FakeRemote` to construct consistent, successful producer records. All public artifact HTTP responses are replaced in memory. Tags and DBC identities remain valid controls; no service is contacted, no files are written, and no commands are launched.

Run this PowerShell block from the reviewed checkout:

```powershell
@'
import sys, os, io, copy, hashlib
sys.dont_write_bytecode = True
sys.path.insert(0, r"scripts\release")
import pytest
import release_matrix as matrix
import release_ops as ops
import verify_release as verify
from test_release_ops import FakeRemote, BASE_CHECKER

def offline(event, args):
    if event in {"socket.connect", "socket.getaddrinfo", "subprocess.Popen"}:
        raise RuntimeError("External operations prohibited")
    flags = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND
    if event == "open" and len(args) > 2 and args[2] & flags:
        raise RuntimeError("Filesystem writes prohibited")

sys.addaudithook(offline)
with pytest.MonkeyPatch.context() as mp:
    plan = matrix.build_plan(
        "1.2.0", oss_commits={"master": "a" * 40, "spark4.1": "a" * 40}
    )
    remote = FakeRemote(mp)
    runs = []
    for build_id, action in enumerate(ops.build_actions(plan), 101):
        operation = ops._operation(plan, action, ["maven"])
        remote.register(operation["command"], build_id)
        remote.succeed(
            build_id, plan, "oss", ["maven"], target=action["target"]
        )
        runs.append({
            "action_ids": [action["id"]],
            "operation": operation,
            "build": ops._evidence_build(remote.build(build_id), operation),
            "definition": ops._evidence_definition(
                remote.definition(action["pipeline_id"])
            ),
            "jobs": ops._public_jobs(
                ops._jobs(remote.timeline(build_id)), from_timeline=True
            ),
            "provenance": remote.provenance(build_id),
        })

    wheel = verify.public_pypi_wheel_name(plan.oss_version)
    public_hash = hashlib.sha256(b"").hexdigest()
    mp.setattr(verify, "_json_get", lambda *_: {
        "info": {"version": plan.oss_version},
        "urls": [{
            "filename": wheel,
            "packagetype": "bdist_wheel",
            "yanked": False,
            "url": "https://files.pythonhosted.org/packages/example/" + wheel,
            "digests": {"sha256": public_hash},
            "size": 0,
        }],
    })
    requests = []

    class EmptyResponse(io.BytesIO):
        headers = {"Content-Length": "0"}

        def read(self, *args):
            raise AssertionError("Current verification never reads the body")

    def open_public(request, **kwargs):
        assert "Authorization" not in request.headers
        requests.append(request.get_method())
        return EmptyResponse(b"")

    mp.setattr(verify.urllib.request, "urlopen", open_public)
    mp.setattr(verify, "Checker", BASE_CHECKER)
    mp.setattr(BASE_CHECKER, "github_tag", lambda *_: (verify.OK, "a" * 40))
    mp.setattr(
        BASE_CHECKER, "public_dbc", lambda *_: (verify.OK, "f" * 64, 321)
    )
    rows, complete = verify.run_plan(plan)
    report = verify.build_report(plan, rows, complete)
    report.update(
        complete=True,
        evidence_kind="producer-verified",
        producer_evidence={
            "schema_version": 1,
            "plan_id": plan.plan_id,
            "checked_at": report["checked_at"],
            "destinations": {},
            "runs": copy.deepcopy(runs),
        },
    )
    verify.validate_evidence(plan, report)
    assert verify.decode_evidence(verify.encode_evidence(report)) == report
    assert public_hash != "e" * 64
    assert complete
    assert len(requests) == 61 and set(requests) == {"HEAD"}
    print("ACCEPTED: empty public files, contradictory receipt hashes/sizes")
    print("61 anonymous HEAD requests; producer validation and transport passed")
'@ | python -B -
```

The primary wheel receipt in this fixture has size `123` and hash `"e" * 64`; the public metadata says size `0` and the SHA-256 of empty bytes. The Maven receipts also describe nonempty files. The mismatch does not affect completion. A companion mutation of the DBC inventory hash fails with `Public DBC hash differs from producer evidence`, confirming that content comparison is selective rather than globally absent.

**Fix direction**

Before marking a bound release complete, stream the required public artifacts anonymously and compare their length and SHA-256 with the applicable producer receipts. PyPI's release-file digest and size should also agree with the primary wheel receipt. Retain the exact filename, version, non-yanked and URL checks.

Cover both Maven endpoints. The current receipt hashes ESRP staging, whereas Blob publication runs in a separate job before the Release job's `publishLocalSigned`. Either publish retained identical artifacts to both destinations or retain destination-specific producer hashes; do not assume independent builds produce byte-identical JARs.

Add regressions for correct coordinates with different bytes, empty responses, wrong lengths, and HTTP-200 non-artifact bodies. Require rejection before evidence export and before notes publication. Keep DBC's existing hash comparison and immutable upload behavior.

## Other checks and evidence

| Area | Result within the offline scope |
| --- | --- |
| Committed notebook admissibility | The existing current-HEAD test passed. In-memory cases rejected unsupported format, language, raw cells, attachments and the tested private-key marker. Saved outputs and execution counts were cleared. |
| Native archive flow | Reviewed the JUPYTER imports, DBC export, archive validation, DBC reimport, per-notebook JUPYTER export and cell comparison. Existing fake-workspace tests passed, including changed content, significant markdown whitespace, archive directory traversal and cleanup failure handling. This does not establish actual Databricks API compatibility. |
| Staging identity | In-memory cases rejected changed plan, source commit, version, hash, size and round-trip status. |
| Immutable DBC upload | Confirmed `--overwrite false`; identical existing bytes avoided upload. Conflicts, including a conflicting post-upload observation, failed. An upload error was accepted only when the subsequent public observation matched; absence failed. |
| Anonymous checks | The artifact-response fixture asserted that requests carried no Authorization header. DBC code builds a separate no-redirect opener and verifies the downloaded hash and source-commit metadata. |
| PyPI availability | Existing test functions passed for missing, source-only, wrongly named, yanked and unavailable wheels, malformed file lists and disallowed download URLs. These checks do not address F1. |
| Maven inventory and local receipts | In-memory cases rejected missing modules, missing Core tests JAR, missing required wheel/DBC, unexpected classifiers, wrong versions and empty staged artifacts. Wheel receipts rejected wrong names/versions, duplicate metadata/version fields, wrong filenames and symlinks; the valid fixture's hash and size matched its bytes. |
| Evidence freshness and transport | Stale and excessively future-dated inventory failed. Oversized decompression, invalid base64 and duplicate JSON members failed. The complete dispatch envelope passed at its exact calculated limit and failed one character over on both encoding and decoding. Missing DBC coverage and a changed DBC hash failed. |
| Notes integration prerequisite | The workflow runs `verify-primary-integration` before artifact validation and release creation. In-memory Git/API responses accepted direct ancestry or the exact merged canonical candidate, and rejected an unmerged candidate, wrong final head, wrong base and an unreachable merge result. |

Execution evidence:

- **51 existing pytest cases passed; 37 were deselected.** The run covered `test_release_dbc.py` and `test_release_dbc_contract.py`, with cache and logging plugins disabled and system-stream capture. A collection hook excluded cases whose fixtures require filesystem writes or local Git commits.
- **70 additional write-free, in-memory fixture cases passed** across artifact availability, DBC source/staging/upload checks, freshness, parsing, integration, Maven inventories and wheel receipts.
- The separate F1 fixture demonstrated acceptance through `run_plan`, `validate_evidence`, `encode_evidence` and `decode_evidence`, with 61 anonymous HEAD requests.
- Test execution used Python `3.14.6` and pytest `9.1.1`. Audit hooks prohibited networking and filesystem writes. The existing committed-notebook test was allowed only read-only `git ls-tree` and `git show` subprocesses.

## Limitations

No live resources, network requests, credentials, production tags, commits or source edits were used. Tests that create files or commits were excluded rather than weakening that restriction. No native Databricks workspace, actual Azure upload condition, public download, Maven Central/PyPI response, or canonical GitHub integration state was exercised. The fixture runtime is not proof of the pipeline's Python runtime or managed-service behavior.

F1 remains unresolved at the reviewed head. I found no additional reproducible blocker in the owned paths under these constraints.

## Resolution work in the shared working tree

The bounded producer and consumer changes are implemented without changing the reviewed commit or creating a commit:

- `verify_release.py` now has destination-specific receipt validation, bounded anonymous GET/hash collection, exact observation validation, and an invocation-local download cache. Required CDN files use independent `blob_artifacts`; Central and PyPI use the existing producer artifact identities. PyPI file metadata must also match its receipt. Availability-only inventory remains separate.
- `release_guard.py` records the actual published Maven-local files into a source/build/plan-bound Blob receipt. Final public Maven manifests use schema 2 and require that handoff. Notes recheck public downloads before producing installation text.
- `pipeline.yaml` retains the Blob receipt from the exact Publish attempt and downloads that selected artifact in Release. It also restores authorized primary-only API documentation publication in the guarded Publish job, after wrapper generation and before Maven upload.
- A focused `test_artifact_content.py` proves that independently built CDN and Central files can differ and still pass, while wrong destination hashes, missing coverage, changed bytes, wrong sizes, redirects and contradictory PyPI metadata fail.

Targeted evidence:

| Command | Result |
| --- | --- |
| `python -B -m pytest scripts/release/test_artifact_content.py -q -x -p no:cacheprovider`, before helper implementation | Failed as expected because the content-proof helper did not exist |
| `python -B -m pytest scripts/release/test_artifact_content.py scripts/release/test_verify_release.py -q -p no:cacheprovider` | 102 passed |
| `python -B -m pytest scripts/release/test_release_guard.py -q -p no:cacheprovider -k 'maven_receipt or pypi_receipt or blob_receipt'` | 27 passed, 84 deselected |
| `python -B -m pytest tools/ci/tests/test_pipeline_yaml.py -q -p no:cacheprovider -k 'blob_receipt or maven_receipt or primary_api_docs or publish_jobs or pipeline_and_templates_parse'` | 5 passed, 72 deselected |

At this checkpoint, the separately owned `release_ops.py` producer-proof integration is still pending. The broader DBC contract run had 114 passed, 13 platform skips, and one failure: its successful-notes fixture still emits an old manifest without CDN proof, which the new guard correctly rejects. That fixture must be upgraded with actual synthetic content, not bypass the new check. Full public-evidence integration and the all-three-runtime compressed/combined budget regression must pass before F1 is marked fully resolved.

No live downloads, uploads, service calls, production tags, commits or pushes were performed. The pipeline documentation task was checked statically, not executed against storage.

The documentation follow-up adds `release_guard.py api-docs`, which resolves the approved primary source and version rather than accepting an arbitrary version input. After `publishDocs`, three bounded anonymous GETs require nonempty Python, Scala, and Scala package index pages. Graphviz/doxygen prerequisites are restored only in the primary release path; port and snapshot behavior remain separate. The focused documentation and pipeline command completed with 12 passed and 120 deselected. Documentation remains mutable under the existing storage policy; these checks do not claim an immutable API-documentation receipt.

### Definitive absence and shared filename follow-up

The verifier now imports the shared `release_matrix.public_pypi_wheel_name`; the verifier and release guard resolve to the same helper.

`public_maven_absence(target)` provides the recovery state machine with a separate, conservative namespace proof. For every required module, a bounded anonymous CDN container-list request must establish an empty exact version prefix without a continuation marker, and the corresponding Central version directory must return HTTP 404. Any remaining blob, including a signature or source-only remnant, or an existing Central directory returns false. Forbidden listing, transport errors, malformed or incomplete XML coverage, oversized responses, and DTD/wide-encoded input fail closed. Aggregate `MISSING` inventory is not absence proof.

`python -B -m pytest scripts/release/test_artifact_content.py scripts/release/test_verify_release.py -q -p no:cacheprovider` passed 116 cases before six additional absence-error cases were added. The final focused command, `python -B -m pytest scripts/release/test_artifact_content.py -q -p no:cacheprovider`, passed all 53 cases, including 13 namespace cases. No request reached a live service. Scoped whitespace checks passed; the available Black 26.5.1 accepted the focused test file, but this is not a claim of validation with the repository's Black 22.3.0 pin.

The absence helper contract has been handed to the recovery owner; state-machine wiring and full producer-evidence integration remain separately owned and are not claimed complete here. Anonymous CDN container listing must be available operationally: if it is disabled, first-publication automation blocks rather than treating inaccessible storage as empty. No fallback to HEAD, automatic republish, or plan identity change was introduced.

### Final working-tree resolution

**F1 is resolved in the collaboratively edited working tree within the offline scope.** HEAD remains `7bc3019409803a3eaf33e9b68b4f84debb5f4846`; no fix commit was created. The initial review and intermediate checkpoints above describe their respective earlier states, not the final integration.

The recovery owner integrated destination-specific manifest schema 2 and measured `public_artifacts` observations into live receipts and public producer-evidence schema 3. The consumer regressions now reject old manifests, missing CDN receipts, and missing download observations. Plan schemas 2 and 4 retain their identities. New tests verify one download per artifact per invocation for resume, status, and evidence export, and require fresh downloads on the next invocation.

An integration fixture initially returned receipt hashes as if they were measured download hashes. The new `test_changed_receipt_cannot_redefine_published_fixture_bytes` failed against that fixture. The recovery owner replaced it with independent URL-to-bytes storage and separately stored PyPI metadata; the regression now passes. Notes tests also change the actual downloaded bytes, keeping their length unchanged, at each of CDN, Central, and PyPI. Notes refuse all three mismatches before creating installation output.

The three-runtime, widespread-warning case initially exceeded the evidence limit. A diagnostic on the same report measured 61,880 encoded characters with alphabetically sorted JSON keys versus 57,952 with producer field order. `encode_evidence` now preserves that order before gzip compression. No proof fields, jobs, receipts, observations, or artifacts were dropped; neither the 60,000-character evidence limit nor the 65,535-character complete-dispatch limit changed. The existing gzip/JSON decoder remains compatible, and canonical plan/operation hashing is unchanged. All four production-sized budget variants pass with distinct producer hashes and independent synthetic download bytes.

The final recovery implementation uses its own approved-file absence scan. The unused container-list prototype and its 13 prototype-only cases were removed from this artifact patch to avoid retaining a second, unwired absence policy. This supersedes the earlier namespace-helper checkpoint. No recovery-owned source or tests were edited here.

Final commands and results:

| Command | Result |
| --- | --- |
| `python -B -m pytest scripts/release/test_artifact_content.py scripts/release/test_verify_release.py scripts/release/test_release_guard.py tools/ci/tests/test_pipeline_yaml.py -q -p no:cacheprovider -k 'test_artifact_content or test_verify_release or maven_receipt or blob_receipt or pypi_receipt or primary_api_docs or publish_jobs or pipeline_and_templates_parse'` | 141 passed, 171 deselected |
| `python -B -m pytest scripts/release/test_release_public.py scripts/release/test_release_dbc_contract.py scripts/release/test_release_warnings.py -q -p no:cacheprovider -k 'test_release_public or test_release_dbc_contract or production_sized_warning_evidence'` | 94 passed, 13 Windows skips, 79 deselected |
| `python -B -m pytest tools/ci/tests/test_pipeline_yaml.py -q -p no:cacheprovider -k primary_api_docs` | 1 passed, 91 deselected |

The last pipeline regression explicitly requires API documentation publication and its three version-bound public checks inside the primary Publish gate before `publishBlob`, and forbids a duplicate documentation publisher in Release. The documentation owner confirmed the implemented contract and updated its owned operator guidance.

Files changed by this artifact owner are `pipeline.yaml`, `scripts/release/verify_release.py`, the receipt/content/notes portions of `scripts/release/release_guard.py`, `scripts/release/test_artifact_content.py`, `scripts/release/test_release_public.py`, the receipt portions of `scripts/release/test_release_guard.py`, `scripts/release/test_release_dbc_contract.py`, the receipt/docs portions of `tools/ci/tests/test_pipeline_yaml.py`, and this report. Shared runtime, recovery, and parent-owned edits were preserved.

Remaining limits: there were no live downloads, uploads, native service runs, production tags, commits, or pushes. Linux-only shell cases were skipped on Windows. Parent-owned combined validation, pinned-format validation, and CI remain outside this result. The active absence scan's real-service latency is unmeasured. API docs retain their existing mutable policy. If later work in the Publish task fails after Blob upload but before pipeline-artifact retention, the upload may exist without the retained receipt; the new proof requirement blocks completion and automatic republishing rather than inventing provenance.

### Rehearsal fixture call-site follow-up

The recovery owner identified one remaining external caller of its strengthened fixture. In `scripts/release/test_release_rehearsal.py`, the simulated-publication test now passes `remote=cli.remote` to `produced_maven_receipt`, so the fixture registers the actual independently produced Central, Blob, and wheel bytes rather than leaving the previous fake publication in place. This is one additional changed file, limited to that call's arguments.

`python -B -m pytest scripts/release/test_release_rehearsal.py -q -p no:cacheprovider -k simulated_publication_resumes --tb=short` failed both selected cases before the change and passed both afterward, with 10 deselected. The selected two- and three-runtime rehearsals do not use the Git-commit/tag fixtures. No recovery-owned source was edited.

A final fresh-process check of the persisted code, without an encoder/compression monkeypatch, ran `python -B -m pytest scripts/release/test_release_warnings.py scripts/release/test_release_rehearsal.py -q -p no:cacheprovider -k 'production_sized_warning_evidence or simulated_publication_resumes' --tb=short`: **6 passed, 89 deselected**. This covers all four production-budget combinations plus both real-producer rehearsal variants.
