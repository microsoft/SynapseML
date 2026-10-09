# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import copy
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

import release_guard as guard
import release_matrix as matrix
import release_ops as ops
import verify_release as verify
from test_release_ops import cli, no_network  # noqa: F401
from test_release_public import producer_report


def plan():
    return matrix.build_plan(
        "1.2.0", oss_commits={"master": "a" * 40, "spark4.1": "b" * 40}
    )


def test_new_public_plans_require_runtime_matched_dbc_archives():
    selected = plan()
    assert selected.schema_version == 4
    rows = ops._required_rows(selected)
    for target in selected.targets:
        name = f"dbcs/SynapseMLExamplesv{target.oss_maven_version}.dbc"
        assert ("dbc", target.key, name, target.oss_maven_version) in rows


def test_saved_public_plan_keeps_its_original_identity_and_scope():
    original = matrix.plan_to_dict(plan())
    original["schema_version"] = 2
    original["plan_id"] = matrix.plan_digest(original)
    saved = copy.deepcopy(original)
    selected = matrix.load_plan(original, require_bound=True)
    assert matrix.plan_to_dict(selected) == saved
    assert not any(key[0] == "dbc" for key in ops._required_rows(selected))
    assert selected.plan_id != plan().plan_id


def test_new_release_evidence_cannot_omit_notebook_archives():
    selected = plan()
    rows, _ = verify._check_plan(
        selected, None, None, [], True, checker=verify._InventoryChecker()
    )
    for row in rows:
        row["status"] = verify.OK
        if "expected_commit" in row:
            row["actual_commit"] = row["expected_commit"]
    rows = [row for row in rows if row["kind"] != "dbc"]
    report = verify.build_report(selected, rows, True)
    with pytest.raises(ValueError, match="omits required rows"):
        verify.validate_inventory(selected, report)


@pytest.mark.parametrize("field", ["sha256", "size"])
def test_release_notes_reject_public_bytes_different_from_producer(cli, field):
    selected, report = producer_report(cli)
    dbc = next(row for row in report["rows"] if row["kind"] == "dbc")
    dbc[field] = "0" * 64 if field == "sha256" else dbc[field] + 1
    with pytest.raises(ValueError, match="DBC hash differs"):
        verify.validate_evidence(selected, report)


def test_release_cannot_complete_while_one_archive_is_missing(cli):
    selected, _ = producer_report(cli)
    cli.remote.missing.add(("oss", "dbc", selected.targets[-1].oss_maven_version))
    report = ops.verified_evidence(selected, cli.state, remote=cli.remote)
    assert not report["complete"]
    with pytest.raises(ValueError):
        verify.validate_evidence(selected, report)


def test_release_notes_advertise_only_selected_archive_variants():
    text = guard.notes_installation(plan())
    assert "SynapseMLExamplesv1.2.0.dbc" in text
    assert "SynapseMLExamplesv1.2.0-spark4.1.dbc" in text
    assert "SynapseMLExamplesv1.2.0-spark4.0.dbc" not in text
    old = matrix.plan_to_dict(plan())
    old["schema_version"] = 2
    old["plan_id"] = matrix.plan_digest(old)
    assert ".dbc" not in guard.notes_installation(matrix.load_plan(old))


@pytest.mark.parametrize(
    "change", ["none", "missing", "hash", "size", "maven", "maven-central", "pypi"]
)
def test_notes_cli_rechecks_live_download_against_producer(
    cli, tmp_path, monkeypatch, change
):
    import json
    import release_dbc

    selected, report = producer_report(cli)
    evidence = tmp_path / "producer.json"
    evidence.write_text(json.dumps(report))
    header = tmp_path / "installation.md"
    current = (b"x" * 321, {"sha256": "f" * 64})
    if change == "missing":
        current = None
    elif change == "hash":
        current = (b"x" * 321, {"sha256": "0" * 64})
    elif change == "size":
        current = (b"x" * 322, {"sha256": "f" * 64})
    public_prefixes = {
        "maven": verify.MAVEN_BASE,
        "maven-central": verify.MAVEN_CENTRAL_BASE,
        "pypi": "https://files.pythonhosted.org",
    }
    if change in public_prefixes:
        url = next(
            url
            for url in cli.remote.public_bytes
            if url.startswith(public_prefixes[change])
        )
        original = cli.remote.public_bytes[url]
        cli.remote.public_bytes[url] = bytes([original[0] ^ 1]) + original[1:]
    monkeypatch.setattr(release_dbc, "fetch_public_archive", lambda *_: current)
    result = guard.main(
        [
            "notes",
            "--plan",
            str(cli.plan),
            "--evidence",
            str(evidence),
            "--approve-plan",
            selected.plan_id,
            "--tag",
            f"v{selected.oss_version}",
            "--commit",
            selected.targets[0].oss_commit,
            "--installation-output",
            str(header),
        ]
    )
    assert result == (0 if change == "none" else 2)
    assert header.exists() is (change == "none")


@pytest.fixture
def pipeline_jobs():
    root = Path(__file__).resolve().parents[2]
    data = yaml.safe_load((root / "pipeline.yaml").read_text())

    def jobs(node):
        if isinstance(node, dict):
            if "job" in node:
                yield node
            for value in node.values():
                yield from jobs(value)
        elif isinstance(node, list):
            for value in node:
                yield from jobs(value)

    return {job["job"]: job for job in jobs(data)}


def publication_steps(job, release=True):
    for step in job["steps"]:
        conditional = "${{ if eq(parameters.publishRelease, true) }}"
        if conditional in step:
            if release:
                yield from step[conditional]
        else:
            yield step


def test_native_dbc_validation_precedes_public_maven_upload(pipeline_jobs):
    steps = list(publication_steps(pipeline_jobs["Publish"]))
    names = [step.get("displayName") for step in steps]
    assert "Build and round-trip release notebook archive" in names
    build = names.index("Build and round-trip release notebook archive")
    retain = names.index("Retain validated release notebook archive")
    publish = names.index("Publish Artifacts")
    source = names.index("Validate approved release source before artifact publication")
    assert source < build < retain < publish
    assert "publishBlob" in steps[publish]["inputs"]["inlineScript"]
    assert steps[publish].get("condition", "succeeded()") == "succeeded()"
    assert steps[build]["name"] == "buildDbc"
    assert steps[retain]["inputs"]["artifact"] == "$(buildDbc.artifactName)"
    assert "System.JobAttempt" in steps[retain]["inputs"]["targetPath"]
    for index in (build, retain, publish):
        assert not steps[index].get("continueOnError", False)
    ordinary = list(publication_steps(pipeline_jobs["Publish"], release=False))
    assert all(
        step.get("displayName")
        not in {
            "Build and round-trip release notebook archive",
            "Retain validated release notebook archive",
        }
        for step in ordinary
    )


def test_native_dbc_publication_reuses_the_validated_publish_artifact(pipeline_jobs):
    release = pipeline_jobs["Release"]
    steps = release["steps"]
    names = [step.get("displayName") for step in steps]
    validate = names.index("Validate notebook artifact handoff")
    download = names.index("Download validated release notebook archive")
    publish = names.index("Publish and verify public release notebook archive")
    receipt = next(i for i, s in enumerate(steps) if "--receipt " in s.get("bash", ""))
    assert "Publish" in release["dependsOn"]
    assert release["variables"]["releaseDbcArtifact"] == (
        "$[ dependencies.Publish.outputs['buildDbc.artifactName'] ]"
    )
    assert validate < download < publish < receipt
    assert not any(
        "release_dbc.py build" in str(step) for step in steps
    ), "Release retries must reuse the already validated archive"
    assert steps[download]["task"] == "DownloadPipelineArtifact@2"
    assert steps[download]["inputs"] == {
        "buildType": "current",
        "artifactName": "$(releaseDbcArtifact)",
        "targetPath": "$(Build.ArtifactStagingDirectory)/dbc-release-$(System.JobAttempt)",
    }
    assert steps[validate]["env"]["DBC_ARTIFACT"] == "$(releaseDbcArtifact)"
    for index in (validate, download):
        assert steps[index]["condition"] == (
            "and(succeeded(), eq(variables.releaseDbc, 'true'))"
        )
    for name in ("publish python package to pypi", "ESRP Publish Package"):
        assert publish < names.index(name)
    build = next(
        step
        for step in publication_steps(pipeline_jobs["Publish"])
        if step.get("displayName") == "Build and round-trip release notebook archive"
    )
    for step in (build, steps[publish]):
        assert step["task"] == "AzureCLI@2"
        assert step["inputs"]["azureSubscription"] == "SynapseML Build"
        assert step["condition"] == "and(succeeded(), eq(variables.releaseDbc, 'true'))"
        assert step["env"]["RELEASE_PLAN_ID"] == "${{ parameters.release_plan_id }}"
        assert (
            step["env"]["RELEASE_PLAN_BASE64"]
            == "${{ parameters.release_plan_base64 }}"
        )
    assert "--dbc-directory" in steps[receipt]["bash"]
    directory = steps[download]["inputs"]["targetPath"]
    assert f'--directory "{directory}"' in steps[publish]["inputs"]["inlineScript"]
    assert f'--dbc-directory "{directory}"' in steps[receipt]["bash"]


@pytest.mark.skipif(sys.platform == "win32", reason="Executes the Linux CI Bash step")
@pytest.mark.parametrize("attempt", [1, 2])
@pytest.mark.parametrize("native_exit", [0, 37])
def test_publish_exports_only_successfully_validated_archive_attempts(
    pipeline_jobs, tmp_path, attempt, native_exit
):
    step = next(
        step
        for step in publication_steps(pipeline_jobs["Publish"])
        if step.get("displayName") == "Build and round-trip release notebook archive"
    )
    script = (
        step["inputs"]["inlineScript"]
        .replace("$(Build.ArtifactStagingDirectory)", str(tmp_path))
        .replace("$(System.JobAttempt)", str(attempt))
    )
    capture = tmp_path / "arguments.txt"
    result = subprocess.run(
        [
            shutil.which("bash") or "/bin/bash",
            "-c",
            'python3() { printf "%s\\n" "$@" > "$CAPTURE"; return "$NATIVE_EXIT"; }\n'
            + script,
        ],
        cwd=tmp_path,
        env={"CAPTURE": str(capture), "NATIVE_EXIT": str(native_exit)},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == native_exit
    assert capture.read_text().splitlines() == [
        "scripts/release/release_dbc.py",
        "build",
        "--directory",
        str(tmp_path / f"dbc-release-{attempt}"),
    ]
    marker = (
        "##vso[task.setvariable variable=artifactName;isOutput=true]"
        f"release-dbc-{attempt}"
    )
    assert (marker in result.stdout) is (native_exit == 0)


@pytest.mark.skipif(sys.platform == "win32", reason="Executes the Linux CI Bash step")
@pytest.mark.parametrize(
    "artifact,valid",
    [
        ("release-dbc-1", True),
        ("release-dbc-2", True),
        ("release-dbc-12", True),
        ("", False),
        ("$(releaseDbcArtifact)", False),
        ("release-dbc-0", False),
        ("release-dbc-other", False),
        ("other-1", False),
        ("release-dbc-1\nother", False),
    ],
)
def test_release_rejects_missing_or_invalid_archive_handoffs(
    pipeline_jobs, tmp_path, artifact, valid
):
    step = next(
        step
        for step in pipeline_jobs["Release"]["steps"]
        if step.get("displayName") == "Validate notebook artifact handoff"
    )
    result = subprocess.run(
        [shutil.which("bash") or "/bin/bash", "-c", step["bash"]],
        cwd=tmp_path,
        env={"DBC_ARTIFACT": artifact},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == (0 if valid else 1)
    if not valid:
        assert "validated notebook artifact" in result.stderr
