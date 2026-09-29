# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import copy
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


@pytest.mark.parametrize("change", ["none", "missing", "hash", "size"])
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


def test_native_dbc_publication_is_wired_before_release_receipt():
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

    release = next(job for job in jobs(data) if job["job"] == "Release")
    steps = release["steps"]
    build = next(
        i
        for i, s in enumerate(steps)
        if s.get("displayName") == "Build and round-trip release notebook archive"
    )
    retain = next(
        i
        for i, s in enumerate(steps)
        if s.get("displayName") == "Retain validated release notebook archive"
    )
    publish = next(
        i
        for i, s in enumerate(steps)
        if s.get("displayName") == "Publish and verify public release notebook archive"
    )
    receipt = next(i for i, s in enumerate(steps) if "--receipt " in s.get("bash", ""))
    assert build < retain < publish < receipt
    for name in ("publish python package to pypi", "ESRP Publish Package"):
        assert publish < next(
            i for i, step in enumerate(steps) if step.get("displayName") == name
        )
    for index in (build, publish):
        step = steps[index]
        assert step["task"] == "AzureCLI@2"
        assert step["inputs"]["azureSubscription"] == "SynapseML Build"
        assert step["condition"] == "and(succeeded(), eq(variables.releaseDbc, 'true'))"
        assert step["env"]["RELEASE_PLAN_ID"] == "${{ parameters.release_plan_id }}"
        assert (
            step["env"]["RELEASE_PLAN_BASE64"]
            == "${{ parameters.release_plan_base64 }}"
        )
    assert "--dbc-directory" in steps[receipt]["bash"]
    assert "System.JobAttempt" in steps[retain]["inputs"]["targetPath"]
