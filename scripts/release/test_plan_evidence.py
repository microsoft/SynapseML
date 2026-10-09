# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import copy
import base64
import gzip
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from test_release_config import private_profile  # noqa: F401

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_matrix as matrix  # noqa: E402
import verify_release as verify  # noqa: E402
from test_verify_release import AlwaysPresentChecker  # noqa: E402
from test_release_ops import cli  # noqa: F401, E402
from test_release_public import producer_report  # noqa: E402

OSS_SHA = "a" * 40
INTERNAL_SHA = "b" * 40


class BoundChecker(AlwaysPresentChecker):
    def github_tag(self, _tag):
        return verify.OK, OSS_SHA

    def ado_tag(self, _tag):
        return verify.OK, INTERNAL_SHA


def plan(**overrides):
    kwargs = {
        "target_keys": ["master"],
        "repositories": ["oss", "internal"],
        "families": ["maven", "pip", "upack"],
        "oss_commits": {"master": OSS_SHA},
        "internal_commits": {"master": INTERNAL_SHA},
    }
    kwargs.update(overrides)
    if kwargs["repositories"] == ["oss"] and "internal_commits" not in overrides:
        kwargs.pop("internal_commits")
    return matrix.build_plan("1.1.4", **kwargs)


def evidence(monkeypatch, release_plan):
    monkeypatch.setattr(verify, "Checker", BoundChecker)
    rows, complete = verify.run_plan(release_plan)
    return verify.build_report(release_plan, rows, complete)


def test_matching_tag_family_at_wrong_commit_is_incomplete(monkeypatch):
    monkeypatch.setattr(verify, "Checker", AlwaysPresentChecker)
    rows, complete = verify.run_plan(plan())
    assert not complete
    tag = next(row for row in rows if row["kind"] == "git-tag")
    assert tag["expected_commit"] == OSS_SHA
    assert tag["actual_commit"] == "github-commit"
    assert tag["status"] == verify.MISSING


def test_bound_report_records_actual_commits_and_plan(monkeypatch):
    release_plan = plan()
    report = evidence(monkeypatch, release_plan)
    assert report["inventory_complete"]
    assert not report["complete"]
    assert report["plan_id"] == release_plan.plan_id
    assert report["coverage"]["skipped"] == 0
    verify.validate_inventory(release_plan, report)
    with pytest.raises(ValueError, match="inventory alone"):
        verify.validate_evidence(release_plan, report)


def test_repository_and_family_selection_controls_real_checks(monkeypatch):
    class UpackOnly(BoundChecker):
        def public_maven(self, *_args):
            raise AssertionError("Maven is not selected")

        def internal_maven(self, *_args):
            raise AssertionError("Internal is not selected")

        def public_pypi(self, *_args):
            raise AssertionError("Public PyPI is not selected")

        def ado_tag(self, _tag):
            raise AssertionError("Internal is not selected")

        def pip(self, *_args, **_kwargs):
            raise AssertionError("Wheels are not selected")

        def upack(self, _package, _version, internal=False):
            assert not internal
            return verify.OK

    monkeypatch.setattr(verify, "Checker", UpackOnly)
    rows, complete = verify.run_plan(plan(families=["upack"], repositories=["oss"]))
    assert complete
    assert {row["kind"] for row in rows} == {"git-tag", "tag-set", "upack"}


def test_all_skipped_is_never_complete():
    rows, complete = verify.run_plan(plan(), skip=sorted(verify.SKIP_CHOICES))
    assert rows
    assert all(row["status"] == verify.SKIPPED for row in rows)
    assert not complete


def test_blob_visibility_cannot_hide_incomplete_central_publication(monkeypatch):
    class MissingCentral(BoundChecker):
        def public_central_maven(self, *_args):
            return verify.MISSING

    monkeypatch.setattr(verify, "Checker", MissingCentral)
    rows, complete = verify.run_plan(plan(repositories=["oss"], families=["maven"]))
    assert not complete
    assert {row["kind"] for row in rows if row["status"] == verify.MISSING} == {
        "maven-central"
    }
    assert len([row for row in rows if row["kind"] == "maven-central"]) == len(
        verify.PUBLIC_MAVEN_MODULES
    )


def test_draft_fails_before_authentication(monkeypatch):
    def unexpected(*_args, **_kwargs):
        raise AssertionError("draft must not contact remote services")

    monkeypatch.setattr(verify, "Checker", unexpected)
    with pytest.raises(ValueError, match="commit"):
        verify.run_plan(matrix.build_plan("1.1.4"))


@pytest.mark.parametrize(
    "corruption", ["identity", "scope", "rows", "duplicate", "skip", "sha", "time"]
)
def test_evidence_cannot_approve_a_different_or_partial_release(
    monkeypatch, corruption
):
    release_plan = plan()
    report = evidence(monkeypatch, release_plan)
    if corruption == "identity":
        report["plan_id"] = "c" * 64
    elif corruption == "scope":
        report["scope"] = "internal-only"
    elif corruption == "rows":
        report["rows"].pop()
    elif corruption == "duplicate":
        report["rows"].append(copy.deepcopy(report["rows"][0]))
    elif corruption == "skip":
        report["rows"][-1]["status"] = verify.SKIPPED
    elif corruption == "sha":
        report["rows"][0]["actual_commit"] = "c" * 40
    else:
        report["checked_at"] = (
            datetime.now(timezone.utc) - timedelta(days=2)
        ).isoformat()
    with pytest.raises(ValueError):
        verify.validate_inventory(release_plan, report)


def test_plan_cli_rejects_reentered_coordinates(tmp_path, capsys):
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(matrix.plan_to_dict(plan())), encoding="utf-8")
    assert verify.main(["--plan", str(path), "--targets", "spark4.0"]) == 2
    assert "cannot" in capsys.readouterr().err


def test_plan_cli_cannot_turn_inventory_into_approval(tmp_path, monkeypatch, capsys):
    release_plan = plan()
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(matrix.plan_to_dict(release_plan)), encoding="utf-8")
    monkeypatch.setattr(verify, "Checker", BoundChecker)
    assert verify.main(["--plan", str(path), "--json"]) == 1
    report = json.loads(capsys.readouterr().out)
    verify.validate_inventory(release_plan, report)
    assert not report["complete"]
    report["complete"] = True
    with pytest.raises(ValueError, match="inventory alone"):
        verify.validate_evidence(release_plan, report)
    assert verify.main(["--plan", str(path), "--inventory-only", "--json"]) == 0
    assert not json.loads(capsys.readouterr().out)["complete"]


def test_compressed_evidence_round_trip_is_bounded(cli):
    _, report = producer_report(cli)
    encoded = verify.encode_evidence(report)
    assert len(encoded) <= verify.MAX_GITHUB_EVIDENCE_CHARS
    assert verify.decode_evidence(encoded) == report
    oversized = base64.b64encode(
        gzip.compress(b"x" * (verify.MAX_EVIDENCE_BYTES + 1))
    ).decode()
    with pytest.raises(ValueError, match="oversized"):
        verify.decode_evidence(oversized)
    with pytest.raises(ValueError):
        verify.decode_evidence("not base64")
    corrupt = bytearray(base64.b64decode(encoded))
    corrupt[10:15] = b"\xff" * 5
    with pytest.raises(ValueError):
        verify.decode_evidence(base64.b64encode(corrupt).decode("ascii"))


def test_github_export_rejects_combined_plan_before_state_access(tmp_path, capsys):
    path = tmp_path / "public-with-private-binding.json"
    release_plan = plan(repositories=["oss", "internal"], families=["maven"])
    path.write_text(json.dumps(matrix.plan_to_dict(release_plan)), encoding="utf-8")
    assert (
        verify.main(
            [
                "--plan",
                str(path),
                "--state",
                str(tmp_path / "absent-state.json"),
                "--github-evidence",
            ]
        )
        == 2
    )
    assert "public-only Maven plan" in capsys.readouterr().err


@pytest.mark.parametrize("destination", ["maven", "maven-central", "pypi"])
def test_fleet_public_content_mismatch_cannot_complete_or_resubmit(cli, destination):
    import release_ops as ops

    release_plan, _ = producer_report(cli)
    queued = len(cli.remote.queued)
    base = {
        "maven": verify.MAVEN_BASE,
        "maven-central": verify.MAVEN_CENTRAL_BASE,
        "pypi": "https://files.pythonhosted.org/",
    }[destination]

    for url, content in cli.remote.public_bytes.items():
        if url.startswith(base):
            cli.remote.public_bytes[url] = b"\x00" + content[1:]
    with pytest.raises(ops.ReleaseReadError):
        ops.verified_evidence(release_plan, cli.state, remote=cli.remote)
    assert cli(plan=release_plan, apply=True)[0] == 2
    assert len(cli.remote.queued) == queued
    state = json.loads(cli.state.read_text())
    assert state["actions"][0]["status"] == "unknown"
    assert state["actions"][0]["build_id"] is not None


@pytest.mark.parametrize("fault", ["missing", "hash", "destination", "duplicate"])
def test_fleet_public_download_observations_are_required_and_bound(cli, fault):
    release_plan, report = producer_report(cli)
    run = report["producer_evidence"]["runs"][0]
    observations = run["public_artifacts"]
    assert {item["destination"] for item in observations} == {
        "maven",
        "maven-central",
        "pypi",
    }
    if fault == "missing":
        run.pop("public_artifacts")
    elif fault == "hash":
        observations[0]["sha256"] = "0" * 64
    elif fault == "destination":
        observations[0]["destination"] = "unapproved"
    else:
        observations.append(copy.deepcopy(observations[0]))
    with pytest.raises(ValueError):
        verify.validate_evidence(release_plan, report)


def test_fleet_old_public_receipt_does_not_authorize_a_repeat_publication(cli):
    import release_ops as ops

    release_plan, _ = producer_report(cli)
    state = json.loads(cli.state.read_text())
    for item in state["actions"]:
        item["receipt"].pop("public_artifacts")
        document = item["receipt"]["provenance"][0]
        document["schema_version"] = 1
        document.pop("blob_artifacts")
        cli.remote.manifests[item["build_id"]] = [copy.deepcopy(document)]
    state["state_id"] = ops._digest(state, "state_id")
    cli.state.write_text(json.dumps(state), encoding="utf-8")
    queued = len(cli.remote.queued)
    assert cli(plan=release_plan, apply=True)[0] == 2
    assert len(cli.remote.queued) == queued
    assert json.loads(cli.state.read_text())["actions"][0]["status"] == "unknown"


def test_fleet_public_content_evidence_uses_schema_three(cli):
    release_plan, report = producer_report(cli)
    assert report["producer_evidence"]["schema_version"] == 3
    verify.validate_evidence(release_plan, report)


@pytest.mark.parametrize("version", [1, 2])
def test_fleet_public_content_evidence_rejects_legacy_envelopes(cli, version):
    release_plan, report = producer_report(cli)
    report["producer_evidence"]["schema_version"] = version
    with pytest.raises(ValueError, match="Producer evidence"):
        verify.validate_evidence(release_plan, report)


def test_fleet_public_content_fixture_hashes_are_destination_specific(cli):
    _, report = producer_report(cli)
    observations = report["producer_evidence"]["runs"][0]["public_artifacts"]
    assert len({item["sha256"] for item in observations}) == len(observations)


@pytest.mark.parametrize("destination", ["maven", "maven-central", "pypi"])
def test_fleet_download_fixture_does_not_follow_receipt_tampering(cli, destination):
    release_plan, _ = producer_report(cli)
    document = cli.remote.manifests[101][0]
    if destination == "maven":
        artifact = document["blob_artifacts"][0]
    else:
        artifact = next(
            item
            for item in document["artifacts"]
            if item["path"].startswith("pypi/") == (destination == "pypi")
        )
    artifact["sha256"] = "0" * 64
    queued = len(cli.remote.queued)
    assert cli("status", plan=release_plan)[0] == 2
    assert len(cli.remote.queued) == queued


@pytest.mark.parametrize("command", ["status", "resume", "verified_evidence"])
def test_fleet_public_content_reads_once_and_rechecks_the_next_invocation(
    cli, monkeypatch, command
):
    import release_ops as ops

    release_plan, _ = producer_report(cli)
    expected_downloads = sum(
        31 if target.key == "master" else 30 for target in release_plan.targets
    )
    downloads = []
    original = verify._download_public_artifact

    def downloaded(url, size):
        downloads.append(url)
        return original(url, size)

    monkeypatch.setattr(verify, "_download_public_artifact", downloaded)
    for _ in range(2):
        downloads.clear()
        if command == "verified_evidence":
            report = ops.verified_evidence(release_plan, cli.state, remote=cli.remote)
        else:
            code, report, error = cli(command, plan=release_plan)
            assert code == 0, error
        assert report["complete"]
        assert len(downloads) == len(set(downloads)) == expected_downloads
