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

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_matrix as matrix  # noqa: E402
import verify_release as verify  # noqa: E402
from test_verify_release import AlwaysPresentChecker  # noqa: E402

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
        "oss_commits": {"master": OSS_SHA},
        "internal_commits": {"master": INTERNAL_SHA},
    }
    kwargs.update(overrides)
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


def test_compressed_evidence_round_trip_is_bounded(monkeypatch):
    report = evidence(monkeypatch, plan())
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


def test_github_export_rejects_internal_binding_before_state_access(tmp_path, capsys):
    path = tmp_path / "public-with-private-binding.json"
    release_plan = plan(repositories=["oss"], families=["maven"])
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
