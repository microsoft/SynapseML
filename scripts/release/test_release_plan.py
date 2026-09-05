# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import hashlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_matrix as matrix  # noqa: E402

OSS_SHA = "a" * 40
INTERNAL_SHA = "b" * 40


def bound_plan(**overrides):
    options = {
        "target_keys": ["master"],
        "oss_commits": {"master": OSS_SHA},
        "internal_commits": {"master": INTERNAL_SHA},
    }
    options.update(overrides)
    return matrix.build_plan("1.1.4", **options)


def resign(data):
    body = {key: value for key, value in data.items() if key != "plan_id"}
    data["plan_id"] = hashlib.sha256(
        json.dumps(
            body, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("utf-8")
    ).hexdigest()
    return data


def test_plan_round_trip_is_bound_and_deterministic():
    plan = bound_plan()
    data = matrix.plan_to_dict(plan)

    assert data["schema_version"] == 1
    assert len(data["plan_id"]) == 64
    assert matrix.plan_to_dict(matrix.load_plan(data, require_bound=True)) == data
    assert matrix.plan_to_dict(bound_plan()) == data


def test_draft_plan_cannot_be_used_for_writes():
    data = matrix.plan_to_dict(matrix.build_plan("1.1.4"))
    assert matrix.load_plan(data).targets[0].oss_commit is None
    with pytest.raises(ValueError, match="commit"):
        matrix.load_plan(data, require_bound=True)


def test_changed_plan_requires_new_approval_identity():
    first = matrix.plan_to_dict(bound_plan())
    second = matrix.plan_to_dict(bound_plan(internal_commits={"master": "c" * 40}))
    assert first["plan_id"] != second["plan_id"]
    first["targets"][0]["internal_commit"] = "c" * 40
    with pytest.raises(ValueError, match="digest|plan_id"):
        matrix.load_plan(first)


@pytest.mark.parametrize("version", [None, True, 0, 2, "1"])
def test_unknown_or_mistyped_schema_is_rejected(version):
    data = matrix.plan_to_dict(bound_plan())
    data["schema_version"] = version
    with pytest.raises(ValueError, match="schema"):
        matrix.load_plan(resign(data))


@pytest.mark.parametrize(
    "alter",
    [
        lambda data: data["targets"][0].update(internal_maven_version="9.9.9"),
        lambda data: data["publish_parameters"].update(build_synapseml_pip_py313=True),
        lambda data: data.update(internal_maven_pipeline_id=123),
        lambda data: data.update(ado_org="https://example.invalid"),
        lambda data: data.update(extra="unreviewed"),
    ],
)
def test_rehashed_inconsistent_plan_still_fails(alter):
    data = matrix.plan_to_dict(bound_plan())
    alter(data)
    with pytest.raises(ValueError):
        matrix.load_plan(resign(data))


def test_oss_upack_recovery_enables_no_other_repo_or_family():
    plan = bound_plan(
        families=["upack"],
        repositories=["oss"],
        upack_iteration={"master": 1},
    )
    enabled = {
        key
        for key, value in plan.publish_parameters.items()
        if key.startswith("build_") and value
    }
    assert enabled == {"build_synapseml_upack_default"}
    assert plan.families == ["upack"]
    assert plan.repositories == ["oss"]
    assert matrix.load_plan(matrix.plan_to_dict(plan), require_bound=True)


def test_internal_only_never_enables_oss():
    plan = bound_plan(
        internal_patch="2",
        scope="internal-only",
        families=["pip"],
    )
    assert plan.repositories == ["internal"]
    assert {
        key
        for key, value in plan.publish_parameters.items()
        if key.startswith("build_") and value
    } == {"build_internal_pip_py311"}
    with pytest.raises(ValueError, match="internal-only"):
        bound_plan(internal_patch="2", scope="internal-only", repositories=["oss"])


@pytest.mark.parametrize(
    "options",
    [
        {"families": []},
        {"target_keys": []},
        {"families": ["pip", "pip"]},
        {"families": ["unknown"]},
        {"repositories": []},
        {"repositories": ["oss", "oss"]},
        {"oss_commits": {"unknown": OSS_SHA}},
        {"oss_commits": {"master": "HEAD"}},
        {"oss_commits": {"master": "0" * 40}},
        {"oss_commits": {"master": "A" * 40}},
    ],
)
def test_invalid_selection_or_binding_fails(options):
    with pytest.raises(ValueError):
        bound_plan(**options)


def test_rehearsal_needs_explicit_isolated_destinations():
    with pytest.raises(ValueError, match="feed|destination"):
        bound_plan(mode="rehearsal", families=["pip", "upack"])
    for feed in ("Synapse-Conda", "A365/synapse-conda"):
        with pytest.raises(ValueError, match="production"):
            bound_plan(
                mode="rehearsal",
                families=["pip", "upack"],
                pip_feed=feed,
                upack_feed="release-test-upack",
            )
    with pytest.raises(ValueError, match="maven"):
        bound_plan(
            mode="rehearsal",
            pip_feed="release-test-pip",
            upack_feed="release-test-upack",
        )
    plan = bound_plan(
        mode="rehearsal",
        families=["pip", "upack"],
        pip_feed="release-test-pip",
        upack_feed="release-test-upack",
    )
    assert matrix.load_plan(matrix.plan_to_dict(plan)).mode == "rehearsal"


def test_reader_and_cli_use_the_same_contract(tmp_path, capsys):
    result = matrix.main(
        [
            "--version",
            "1.1.4",
            "--targets",
            "master",
            "--families",
            "upack",
            "--repositories",
            "oss",
            "--oss-commit",
            "master=" + OSS_SHA,
            "--json",
        ]
    )
    assert result == 0
    data = json.loads(capsys.readouterr().out)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(data), encoding="utf-8")
    assert (
        matrix.read_plan(str(plan_path), require_bound=True).plan_id == data["plan_id"]
    )


def test_text_plan_cannot_bypass_guarded_driver():
    text = matrix.render_text(bound_plan())
    assert "az pipelines run" not in text
    assert "release_ops.py" in text
    assert bound_plan().plan_id in text


@pytest.mark.parametrize("option", ["--targets", "--families", "--repositories"])
def test_empty_cli_selection_never_expands_to_everything(option):
    assert matrix.main(["--version", "1.1.4", option, ""]) == 2
