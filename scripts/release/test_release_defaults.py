# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import json

import pytest

import release_guard as guard
import release_matrix as matrix
import release_ops as ops


def bound_plan(keys=None):
    selected = ["master", "spark4.1"] if keys is None else keys
    return matrix.build_plan(
        "1.2.0",
        target_keys=keys,
        oss_commits={
            key: {"master": "a", "spark4.0": "b", "spark4.1": "c"}[key] * 40
            for key in selected
        },
    )


def test_default_public_plan_and_cli_select_only_master_and_spark41(capsys):
    plan = matrix.build_plan("1.2.0")
    assert [target.key for target in plan.targets] == ["master", "spark4.1"]
    assert "v1.2.0-spark4.0" not in plan.all_oss_tags
    assert "v1.2.0-python3.12" not in plan.all_oss_tags
    assert matrix.main(["--version", "1.2.0", "--json"]) == 0
    assert [
        target["key"] for target in json.loads(capsys.readouterr().out)["targets"]
    ] == ["master", "spark4.1"]


def test_explicit_three_target_plan_keeps_its_preexisting_identity():
    plan = bound_plan(["master", "spark4.0", "spark4.1"])
    assert plan.plan_id == (
        "2872f6280e4022bb5c86cf46c0eaae88b1fd4b3c96f231d31ed7a6bdc8f14443"
    )
    document = matrix.plan_to_dict(plan)
    assert (
        matrix.plan_to_dict(matrix.load_plan(document, require_bound=True)) == document
    )


def test_default_does_not_infer_opt_in_from_extra_commit_bindings():
    with pytest.raises(ValueError, match="unselected target"):
        matrix.build_plan(
            "1.2.0", oss_commits={target.key: "a" * 40 for target in matrix.TARGETS}
        )


def test_full_release_defaults_and_explicit_spark40_policy():
    assert [t.key for t in guard.full_release("1.2.0").targets] == [
        "master",
        "spark4.1",
    ]
    assert [t.key for t in guard.full_release("1.2.0", "true").targets] == [
        "master",
        "spark4.1",
    ]
    included = guard.full_release("1.2.0", include_spark40="true")
    assert [t.key for t in included.targets] == ["master", "spark4.0", "spark4.1"]
    with pytest.raises(ValueError, match="SKIP_SPARK40"):
        guard.full_release("1.2.0", "true", include_spark40="true")


def test_default_plan_needs_no_spark40_repository_policy():
    class Remote:
        def github_variables(self):
            pytest.fail("An unselected optional target must not require its policy API")

    assert ops._policy(bound_plan(), Remote())["required"] is False


@pytest.mark.parametrize("include40", [False, True])
def test_notes_coverage_and_installation_rows_follow_the_approved_plan(include40):
    keys = ["master", "spark4.0", "spark4.1"] if include40 else None
    plan = bound_plan(keys)
    guard.notes_plan(plan, "v1.2.0", "a" * 40, plan.plan_id)
    header = guard.notes_installation(plan)
    assert "| 3.5 | 3.11 |" in header
    assert "| 4.1 | 3.13 |" in header
    assert ("| 4.0 | 3.12 |" in header) is include40
    assert ("v1.2.0-spark4.0" in header) is include40


@pytest.mark.parametrize("keys", [["master"], ["spark4.1"], ["master", "spark4.0"]])
def test_primary_release_notes_still_require_both_default_targets(keys):
    plan = bound_plan(keys)
    with pytest.raises(ValueError, match="target"):
        guard.notes_plan(plan, "v1.2.0", "a" * 40, plan.plan_id)
