#!/usr/bin/env python3
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
"""Tests for release_matrix. Expected values are transcribed from the LIVE
v1.1.3 / v1.1.1 releases (github tags + BBC-VHD_PublicPackages + Synapse-Conda),
so a regression here means the matrix has drifted from reality."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from release_matrix import build_plan, parse_iterations  # noqa: E402


def _by_key(plan):
    return {tp.key: tp for tp in plan.targets}


def test_rejects_bad_versions():
    for bad in ["1.1", "v1.1.3", "1.1.3.0", "abc", ""]:
        with pytest.raises(ValueError):
            build_plan(bad)


def test_rejects_non_numeric_internal_patch():
    with pytest.raises(ValueError):
        build_plan("1.1.3", internal_patch="x")


@pytest.mark.parametrize("patch", ["00", "01", "-1", "²"])
def test_rejects_non_canonical_internal_patch(patch):
    with pytest.raises(ValueError):
        build_plan("1.1.3", internal_patch=patch)


def test_rejects_unknown_target():
    with pytest.raises(ValueError):
        build_plan("1.1.3", target_keys=["spark9.9"])


def test_rejects_duplicate_targets():
    with pytest.raises(ValueError):
        build_plan("1.1.3", target_keys=["master", "master"])


@pytest.mark.parametrize(
    "kwargs",
    [
        {"upack_iteration": {"spark9.9": 1}},
        {"upack_iteration": {"spark4.0": 0}},
        {"upack_iteration": {"spark4.0": -1}},
        {"internal_upack_iteration": {"spark4.0": True}},
    ],
)
def test_rejects_invalid_rebuild_iterations(kwargs):
    with pytest.raises(ValueError):
        build_plan("1.1.3", **kwargs)


def test_rejects_iteration_for_unselected_target():
    with pytest.raises(ValueError):
        build_plan(
            "1.1.3",
            target_keys=["master"],
            upack_iteration={"spark4.0": 1},
        )


def test_parse_iterations_normalizes_key_and_value_whitespace():
    assert parse_iterations(
        " spark4.0 = 1, master= 2 ",
        "--upack-iteration",
    ) == {"spark4.0": 1, "master": 2}


@pytest.mark.parametrize("raw", ["=1", " = 1"])
def test_parse_iterations_rejects_empty_target(raw):
    with pytest.raises(ValueError, match="non-empty target"):
        parse_iterations(raw, "--upack-iteration")


def test_master_carries_three_oss_tags():
    """Verified live: v1.1.3, v1.1.3-spark3.5 and v1.1.3-python3.11 all point
    at commit a833941704b5. A release that creates only two of them is broken."""
    m = _by_key(build_plan("1.1.3"))["master"]
    assert m.oss_tags == ["v1.1.3", "v1.1.3-spark3.5", "v1.1.3-python3.11"]
    assert m.internal_tags == ["v1.1.3.0", "v1.1.3.0-spark3.5", "v1.1.3.0-python3.11"]


def test_non_anchor_targets_have_no_bare_tag():
    t = _by_key(build_plan("1.1.3"))["spark4.0"]
    assert t.oss_tags == ["v1.1.3-spark4.0", "v1.1.3-python3.12"]
    assert "v1.1.3" not in t.oss_tags


def test_upack_dot_dash_asymmetry_is_preserved():
    """The single most error-prone fact in the whole release:
    OSS UPack mangles the dot, internal UPack does not."""
    t = _by_key(build_plan("1.1.3"))["spark4.0"]
    assert t.oss_upack_version == "1.1.3-spark4-0"
    assert t.internal_upack_version == "1.1.3-0-spark4.0"

    t41 = _by_key(build_plan("1.1.3"))["spark4.1"]
    assert t41.oss_upack_version == "1.1.3-spark4-1"
    assert t41.internal_upack_version == "1.1.3-0-spark4.1"


def test_master_upack_has_no_spark_suffix():
    m = _by_key(build_plan("1.1.3"))["master"]
    assert m.oss_upack_version == "1.1.3"
    assert m.internal_upack_version == "1.1.3-0"


def test_pip_uses_pep440_local_segment():
    m = _by_key(build_plan("1.1.3"))
    assert m["master"].oss_pip_version == "1.1.3+python3.11"
    assert m["spark4.0"].oss_pip_version == "1.1.3+python3.12"
    assert m["spark4.1"].internal_pip_version == "1.1.3.0+python3.13"


def test_maven_coordinates_follow_release_tags():
    m = _by_key(build_plan("1.1.3"))
    assert m["master"].scala == "2.12"
    assert m["master"].oss_maven_version == "1.1.3"
    assert m["spark4.0"].scala == "2.13"
    assert m["spark4.0"].oss_maven_version == "1.1.3-spark4.0"
    assert m["spark4.1"].oss_maven_version == "1.1.3-spark4.1"


def test_internal_superpatch_flows_everywhere():
    """v1.1.3.1 was a real internal-only hotfix: UPack 1.1.3-1, pip 1.1.3.1+python3.11."""
    m = _by_key(build_plan("1.1.3", internal_patch="1"))["master"]
    assert m.internal_tags[0] == "v1.1.3.1"
    assert m.internal_upack_version == "1.1.3-1"
    assert m.internal_pip_version == "1.1.3.1+python3.11"
    assert (
        m.oss_upack_version == "1.1.3"
    ), "OSS artifacts must not move on an internal-only hotfix"


def test_upack_rebuild_iteration_suffix():
    """1.1.1-spark4-0-1 exists in the live feed: a republish after a bad build."""
    m = _by_key(build_plan("1.1.1", upack_iteration={"spark4.0": 1}))["spark4.0"]
    assert m.oss_upack_version == "1.1.1-spark4-0-1"
    assert m.internal_upack_version == "1.1.1-0-spark4.0", (
        "OSS and Internal are separate packages with independent rebuild "
        "counters; an OSS republish must not renumber the Internal package"
    )


def test_internal_rebuild_iteration_is_independent():
    m = _by_key(build_plan("1.1.1", internal_upack_iteration={"spark4.0": 2}))[
        "spark4.0"
    ]
    assert m.oss_upack_version == "1.1.1-spark4-0"
    assert m.internal_upack_version == "1.1.1-0-spark4.0-2"


def test_reproduces_production_bbcvhd_spark40_setup_sh():
    """Byte-for-byte round-trip against the live BBC-VHD dev/spark40 file:

        SYNAPSEML_VERSION=1.1.1-spark4-0-1
        SYNAPSEML_INTERNAL_VERSION=1.1.1-0-spark4.0

    Note the asymmetry that makes hand-editing this file so error-prone: the
    OSS package mangles the spark dot to a dash and carries a rebuild counter,
    while the Internal package preserves the dot and carries none.
    """
    m = _by_key(
        build_plan("1.1.1", internal_patch="0", upack_iteration={"spark4.0": 1})
    )["spark4.0"]
    assert m.oss_upack_version == "1.1.1-spark4-0-1"
    assert m.internal_upack_version == "1.1.1-0-spark4.0"


def test_target_subset_is_respected():
    plan = build_plan("1.1.4", target_keys=["master", "spark4.0"])
    assert [tp.key for tp in plan.targets] == ["master", "spark4.0"]


def test_publish_parameters_enable_exact_selected_targets():
    plan = build_plan("1.1.4", target_keys=["master", "spark4.1"])
    assert plan.publish_pipeline_id == 35879
    assert plan.publish_parameters["synapseml_version"] == "1.1.4"
    assert plan.publish_parameters["internal_patch_version"] == "0"
    assert plan.publish_parameters["build_synapseml_pip_py311"] is True
    assert plan.publish_parameters["build_synapseml_upack_default"] is True
    assert plan.publish_parameters["build_synapseml_pip_py312"] is False
    assert plan.publish_parameters["build_synapseml_upack_spark4"] is False
    assert plan.publish_parameters["build_synapseml_pip_py313"] is True
    assert plan.publish_parameters["build_synapseml_upack_spark41"] is True
    assert plan.publish_parameters["build_internal_pip_py313"] is True
    assert plan.publish_parameters["build_internal_upack_spark41"] is True


def test_base_branch_chain_matches_rebase_order():
    b = {tp.key: tp.base_branch for tp in build_plan("1.1.4").targets}
    assert b == {"master": None, "spark4.0": "master", "spark4.1": "spark4.0"}


def test_all_tag_helpers_are_unique_and_complete():
    plan = build_plan("1.1.4")
    assert len(plan.all_oss_tags) == len(set(plan.all_oss_tags)) == 7
    assert len(plan.all_internal_tags) == len(set(plan.all_internal_tags)) == 7


@pytest.mark.parametrize(
    "args",
    [
        ["--version", "1.1.4", "--upack-iteration", "spark4.0=0"],
        ["--version", "1.1.4", "--upack-iteration", "spark4.0=²"],
        [
            "--version",
            "1.1.4",
            "--upack-iteration",
            "spark4.0=1,spark4.0=2",
        ],
    ],
)
def test_cli_rejects_invalid_iterations(args):
    from release_matrix import main

    assert main(args) == 2
