# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import json
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bump_bbcvhd as bump  # noqa: E402
import release_matrix as matrix  # noqa: E402
import release_ops as ops  # noqa: E402
import verify_release as verify  # noqa: E402
from test_release_ops import FakeRemote  # noqa: E402


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("BBC-VHD tests must use the local fake transport")

    monkeypatch.setattr(urllib.request, "urlopen", forbidden)
    monkeypatch.setattr(urllib.request.OpenerDirector, "open", forbidden)
    monkeypatch.setattr(ops, "AzureRemote", forbidden)


def bound_plan(version="1.1.3", **overrides):
    keys = overrides.get("target_keys", ["spark4.0"])
    options = {
        "target_keys": keys,
        "families": ["upack"],
        "oss_commits": {key: "a" * 40 for key in keys},
        "internal_commits": {key: "b" * 40 for key in keys},
    }
    options.update(overrides)
    return matrix.build_plan(version, **options)


def publish_plan(tmp_path, plan, remote=None):
    tmp_path.mkdir(parents=True, exist_ok=True)
    plan_path, proof_path = tmp_path / "plan.json", tmp_path / "evidence.json"
    state_path = tmp_path / "state.json"
    plan_path.write_text(json.dumps(matrix.plan_to_dict(plan)), encoding="utf-8")
    with pytest.MonkeyPatch.context() as patches:
        remote = remote if remote is not None else FakeRemote(patches)
        remote.missing.update(
            (
                repository,
                family,
                getattr(target, f"{repository}_{family}_version"),
            )
            for target in plan.targets
            for repository in plan.repositories
            for family in plan.families
        )
        remote.oss_commit = plan.targets[0].oss_commit
        queued_before = len(remote.queued)
        command = [
            "resume",
            "--plan",
            str(plan_path),
            "--state",
            str(state_path),
            "--apply",
            "--approve-plan",
            plan.plan_id,
        ]
        for _ in range(len(ops.build_actions(plan)) + 1):
            result = ops.main(command, remote=remote)
            assert result in (0, 1)
            state = json.loads(state_path.read_text(encoding="utf-8"))
            if result == 0:
                break
            pending = {
                action["build_id"]: action
                for action in state["actions"]
                if action["build_id"] is not None
                and remote.builds[action["build_id"]]["result"] != "succeeded"
            }
            assert pending, "The real driver could not advance the fake publication"
            for build_id, action in pending.items():
                remote.succeed(
                    build_id,
                    plan,
                    action["repository"],
                    action["operation"]["families"],
                    target=action["target"],
                )
        else:
            pytest.fail("The real driver did not complete the fake publication")
        report = ops.verified_evidence(plan, state_path, remote=remote)
        verify.validate_evidence(plan, report)
        assert all(action["status"] == "complete" for action in state["actions"])
        assert len(remote.queued) - queued_before == len(
            {action["build_id"] for action in state["actions"]}
        )
    proof_path.write_text(json.dumps(report), encoding="utf-8")
    return plan_path, proof_path


def approved_args(tmp_path, version="1.1.3", **overrides):
    plan = bound_plan(version, **overrides)
    plan_path, proof_path = publish_plan(tmp_path, plan)
    return [
        "--repo",
        str(tmp_path),
        "--target",
        "spark4.0",
        "--plan",
        str(plan_path),
        "--evidence",
        str(proof_path),
        "--apply",
        "--approve-plan",
        plan.plan_id,
    ]


def paired_args(tmp_path, *, internal=None, oss=None, target="spark4.0"):
    oss_plan = bound_plan(
        **{"repositories": ["oss"], "internal_commits": {}, **(oss or {})}
    )
    internal_plan = bound_plan(**{"repositories": ["internal"], **(internal or {})})
    with pytest.MonkeyPatch.context() as patches:
        remote = FakeRemote(patches)
        oss_path, oss_proof = publish_plan(tmp_path / "oss", oss_plan, remote)
        internal_path, internal_proof = publish_plan(
            tmp_path / "internal", internal_plan, remote
        )
    return [
        "--repo",
        str(tmp_path),
        "--target",
        target,
        "--plan",
        str(internal_path),
        "--evidence",
        str(internal_proof),
        "--approve-plan",
        internal_plan.plan_id,
        "--oss-plan",
        str(oss_path),
        "--oss-evidence",
        str(oss_proof),
        "--approve-oss-plan",
        oss_plan.plan_id,
        "--apply",
    ]


def remove_option(args, option):
    index = args.index(option)
    del args[index : index + 2]


def component_bytes(component):
    return {path.name: path.read_bytes() for path in component.iterdir()}


def make_component(
    tmp_path: Path,
    *,
    newline="\n",
    oss="1.1.1-spark4-0-1",
    internal="1.1.1-0-spark4.0",
    revision="1.4.26",
    target="spark4.0",
):
    component = tmp_path / "Components" / "MMLSpark" / bump.COMPONENT_DIR[target]
    component.mkdir(parents=True)
    setup = (
        "#!/bin/bash"
        + newline
        + f"{bump.OSS_VAR}={oss}"
        + newline
        + f"{bump.INTERNAL_VAR}={internal}"
        + newline
        + "echo ready"
        + newline
    )
    bump.write_text_exact(component / "setup.sh", setup)
    bump.write_text_exact(component / "version.txt", revision + newline)
    return component


@pytest.mark.parametrize("newline", ["", "\n", "\r\n"])
def test_bump_component_revision_preserves_trailing_newline(newline):
    updated, old, new = bump.bump_component_revision("1.4.26" + newline)
    assert (old, new) == ("1.4.26", "1.4.27")
    assert updated == "1.4.27" + newline


@pytest.mark.parametrize("invalid", [" 1.4.26\n", "1.4.26 \n", "1.4\n", "\n1.4.26\n"])
def test_bump_component_revision_rejects_non_bare_content(invalid):
    with pytest.raises(ValueError):
        bump.bump_component_revision(invalid)


def test_set_shell_var_preserves_crlf():
    text = "A=1\r\nSYNAPSEML_VERSION=old\r\nB=2\r\n"
    updated, old = bump.set_shell_var(text, bump.OSS_VAR, "new")
    assert old == "old"
    assert updated == "A=1\r\nSYNAPSEML_VERSION=new\r\nB=2\r\n"


def test_set_shell_var_rejects_duplicate_assignments():
    text = "SYNAPSEML_VERSION=one\nSYNAPSEML_VERSION=two\n"
    with pytest.raises(ValueError):
        bump.set_shell_var(text, bump.OSS_VAR, "new")


def test_main_updates_component_and_preserves_crlf(tmp_path):
    component = make_component(tmp_path, newline="\r\n")

    result = bump.main(approved_args(tmp_path))

    assert result == 0
    setup = bump.read_text_exact(component / "setup.sh")
    assert f"{bump.OSS_VAR}=1.1.3-spark4-0\r\n" in setup
    assert f"{bump.INTERNAL_VAR}=1.1.3-0-spark4.0\r\n" in setup
    assert "\n" not in setup.replace("\r\n", "")
    assert bump.read_text_exact(component / "version.txt") == "1.4.27\r\n"


@pytest.mark.parametrize("corruption", ["inventory", "receipt", "stale"])
def test_bbc_write_rejects_incomplete_producer_evidence(tmp_path, corruption):
    component = make_component(tmp_path)
    before = {path.name: path.read_bytes() for path in component.iterdir()}
    args = approved_args(tmp_path)
    proof_path = tmp_path / "evidence.json"
    proof = json.loads(proof_path.read_text(encoding="utf-8"))
    if corruption == "inventory":
        proof["evidence_kind"] = "inventory"
        del proof["producer_evidence"]
    elif corruption == "receipt":
        proof["producer_evidence"]["runs"][0]["provenance"] = []
    else:
        proof["checked_at"] = (
            datetime.now(timezone.utc) - timedelta(hours=2)
        ).isoformat()
    proof_path.write_text(json.dumps(proof), encoding="utf-8")
    assert bump.main(args) == 2
    assert {path.name: path.read_bytes() for path in component.iterdir()} == before


def test_dry_run_does_not_write(tmp_path):
    component = make_component(tmp_path)
    before_setup = bump.read_text_exact(component / "setup.sh")
    before_revision = bump.read_text_exact(component / "version.txt")

    result = bump.main(
        [
            "--repo",
            str(tmp_path),
            "--version",
            "1.1.3",
            "--target",
            "spark4.0",
            "--dry-run",
        ]
    )

    assert result == 0
    assert bump.read_text_exact(component / "setup.sh") == before_setup
    assert bump.read_text_exact(component / "version.txt") == before_revision


def test_repeated_release_does_not_bump_component_revision(tmp_path, capsys):
    component = make_component(
        tmp_path,
        oss="1.1.3-spark4-0",
        internal="1.1.3-0-spark4.0",
    )

    result = bump.main(approved_args(tmp_path))

    assert result == 2
    assert bump.read_text_exact(component / "version.txt") == "1.4.26\n"
    assert "already references" in capsys.readouterr().err


def test_force_revision_allows_intentional_image_rebuild(tmp_path):
    component = make_component(
        tmp_path,
        oss="1.1.3-spark4-0",
        internal="1.1.3-0-spark4.0",
    )

    result = bump.main(approved_args(tmp_path) + ["--force-revision"])

    assert result == 0
    assert bump.read_text_exact(component / "version.txt") == "1.4.27\n"


def test_write_failure_rolls_back_both_files(tmp_path, monkeypatch, capsys):
    component = make_component(tmp_path)
    setup_path = component / "setup.sh"
    version_path = component / "version.txt"
    original_setup = bump.read_text_exact(setup_path)
    original_version = bump.read_text_exact(version_path)
    real_write = bump.write_text_exact
    calls = []

    def fail_second_write(path, text):
        calls.append(path)
        if len(calls) == 2:
            raise OSError("simulated version write failure")
        real_write(path, text)

    monkeypatch.setattr(bump, "write_text_exact", fail_second_write)
    result = bump.main(approved_args(tmp_path))

    assert result == 1
    assert bump.read_text_exact(setup_path) == original_setup
    assert bump.read_text_exact(version_path) == original_version
    assert "was rolled back" in capsys.readouterr().err


def test_legacy_write_cannot_bypass_plan_evidence(tmp_path, capsys):
    component = make_component(tmp_path)
    before = bump.read_text_exact(component / "setup.sh")
    result = bump.main(
        [
            "--repo",
            str(tmp_path),
            "--version",
            "1.1.3",
            "--target",
            "spark4.0",
        ]
    )
    assert result == 2
    assert "plan" in capsys.readouterr().err
    assert bump.read_text_exact(component / "setup.sh") == before


def test_internal_only_preview_never_drops_existing_oss_counter(tmp_path, capsys):
    make_component(tmp_path)
    result = bump.main(
        [
            "--repo",
            str(tmp_path),
            "--version",
            "1.1.1",
            "--internal-patch",
            "1",
            "--target",
            "spark4.0",
            "--dry-run",
        ]
    )
    assert result == 0
    output = capsys.readouterr().out
    assert "1.1.1-spark4-0-1  ->  1.1.1-spark4-0-1" in output


def test_internal_only_bound_update_preserves_oss(tmp_path):
    component = make_component(tmp_path)
    result = bump.main(
        approved_args(
            tmp_path,
            "1.1.1",
            internal_patch="1",
            scope="internal-only",
            upack_iteration={"spark4.0": 1},
        )
    )
    assert result == 0
    text = bump.read_text_exact(component / "setup.sh")
    assert "SYNAPSEML_VERSION=1.1.1-spark4-0-1\n" in text
    assert "SYNAPSEML_INTERNAL_VERSION=1.1.1-1-spark4.0\n" in text


def test_internal_only_wrong_oss_plan_is_rejected_before_writes(tmp_path):
    component = make_component(tmp_path)
    before = bump.read_text_exact(component / "setup.sh")
    result = bump.main(
        approved_args(
            tmp_path,
            "1.1.1",
            internal_patch="1",
            scope="internal-only",
        )
    )
    assert result == 2
    assert bump.read_text_exact(component / "setup.sh") == before
    assert bump.read_text_exact(component / "version.txt") == "1.4.26\n"


def test_missing_or_partial_evidence_cannot_approve_update(tmp_path):
    component = make_component(tmp_path)
    args = approved_args(tmp_path)
    proof_path = tmp_path / "evidence.json"
    proof = json.loads(proof_path.read_text(encoding="utf-8"))
    proof["rows"].pop()
    proof_path.write_text(json.dumps(proof), encoding="utf-8")
    assert bump.main(args) == 2
    assert bump.read_text_exact(component / "version.txt") == "1.4.26\n"


@pytest.mark.parametrize("target", ["master", "spark4.0", "spark4.1"])
def test_paired_staged_rollout_updates_both_pins_once(tmp_path, monkeypatch, target):
    prior = bound_plan("1.1.1", target_keys=[target]).targets[0]
    component = make_component(
        tmp_path,
        target=target,
        oss=prior.oss_upack_version,
        internal=prior.internal_upack_version,
        newline="\r\n",
    )
    args = paired_args(
        tmp_path,
        internal={"target_keys": [target]},
        oss={"target_keys": [target]},
        target=target,
    )
    originals = {
        path: path.read_bytes()
        for directory in ("oss", "internal")
        for path in (tmp_path / directory).iterdir()
    }
    plans = [
        matrix.read_plan(tmp_path / directory / "plan.json", require_bound=True)
        for directory in ("oss", "internal")
    ]
    assert plans[0].plan_id != plans[1].plan_id
    producer_builds = []
    for plan, directory in zip(plans, ("oss", "internal")):
        assert plan.scope == "full" and plan.internal_patch == "0"
        report = json.loads(
            (tmp_path / directory / "evidence.json").read_text(encoding="utf-8")
        )
        verify.validate_evidence(plan, report)
        producer_builds.append(
            {run["build"]["id"] for run in report["producer_evidence"]["runs"]}
        )
        assert report["producer_evidence"]["plan_id"] == plan.plan_id
        assert all(
            receipt["plan_id"] == plan.plan_id
            for run in report["producer_evidence"]["runs"]
            for receipt in run["provenance"]
        )
    assert producer_builds[0].isdisjoint(producer_builds[1])
    writes = []
    real_write = bump.write_text_exact

    def record_write(path, text):
        writes.append(path)
        real_write(path, text)

    monkeypatch.setattr(bump, "write_text_exact", record_write)
    assert bump.main(args) == 0
    assert writes == [component / "setup.sh", component / "version.txt"]
    setup = bump.read_text_exact(component / "setup.sh")
    assert f"{bump.OSS_VAR}={plans[0].targets[0].oss_upack_version}\r\n" in setup
    assert (
        f"{bump.INTERNAL_VAR}={plans[1].targets[0].internal_upack_version}\r\n" in setup
    )
    assert bump.read_text_exact(component / "version.txt") == "1.4.27\r\n"
    assert {path: path.read_bytes() for path in originals} == originals
    after = component_bytes(component)
    assert bump.main(args) == 2
    assert component_bytes(component) == after
    assert writes == [component / "setup.sh", component / "version.txt"]


def test_separate_plans_cannot_move_the_existing_base_one_at_a_time(tmp_path):
    component = make_component(tmp_path)
    before = component_bytes(component)
    args = paired_args(tmp_path)
    internal_args = args.copy()
    for option in ("--oss-plan", "--oss-evidence", "--approve-oss-plan"):
        remove_option(internal_args, option)
    assert bump.main(internal_args) == 2
    assert component_bytes(component) == before
    oss_args = internal_args.copy()
    for option, companion_option in (
        ("--plan", "--oss-plan"),
        ("--evidence", "--oss-evidence"),
        ("--approve-plan", "--approve-oss-plan"),
    ):
        oss_args[oss_args.index(option) + 1] = args[args.index(companion_option) + 1]
    assert bump.main(oss_args) == 2
    assert component_bytes(component) == before
    assert bump.main(args) == 0
    assert bump.read_text_exact(component / "version.txt") == "1.4.27\n"


def test_paired_rollout_keeps_independent_upack_counters(tmp_path):
    component = make_component(tmp_path)
    args = paired_args(
        tmp_path,
        internal={
            "upack_iteration": {"spark4.0": 2},
            "internal_upack_iteration": {"spark4.0": 3},
        },
        oss={"upack_iteration": {"spark4.0": 2}},
    )
    assert bump.main(args) == 0
    setup = bump.read_text_exact(component / "setup.sh")
    assert f"{bump.OSS_VAR}=1.1.3-spark4-0-2\n" in setup
    assert f"{bump.INTERNAL_VAR}=1.1.3-0-spark4.0-3\n" in setup
    assert bump.read_text_exact(component / "version.txt") == "1.4.27\n"


@pytest.mark.parametrize("explicit_dry_run", [False, True])
def test_paired_preview_needs_neither_evidence_nor_approval(tmp_path, explicit_dry_run):
    component = make_component(tmp_path)
    before = component_bytes(component)
    args = ["--repo", str(tmp_path), "--target", "spark4.0"]
    for repository, flag in (("internal", "--plan"), ("oss", "--oss-plan")):
        plan = bound_plan(repositories=[repository])
        path = tmp_path / (repository + ".json")
        path.write_text(json.dumps(matrix.plan_to_dict(plan)), encoding="utf-8")
        args.extend([flag, str(path)])
    if explicit_dry_run:
        args.append("--dry-run")
    assert bump.main(args) == 0
    assert component_bytes(component) == before
    assert not list(tmp_path.glob("**/state.json"))


@pytest.mark.parametrize("option", ["--approve-plan", "--approve-oss-plan"])
@pytest.mark.parametrize("approval", ["absent", "wrong", "other-plan"])
def test_paired_apply_requires_both_exact_approvals(tmp_path, option, approval, capsys):
    component = make_component(tmp_path)
    before = component_bytes(component)
    args = paired_args(tmp_path)
    if approval == "absent":
        remove_option(args, option)
    elif approval == "wrong":
        args[args.index(option) + 1] = "f" * 64
    else:
        other = "--approve-oss-plan" if option == "--approve-plan" else "--approve-plan"
        args[args.index(option) + 1] = args[args.index(other) + 1]
    assert bump.main(args) == 2
    assert option in capsys.readouterr().err
    assert component_bytes(component) == before


@pytest.mark.parametrize("repository", ["internal", "oss"])
@pytest.mark.parametrize(
    "corruption",
    [
        "absent",
        "missing-file",
        "inventory",
        "receipt",
        "source",
        "stale",
        "destination",
    ],
)
def test_paired_apply_validates_each_original_evidence(
    tmp_path, repository, corruption
):
    component = make_component(tmp_path)
    before = component_bytes(component)
    args = paired_args(tmp_path)
    option = "--evidence" if repository == "internal" else "--oss-evidence"
    proof_path = Path(args[args.index(option) + 1])
    if corruption == "absent":
        remove_option(args, option)
    elif corruption == "missing-file":
        proof_path.unlink()
    else:
        proof = json.loads(proof_path.read_text(encoding="utf-8"))
        producer = proof["producer_evidence"]
        if corruption == "inventory":
            proof["evidence_kind"] = "inventory"
            del proof["producer_evidence"]
        elif corruption == "receipt":
            producer["runs"][0]["provenance"] = []
        elif corruption == "source":
            producer["runs"][0]["provenance"][0]["source_commit"] = "e" * 40
        elif corruption == "stale":
            timestamp = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
            proof["checked_at"] = producer["checked_at"] = timestamp
        else:
            producer["destinations"]["upack"]["id"] = "f" * 36
        proof_path.write_text(json.dumps(proof), encoding="utf-8")
    assert bump.main(args) == 2
    assert component_bytes(component) == before


@pytest.mark.parametrize("repository", ["internal", "oss"])
@pytest.mark.parametrize("replacement", ["evidence-file", "producer-runs"])
def test_paired_apply_cannot_adopt_another_plans_receipts(
    tmp_path, repository, replacement
):
    component = make_component(tmp_path)
    before = component_bytes(component)
    args = paired_args(tmp_path)
    other = "oss" if repository == "internal" else "internal"
    other_path = tmp_path / other / "evidence.json"
    if replacement == "evidence-file":
        flag = "--evidence" if repository == "internal" else "--oss-evidence"
        args[args.index(flag) + 1] = str(other_path)
    else:
        path = tmp_path / repository / "evidence.json"
        proof = json.loads(path.read_text(encoding="utf-8"))
        other_proof = json.loads(other_path.read_text(encoding="utf-8"))
        proof["producer_evidence"]["runs"] = other_proof["producer_evidence"]["runs"]
        path.write_text(json.dumps(proof), encoding="utf-8")
    assert bump.main(args) == 2
    assert component_bytes(component) == before


@pytest.mark.parametrize(
    "override",
    [
        {"version": "1.1.4"},
        {"oss_commits": {"spark4.0": "e" * 40}},
        {"upack_iteration": {"spark4.0": 2}},
    ],
    ids=["base", "source-commit", "oss-counter"],
)
def test_paired_apply_rejects_incompatible_approved_plans(tmp_path, override, capsys):
    component = make_component(tmp_path)
    before = component_bytes(component)
    args = paired_args(tmp_path, oss=override)
    assert bump.main(args) == 2
    assert "match" in capsys.readouterr().err
    assert component_bytes(component) == before


@pytest.mark.parametrize("repository", ["internal", "oss"])
@pytest.mark.parametrize(
    "override",
    [
        {"repositories": ["oss", "internal"]},
        {"families": ["pip"]},
        {
            "mode": "rehearsal",
            "pip_feed": "release-test-pip",
            "upack_feed": "release-test-upack",
        },
        {"target_keys": ["master"]},
    ],
    ids=["combined-role", "no-upack", "rehearsal", "missing-target"],
)
def test_paired_apply_rejects_wrong_plan_role_or_selection(
    tmp_path, repository, override
):
    component = make_component(tmp_path)
    before = component_bytes(component)
    if "repositories" in override:
        override = {**override, "internal_commits": {"spark4.0": "b" * 40}}
    args = paired_args(tmp_path, **{repository: override})
    assert bump.main(args) == 2
    assert component_bytes(component) == before


@pytest.mark.parametrize(
    "internal,oss",
    [
        ({"repositories": ["oss"]}, None),
        (
            None,
            {"repositories": ["internal"], "internal_commits": {"spark4.0": "b" * 40}},
        ),
    ],
)
def test_paired_apply_rejects_swapped_repository_roles(tmp_path, internal, oss):
    component = make_component(tmp_path)
    before = component_bytes(component)
    assert bump.main(paired_args(tmp_path, internal=internal, oss=oss)) == 2
    assert component_bytes(component) == before


@pytest.mark.parametrize("old_oss", ["1.1.1-spark4-0-1", "1.1.3-spark4-0"])
def test_internal_only_hotfix_cannot_use_oss_companion(tmp_path, old_oss, capsys):
    component = make_component(tmp_path, oss=old_oss)
    before = component_bytes(component)
    args = paired_args(
        tmp_path, internal={"scope": "internal-only", "internal_patch": "1"}
    )
    assert bump.main(args) == 2
    assert "full" in capsys.readouterr().err
    assert component_bytes(component) == before


def test_internal_only_hotfix_cannot_be_the_oss_companion(tmp_path, capsys):
    component = make_component(tmp_path)
    before = component_bytes(component)
    args = paired_args(
        tmp_path,
        oss={
            "scope": "internal-only",
            "internal_patch": "1",
            "repositories": ["internal"],
            "internal_commits": {"spark4.0": "b" * 40},
        },
    )
    assert bump.main(args) == 2
    assert "scope=full" in capsys.readouterr().err
    assert component_bytes(component) == before


@pytest.mark.parametrize("repository", ["internal", "oss"])
@pytest.mark.parametrize(
    "field", ["spark", "scala", "python", "oss_commit", "upack_feed"]
)
def test_paired_preview_rejects_unbound_or_inconsistent_plan_fields(
    tmp_path, repository, field
):
    component = make_component(tmp_path)
    before = component_bytes(component)
    args = ["--repo", str(tmp_path), "--target", "spark4.0"]
    for role, flag in (("internal", "--plan"), ("oss", "--oss-plan")):
        plan = bound_plan(repositories=[role])
        document = matrix.plan_to_dict(plan)
        if role == repository:
            if field == "upack_feed":
                document[field] = "release-test-upack"
            else:
                document["targets"][0][field] = None if field == "oss_commit" else "9.9"
            document["plan_id"] = matrix.plan_digest(document)
        path = tmp_path / (role + ".json")
        path.write_text(json.dumps(document), encoding="utf-8")
        args.extend([flag, str(path)])
    assert bump.main(args) == 2
    assert component_bytes(component) == before


@pytest.mark.parametrize("orphan", ["--oss-evidence", "--approve-oss-plan"])
def test_orphan_companion_options_are_rejected(tmp_path, orphan, capsys):
    component = make_component(tmp_path)
    before = component_bytes(component)
    args = approved_args(tmp_path)
    assert bump.main(args + [orphan, "unused"]) == 2
    assert "--oss-plan" in capsys.readouterr().err
    assert component_bytes(component) == before


def test_legacy_version_cannot_be_paired(tmp_path, capsys):
    component = make_component(tmp_path)
    before = component_bytes(component)
    args = paired_args(tmp_path)
    for option in ("--plan", "--evidence", "--approve-plan"):
        remove_option(args, option)
    args.remove("--apply")
    args.extend(["--version", "1.1.3", "--dry-run"])
    assert bump.main(args) == 2
    assert "--plan" in capsys.readouterr().err
    assert component_bytes(component) == before


def test_paired_rollout_accepts_complete_broader_original_plans(tmp_path):
    component = make_component(tmp_path)
    args = paired_args(
        tmp_path,
        internal={
            "target_keys": ["spark4.0", "spark4.1"],
            "families": ["pip", "upack"],
        },
        oss={"target_keys": ["master", "spark4.0"], "families": ["maven", "upack"]},
    )
    assert bump.main(args) == 0
    assert bump.read_text_exact(component / "version.txt") == "1.4.27\n"
    assert not (component.parent / "spark35").exists()
    assert not (component.parent / "spark41").exists()


@pytest.mark.parametrize("repository", ["internal", "oss"])
@pytest.mark.parametrize("omission", ["other-target", "other-family"])
def test_paired_rollout_requires_evidence_for_the_entire_original_plan(
    tmp_path, repository, omission
):
    component = make_component(tmp_path)
    before = component_bytes(component)
    options = {
        "target_keys": ["spark4.0", "spark4.1"],
        "families": ["pip", "upack"],
    }
    args = paired_args(tmp_path, **{repository: options})
    path = tmp_path / repository / "evidence.json"
    proof = json.loads(path.read_text(encoding="utf-8"))
    for run in proof["producer_evidence"]["runs"]:
        run["provenance"] = [
            receipt
            for receipt in run["provenance"]
            if (
                receipt["target"] != "spark4.1"
                if omission == "other-target"
                else receipt["family"] != "pip"
            )
        ]
    path.write_text(json.dumps(proof), encoding="utf-8")
    assert bump.main(args) == 2
    assert component_bytes(component) == before


@pytest.mark.parametrize("failure", ["write", "postcondition"])
def test_paired_rollout_uses_existing_atomic_rollback(tmp_path, monkeypatch, failure):
    component = make_component(tmp_path, newline="\r\n")
    before = component_bytes(component)
    args = paired_args(tmp_path)
    real_write = bump.write_text_exact
    calls = []

    def fail_revision_once(path, text):
        calls.append(path)
        if len(calls) == 2:
            if failure == "write":
                raise OSError("simulated component revision write failure")
            text = "0.0.0\r\n"
        real_write(path, text)

    monkeypatch.setattr(bump, "write_text_exact", fail_revision_once)
    assert bump.main(args) == 1
    assert component_bytes(component) == before
    assert calls == [
        component / "setup.sh",
        component / "version.txt",
        component / "setup.sh",
        component / "version.txt",
    ]
