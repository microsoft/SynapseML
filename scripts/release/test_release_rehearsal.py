# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

"""Offline agent checkpoints using real release code and simulated services."""

import os
import socket
import subprocess
from pathlib import Path

import pytest

from test_release_bootstrap import candidates  # noqa: F401
from test_release_ops import (  # noqa: F401
    OSS_SHA,
    cli,
    matrix,
    ops,
    produced_maven_receipt,
    saved,
)


@pytest.fixture(autouse=True)
def offline_only(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("Offline rehearsal attempted a live service call")

    popen = subprocess.Popen

    def local_git_only(command, *args, **kwargs):
        if (
            not isinstance(command, (list, tuple))
            or Path(command[0]).name.lower() not in {"git", "git.exe"}
            or kwargs.get("shell")
        ):
            forbidden()
        return popen(command, *args, **kwargs)

    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(subprocess, "Popen", local_git_only)
    monkeypatch.setattr(ops, "AzureRemote", forbidden)
    monkeypatch.setenv("GIT_ALLOW_PROTOCOL", "file")
    monkeypatch.setenv("GIT_TERMINAL_PROMPT", "0")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")


def public_plan(include_spark40):
    keys = [target.key for target in matrix.TARGETS] if include_spark40 else None
    return matrix.build_plan(
        "1.2.0",
        target_keys=keys,
        oss_commits={key: OSS_SHA for key in keys or matrix.DEFAULT_TARGET_KEYS},
    )


def bare_git(repo, *args):
    return subprocess.run(
        ["git", f"--git-dir={repo}", *args],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


@pytest.mark.parametrize("include_spark40", [False, True])
def test_agent_read_only_checkpoints_never_publish(cli, include_spark40):
    plan = public_plan(include_spark40)
    cli.remote.missing = {("oss", "maven")}
    for command, expected in (("preflight", 0), ("resume", 1), ("status", 1)):
        code, report, error = cli(command, plan=plan)
        assert code == expected, error
        assert report["plan_id"] == plan.plan_id
        assert report["complete"] is False
        assert not cli.remote.queued
        assert all(item["build_id"] is None for item in saved(cli)["actions"])
    assert len(saved(cli)["actions"]) == len(plan.targets)


@pytest.mark.parametrize(
    "flags",
    [
        ["--apply"],
        ["--approve-plan", "a" * 64],
        ["--apply", "--approve-plan", "a" * 64],
    ],
)
def test_agent_cannot_treat_a_plan_as_approval(cli, flags):
    code, report, error = cli(plan=public_plan(False), extra=flags)
    assert code == 2 and report is None
    assert "Approval requires" in error
    assert not cli.remote.inventory_calls
    assert not cli.remote.queued
    assert not cli.state.exists()


@pytest.mark.parametrize("include_spark40", [False, True])
def test_agent_simulated_publication_resumes_and_verifies_receipts(
    cli, tmp_path, include_spark40
):
    plan = public_plan(include_spark40)
    cli.remote.missing = {("oss", "maven")}
    assert cli("preflight", plan=plan)[0] == 0
    assert cli(plan=plan)[0] == 1
    assert not cli.remote.queued
    assert cli(plan=plan, apply=True)[0] == 1
    original = {item["target"]: item["build_id"] for item in saved(cli)["actions"]}
    assert len(cli.remote.queued) == len(plan.targets)
    for command in ("status", "resume"):
        code, report, error = cli(command, plan=plan)
        assert code == 1, error
        assert not report["complete"]
        assert {
            item["target"]: item["build_id"] for item in saved(cli)["actions"]
        } == original
        assert len(cli.remote.queued) == len(plan.targets)
    for key, build_id in original.items():
        cli.remote.succeed(build_id, plan, "oss", ["maven"], target=key)
        cli.remote.manifests[build_id] = [
            produced_maven_receipt(
                plan,
                build_id,
                tmp_path / key / "maven",
                target_key=key,
                remote=cli.remote,
            )
        ]
    code, report, error = cli("status", plan=plan)
    assert code == 0 and report["complete"], error
    assert ops.verified_evidence(plan, cli.state, remote=cli.remote)["complete"]
    assert cli(plan=plan, apply=True)[0] == 0
    assert len(cli.remote.queued) == len(plan.targets)


def test_agent_refuses_success_without_publication_receipts(cli):
    plan = public_plan(False)
    cli.remote.missing = {("oss", "maven")}
    assert cli(plan=plan, apply=True)[0] == 1
    for item in saved(cli)["actions"]:
        cli.remote.succeed(
            item["build_id"], plan, "oss", ["maven"], target=item["target"]
        )
        cli.remote.manifests[item["build_id"]] = []
    code, report, error = cli("status", plan=plan)
    assert code != 0
    assert report is None or not report["complete"]
    assert len(cli.remote.queued) == len(plan.targets)


def test_agent_plan_change_cannot_reuse_the_ledger(cli):
    plan = public_plan(False)
    cli.remote.missing = {("oss", "maven")}
    assert cli(plan=plan)[0] == 1
    before = cli.state.read_bytes()
    changed = matrix.build_plan(
        "1.2.1", oss_commits={key: OSS_SHA for key in matrix.DEFAULT_TARGET_KEYS}
    )
    code, report, error = cli(plan=changed, apply=True)
    assert code == 2 and report is None and error
    assert cli.state.read_bytes() == before
    assert not cli.remote.queued


def test_agent_bootstrap_preview_and_local_tag_recovery(candidates):
    bootstrap, repo, origin, plan, _, _ = candidates
    before = bare_git(origin, "show-ref")
    result = bootstrap.execute(repo, plan, None, apply=False)
    assert result["applied"] is False
    assert bare_git(origin, "show-ref") == before
    with pytest.raises(ValueError):
        bootstrap.execute(repo, plan, "a" * 64, apply=True)
    assert bare_git(origin, "show-ref") == before
    assert bootstrap.execute(repo, plan, plan.plan_id, apply=True)["applied"]
    tags = bare_git(origin, "show-ref", "--tags")
    assert bootstrap.execute(repo, plan, plan.plan_id, apply=True)["applied"]
    assert bare_git(origin, "show-ref", "--tags") == tags
    for target in plan.targets:
        for tag in target.oss_tags:
            assert (
                bare_git(origin, "rev-parse", f"refs/tags/{tag}") == target.oss_commit
            )


def test_agent_bootstrap_blocks_pending_candidate_ci(candidates):
    bootstrap, repo, origin, plan, checks, _ = candidates
    checks[plan.targets[0].oss_commit][0]["status"] = "in_progress"
    before = bare_git(origin, "show-ref")
    with pytest.raises(ValueError):
        bootstrap.execute(repo, plan, plan.plan_id, apply=True)
    assert bare_git(origin, "show-ref") == before


def test_rehearsal_rejects_network_and_live_cli_execution():
    with pytest.raises(AssertionError, match="live service"):
        socket.create_connection(("example.invalid", 443))
    with pytest.raises(AssertionError, match="live service"):
        subprocess.run(["gh", "api", "user"], check=True)
    with pytest.raises(AssertionError, match="live service"):
        ops.AzureRemote()
