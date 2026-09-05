# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import base64
import copy
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_matrix as matrix  # noqa: E402
import release_ops as ops  # noqa: E402
import verify_release as verify  # noqa: E402

BASE_CHECKER = verify.Checker
OSS_SHA = "a" * 40
INTERNAL_SHA = "b" * 40
PUBLISHER_SHA = "c" * 40
PROJECT_ID = "b9b2accc-2d1c-45b3-9d24-0eb5d78cc47f"
PROD_PIP_ID = "40ba8cc6-45a4-4580-bf84-257ce1012263"
PROD_UPACK_ID = "cdb0dc93-5fbe-4f25-b8ba-ca322c3fcc03"
TEST_PIP_ID = "33333333-3333-3333-3333-333333333333"
TEST_UPACK_ID = "44444444-4444-4444-4444-444444444444"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("A release driver test attempted a network request")

    monkeypatch.setattr(urllib.request, "urlopen", forbidden)
    monkeypatch.setattr(urllib.request.OpenerDirector, "open", forbidden)


def release_plan(**overrides):
    options = {
        "target_keys": ["master"],
        "oss_commits": {"master": OSS_SHA},
    }
    if overrides.get("repositories") != ["oss"]:
        options["internal_commits"] = {"master": INTERNAL_SHA}
    options.update(overrides)
    return matrix.build_plan("1.1.4", **options)


def command_values(command, option):
    if option not in command:
        return {}
    result = {}
    for value in command[command.index(option) + 1 :]:
        if value.startswith("--"):
            break
        key, separator, text = value.partition("=")
        assert separator
        result[key] = text
    return result


def boolean_parameters(parameters):
    return {
        key
        for key, value in parameters.items()
        if key.startswith("build_") and value in (True, "true")
    }


class InventoryChecker:
    def __init__(self, remote):
        self.remote = remote

    def github_tag(self, tag):
        return verify.OK, self.remote.oss_commit

    def ado_tag(self, tag):
        return verify.OK, INTERNAL_SHA

    def public_maven(self, module, scala, version):
        return self.remote.present("oss", "maven", version)

    def public_central_maven(self, module, scala, version):
        return self.remote.present("oss", "maven-central", version)

    def internal_maven(self, scala, version):
        return self.remote.present("internal", "maven", version)

    def public_pypi(self, version):
        return self.remote.present("oss", "maven", version)

    def pip(self, package, version, internal=False):
        return self.remote.present("internal" if internal else "oss", "pip", version)

    def upack(self, package, version, internal=False):
        return self.remote.present("internal" if internal else "oss", "upack", version)


class FakeRemote:
    """Exercise the real verifier/driver with no network or real CLI execution."""

    def __init__(self, monkeypatch, missing=()):
        self.missing = set(missing)
        self.oss_commit = OSS_SHA
        self.queued = []
        self.builds = {}
        self.timelines = {}
        self.manifests = {}
        self.inventory_calls = []
        self.policy_calls = 0
        self.build_calls = []
        self.queue_error = None
        self.queue_response = None
        self.before_queue = None
        self.absence_calls = []
        self.absence_error = None
        self.variables = {"total_count": 0, "variables": []}
        self.feeds = {
            matrix.PIP_FEED: {"id": PROD_PIP_ID, "name": matrix.PIP_FEED},
            matrix.UPACK_FEED: {"id": PROD_UPACK_ID, "name": matrix.UPACK_FEED},
            "release-test-pip": {"id": TEST_PIP_ID, "name": "release-test-pip"},
            "release-test-upack": {
                "id": TEST_UPACK_ID,
                "name": "release-test-upack",
            },
        }
        for feed in self.feeds.values():
            feed["project"] = {"name": matrix.ADO_PROJECT, "id": PROJECT_ID}
        monkeypatch.setattr(verify, "Checker", lambda *_args: InventoryChecker(self))

    def present(self, repository, family, version):
        return (
            verify.MISSING
            if (repository, family) in self.missing
            or (repository, family, version) in self.missing
            else verify.OK
        )

    def inventory(self, plan):
        self.inventory_calls.append(plan)
        return verify.run_plan(plan)

    def github_variables(self):
        self.policy_calls += 1
        if isinstance(self.variables, Exception):
            raise self.variables
        return copy.deepcopy(self.variables)

    def resolve_feed(self, name):
        value = self.feeds[name.rsplit("/", 1)[-1]]
        if isinstance(value, Exception):
            raise value
        return copy.deepcopy(value)

    def definition(self, pipeline_id):
        repositories = {
            matrix.OSS_MAVEN_PIPELINE_ID: {
                "id": "microsoft/SynapseML",
                "name": "microsoft/SynapseML",
                "type": "GitHub",
                "url": "https://github.com/microsoft/SynapseML",
            },
            matrix.INTERNAL_MAVEN_PIPELINE_ID: {
                "id": "internal-repository-id",
                "name": "SynapseML-Internal",
                "type": "TfsGit",
                "url": ("https://dev.azure.com/msdata/A365/" "_git/SynapseML-Internal"),
            },
            matrix.PUBLISH_PIPELINE_ID: {
                "id": "publisher-repository-id",
                "name": "publisher",
                "type": "TfsGit",
                "url": "https://dev.azure.com/msdata/A365/_git/publisher",
            },
        }
        return {"id": pipeline_id, "repository": repositories[pipeline_id]}

    def build(self, build_id):
        self.build_calls.append(build_id)
        result = self.builds[build_id]
        if isinstance(result, Exception):
            raise result
        return copy.deepcopy(result)

    def timeline(self, build_id):
        return copy.deepcopy(self.timelines[build_id])

    def provenance(self, build_id):
        return copy.deepcopy(self.manifests.get(build_id, []))

    def absence(self, plan, actions, destinations):
        self.absence_calls.append([item["id"] for item in actions])
        if self.absence_error:
            raise self.absence_error
        artifacts = []
        for item in actions:
            family, repository = item["family"], item["repository"]
            package = (
                "synapseml"
                if repository == "oss"
                else "synapseml-internal"
                if family == "pip"
                else "synapseml_internal"
            )
            artifacts.append(
                {
                    "action_id": item["id"],
                    "repository": repository,
                    "target": item["target"],
                    "family": family,
                    "name": package,
                    "version": item["version"],
                    "feed_id": destinations[family]["id"],
                    "project_id": destinations[family]["project_id"],
                    "status": "absent"
                    if self.present(repository, family, item["version"])
                    == verify.MISSING
                    else "present",
                }
            )
        return {
            "schema_version": 1,
            "plan_id": plan.plan_id,
            "checked_at": ops.now(),
            "artifacts": artifacts,
        }

    def fail(self, build_id):
        self.builds[build_id].update(
            status="completed", result="failed", finishTime=ops.now()
        )
        self.timelines[build_id] = {
            "records": [
                {
                    "id": "failed-job",
                    "name": "Release",
                    "type": "Job",
                    "state": "completed",
                    "result": "failed",
                }
            ]
        }

    def queue(self, command):
        if self.before_queue:
            self.before_queue(command)
        self.queued.append(list(command))
        if self.queue_error:
            raise self.queue_error
        build_id = 100 + len(self.queued)
        self.register(command, build_id)
        if self.queue_response is not None:
            return copy.deepcopy(self.queue_response)
        return copy.deepcopy(self.builds[build_id])

    def register(self, command, build_id):
        pipeline_id = int(command[command.index("--id") + 1])
        parameters = command_values(command, "--parameters")
        for key, value in list(parameters.items()):
            if value in ("true", "false"):
                parameters[key] = value == "true"
        source_branch = "refs/heads/master"
        source_version = PUBLISHER_SHA
        if pipeline_id != matrix.PUBLISH_PIPELINE_ID:
            source_branch = command[command.index("--branch") + 1]
            source_version = command[command.index("--commit-id") + 1]
        self.builds[build_id] = {
            "id": build_id,
            "definition": {"id": pipeline_id},
            "repository": self.definition(pipeline_id)["repository"],
            "sourceBranch": source_branch,
            "sourceVersion": source_version,
            "templateParameters": parameters,
            "parameters": json.dumps(command_values(command, "--variables")),
            "status": "notStarted",
            "result": None,
            "queueTime": ops.now(),
        }
        self.timelines[build_id] = {"records": []}
        return self.builds[build_id]

    def succeed(self, build_id, plan, repository, families, target="master"):
        build = self.builds[build_id]
        build.update(status="completed", result="succeeded", finishTime=ops.now())
        self.timelines[build_id] = {
            "records": [
                {
                    "id": "publish-job",
                    "type": "Job",
                    "name": "Release",
                    "state": "completed",
                    "result": "succeeded",
                }
            ]
        }
        tp = next(value for value in plan.targets if value.key == target)
        self.manifests[build_id] = [
            {
                "schema_version": 1,
                "plan_id": plan.plan_id,
                "build_id": build_id,
                "pipeline_id": build["definition"]["id"],
                "repository": repository,
                "target": target,
                "families": [family],
                "source_tag": (
                    f"v{getattr(tp, repository + '_pip_version').split('+')[0]}"
                    f"-python{tp.python}"
                    if family == "pip"
                    else getattr(tp, repository + "_maven_tag")
                ),
                "source_commit": getattr(tp, repository + "_commit"),
                "version": getattr(tp, repository + "_" + family + "_version"),
                "artifacts": [
                    {
                        "path": "output/package.bin",
                        "sha256": "d" * 64,
                        "size": 17,
                    }
                ],
            }
            for family in families
        ]
        if (
            build["definition"]["id"] == matrix.OSS_MAVEN_PIPELINE_ID
            and target == "master"
        ):
            self.manifests[build_id][0]["artifacts"].append(
                {
                    "path": f"pypi/{verify.public_pypi_wheel_name(plan.oss_version)}",
                    "sha256": "e" * 64,
                    "size": 123,
                }
            )
        if build["definition"]["id"] == matrix.PUBLISH_PIPELINE_ID:
            for receipt in self.manifests[build_id]:
                family = receipt["families"][0]
                feed = self.feeds[getattr(plan, family + "_feed").rsplit("/", 1)[-1]]
                destination = {
                    "feed_id": feed["id"],
                    "feed_name": feed["name"],
                    "project_id": PROJECT_ID,
                    "project_name": matrix.ADO_PROJECT,
                }
                package = (
                    "synapseml"
                    if repository == "oss"
                    else "synapseml-internal"
                    if family == "pip"
                    else "synapseml_internal"
                )
                receipt.update(
                    receipt_type="synapseml-publisher",
                    publisher_source_commit=PUBLISHER_SHA,
                    mode=plan.mode,
                    scope=plan.scope,
                    family=family,
                    package_name=package,
                    destination=destination,
                    outputs=[
                        {
                            "family": family,
                            "package_name": package,
                            "version": receipt["version"],
                            **destination,
                        }
                    ],
                )
        for family in families:
            self.missing.discard((repository, family))
            self.missing.discard(
                (
                    repository,
                    family,
                    getattr(tp, repository + "_" + family + "_version"),
                )
            )


@pytest.fixture
def cli(tmp_path, monkeypatch, capsys):
    remote = FakeRemote(monkeypatch)
    plan_path = tmp_path / "plan.json"
    state_path = tmp_path / "state.json"

    def invoke(command="resume", plan=None, apply=False, extra=(), state=True):
        chosen = plan or release_plan()
        plan_path.write_text(json.dumps(matrix.plan_to_dict(chosen)), encoding="utf-8")
        argv = [command, "--plan", str(plan_path)]
        if state:
            argv += ["--state", str(state_path)]
        if apply:
            argv += ["--apply", "--approve-plan", chosen.plan_id]
        argv += list(extra)
        code = ops.main(argv, remote=remote)
        output = capsys.readouterr()
        return code, json.loads(output.out) if output.out else None, output.err

    invoke.remote = remote
    invoke.state = state_path
    invoke.plan = plan_path
    return invoke


def action(state, repository, family, target="master"):
    return next(
        item
        for item in state["actions"]
        if (item["repository"], item["target"], item["family"])
        == (repository, target, family)
    )


def failed_publisher(cli, families=("upack",)):
    plan = release_plan(families=list(families), repositories=["oss"])
    cli.remote.missing = {("oss", family) for family in families}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.fail(101)
    assert cli("status", plan=plan)[0] == 1
    return plan


def test_r3_retry_preserves_failed_attempt_and_reuses_completed_success(cli):
    plan = failed_publisher(cli)
    previous = copy.deepcopy(action(saved(cli), "oss", "upack"))
    code, _, error = cli(plan=plan, apply=True, extra=["--retry", previous["id"]])
    assert code == 1, error
    item = action(saved(cli), "oss", "upack")
    assert item["build_id"] == 102
    assert item["status"] == "pending"
    assert item["attempts"][0]["previous"] == {
        key: value for key, value in previous.items() if key != "attempts"
    }
    assert item["attempts"][0]["proof"]["build"]["id"] == 101
    assert item["attempts"][0]["proof"]["build"]["result"] == "failed"
    assert cli.remote.absence_calls == [[previous["id"]]]
    cli.remote.succeed(102, plan, "oss", ["upack"])
    assert cli(plan=plan, apply=True)[0] == 0
    verify.validate_evidence(
        plan, ops.verified_evidence(plan, cli.state, remote=cli.remote)
    )
    assert len(action(saved(cli), "oss", "upack")["attempts"]) == 1
    assert len(cli.remote.queued) == 2


@pytest.mark.parametrize("approval", ["missing", "wrong", "no-apply"])
def test_r3_retry_requires_exact_approval_before_probes(cli, approval):
    plan = failed_publisher(cli)
    arguments = ["--retry", "publisher.oss.master.upack"]
    if approval == "wrong":
        arguments += ["--apply", "--approve-plan", "0" * 64]
    elif approval == "no-apply":
        arguments += ["--approve-plan", plan.plan_id]
    before = len(cli.remote.inventory_calls)
    assert cli(plan=plan, extra=arguments)[0] == 2
    assert len(cli.remote.inventory_calls) == before
    assert len(cli.remote.queued) == 1


@pytest.mark.parametrize(
    "fault",
    [
        "pending",
        "unknown",
        "unbound",
        "succeeded",
        "canceled",
        "active-job",
        "no-jobs",
        "source",
        "request",
        "inventory-error",
        "absence-error",
    ],
)
def test_r3_retry_refuses_uncertain_or_nonfailed_attempts(cli, fault):
    plan = failed_publisher(cli)
    if fault in {"pending", "unknown", "unbound"}:
        data = saved(cli)
        item = action(data, "oss", "upack")
        item["status"] = "pending" if fault == "pending" else "unknown"
        if fault == "unbound":
            item["build_id"] = None
            item["outcome"] = None
        data["state_id"] = ops._digest(data, "state_id")
        cli.state.write_text(json.dumps(data), encoding="utf-8")
    elif fault in {"succeeded", "canceled"}:
        cli.remote.builds[101]["result"] = fault
    elif fault == "active-job":
        cli.remote.timelines[101]["records"][0]["state"] = "inProgress"
    elif fault == "no-jobs":
        cli.remote.timelines[101]["records"] = []
    elif fault == "source":
        cli.remote.builds[101]["sourceVersion"] = "e" * 40
    elif fault == "request":
        cli.remote.builds[101]["templateParameters"]["release_plan_id"] = "e" * 64
    elif fault == "inventory-error":
        cli.remote.inventory = lambda _plan: (_ for _ in ()).throw(
            RuntimeError("inventory authentication unavailable")
        )
    else:
        cli.remote.absence_error = RuntimeError("absence authentication unavailable")
    code, _, _ = cli(
        plan=plan, apply=True, extra=["--retry", "publisher.oss.master.upack"]
    )
    assert code == 2
    assert len(cli.remote.queued) == 1
    assert not action(saved(cli), "oss", "upack").get("attempts")


@pytest.mark.parametrize("present_family", ["pip", "upack"])
def test_r3_retry_refuses_partial_group_presence(cli, present_family):
    plan = failed_publisher(cli, ("pip", "upack"))
    cli.remote.missing.discard(("oss", present_family))
    code, _, _ = cli(
        plan=plan, apply=True, extra=["--retry", "publisher.oss.master.upack"]
    )
    assert code == 2
    assert len(cli.remote.queued) == 1
    assert not cli.remote.absence_calls


def test_r3_retry_queues_only_original_group_not_unrelated_work(cli):
    plan = release_plan(families=["pip", "upack"], repositories=["oss", "internal"])
    cli.remote.missing = {
        (repository, family)
        for repository in plan.repositories
        for family in plan.families
    }
    assert cli(plan=plan)[0] == 1
    item = action(saved(cli), "oss", "upack")
    operation = ops._operation(plan, item, ["pip", "upack"])
    cli.remote.register(operation["command"], 201)
    cli.remote.fail(201)
    assert cli(plan=plan, apply=True, extra=["--adopt", item["id"] + "=201"])[0] == 1
    assert not cli.remote.queued
    code, _, error = cli(plan=plan, apply=True, extra=["--retry", item["id"]])
    assert code == 1, error
    assert len(cli.remote.queued) == 1
    data = saved(cli)
    for family in ("pip", "upack"):
        retried = action(data, "oss", family)
        assert retried["build_id"] == 101
        assert retried["attempts"][0]["previous"]["build_id"] == 201
        assert action(data, "internal", family)["operation"] is None
    assert boolean_parameters(command_values(cli.remote.queued[0], "--parameters")) == {
        "build_synapseml_pip_py311",
        "build_synapseml_upack_default",
    }


@pytest.mark.parametrize(
    "fault", ["source", "request", "jobs", "absence", "group", "build-id"]
)
def test_r3_retry_history_is_validated_even_after_rehash(cli, fault):
    plan = failed_publisher(cli, ("pip", "upack"))
    assert (
        cli(plan=plan, apply=True, extra=["--retry", "publisher.oss.master.upack"])[0]
        == 1
    )
    data = saved(cli)
    entry = action(data, "oss", "upack")["attempts"][0]
    if fault == "source":
        entry["previous"]["source_commit"] = "e" * 40
    elif fault == "request":
        entry["proof"]["build"]["templateParameters"]["build_internal_pip_py311"] = True
    elif fault == "jobs":
        entry["proof"]["jobs"][0]["state"] = "inProgress"
    elif fault == "absence":
        entry["proof"]["absence"]["artifacts"][0]["status"] = "present"
    elif fault == "group":
        action(data, "oss", "pip")["attempts"] = []
    else:
        action(data, "oss", "upack")["build_id"] = 101
        action(data, "oss", "pip")["build_id"] = 101
    entry["attempt_id"] = ops._digest(entry, "attempt_id")
    data["state_id"] = ops._digest(data, "state_id")
    cli.state.write_text(json.dumps(data), encoding="utf-8")
    before = cli.state.read_bytes()
    assert cli("status", plan=plan)[0] == 2
    assert cli.state.read_bytes() == before
    assert len(cli.remote.queued) == 2


def test_r3_retry_intent_crash_does_not_become_ordinary_planned_work(cli):
    plan = failed_publisher(cli)
    cli.remote.queue_error = RuntimeError("connection lost")
    assert (
        cli(plan=plan, apply=True, extra=["--retry", "publisher.oss.master.upack"])[0]
        == 1
    )
    item = action(saved(cli), "oss", "upack")
    assert item["status"] == "unknown"
    assert item["build_id"] is None
    assert item["outcome"] is None
    assert item["attempts"][0]["previous"]["build_id"] == 101
    assert cli(plan=plan, apply=True)[0] == 1
    assert cli(plan=plan, apply=True, extra=["--retry", item["id"]])[0] == 2
    assert len(cli.remote.queued) == 2


def test_r3_maven_missing_is_not_definitive_namespace_absence(cli, monkeypatch):
    requested = []

    def exists(url, _headers):
        requested.append(url)
        return url.endswith(".jar")

    monkeypatch.setattr(verify, "_url_exists", exists)
    checker = BASE_CHECKER.__new__(BASE_CHECKER)
    checker._public_headers = {}
    assert (
        BASE_CHECKER._maven(checker, "synapseml-core", "2.12", "1.1.4")
        == verify.MISSING
    )
    assert len(requested) == 1, "MISSING can hide a present JAR after a missing POM"
    plan = release_plan(families=["maven"], repositories=["oss"])
    cli.remote.missing = {("oss", "maven")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.fail(101)
    assert cli("status", plan=plan)[0] == 1
    code, _, error = cli(plan=plan, apply=True, extra=["--retry", "maven.oss.master"])
    assert code == 2
    assert "Maven" in error and "absence" in error
    assert len(cli.remote.queued) == 1


def test_r3_plan_claim_rejects_competing_ledger_names(cli):
    plan = failed_publisher(cli)
    other = cli.state.with_name("another-ledger.json")
    before = len(cli.remote.inventory_calls)
    with pytest.raises(ops.ReleaseError, match="ledger"):
        with ops.StateStore(other, plan):
            pytest.fail("A second ledger was acquired")
    assert not other.exists()
    assert len(cli.remote.inventory_calls) == before
    assert len(cli.remote.queued) == 1


def test_r3_plan_guard_serializes_different_ledger_names(cli):
    plan = release_plan(families=["upack"], repositories=["oss"])
    with ops.StateStore(cli.state, plan):
        with pytest.raises(ops.ReleaseError, match="lock"):
            with ops.StateStore(cli.state.with_name("other.json"), plan):
                pytest.fail("A competing filename acquired the same plan")


def test_r3_legacy_state_is_strictly_validated_before_claim_and_upgrade(cli):
    plan = release_plan(families=["upack"], repositories=["oss"])
    assert cli(plan=plan)[0] == 1
    data = saved(cli)
    data["schema_version"] = 1
    for item in data["actions"]:
        item.pop("attempts", None)
    data["state_id"] = ops._digest(data, "state_id")
    cli.state.write_text(json.dumps(data), encoding="utf-8")
    claim = cli.state.parent / f".release-plan-{plan.plan_id}.json"
    if claim.exists():
        claim.unlink()
    assert cli("status", plan=plan)[0] == 1
    migrated = saved(cli)
    assert migrated["schema_version"] == 2
    assert all(item["attempts"] == [] for item in migrated["actions"])
    assert claim.is_file()
    assert not cli.remote.queued


def test_r3_legacy_sibling_is_not_silently_replaced_by_a_new_ledger(cli):
    plan = release_plan(families=["upack"], repositories=["oss"])
    assert cli(plan=plan)[0] == 1
    claim = cli.state.parent / f".release-plan-{plan.plan_id}.json"
    if claim.exists():
        claim.unlink()
    with pytest.raises(ops.ReleaseError, match="ledger"):
        with ops.StateStore(cli.state.with_name("typo.json"), plan):
            pytest.fail("An existing unclaimed ledger was ignored")
    assert not cli.remote.queued


def test_r3_claimed_deleted_ledger_is_not_recreated(cli):
    plan = failed_publisher(cli)
    cli.state.unlink()
    assert cli(plan=plan, apply=True)[0] == 2
    assert not cli.state.exists()
    assert len(cli.remote.queued) == 1


def test_r3_inspect_lock_is_bounded_read_only_and_hides_owner(cli):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.plan.write_text(json.dumps(matrix.plan_to_dict(plan)), encoding="utf-8")
    lock = cli.state.with_name(cli.state.name + ".lock")
    owner = "owner-capability-must-not-appear"
    lock.write_text(
        json.dumps(
            {
                "owner": owner,
                "pid": 424242,
                "host": "release-host",
                "plan_id": plan.plan_id,
                "created_at": ops.now(),
            }
        ),
        encoding="utf-8",
    )
    before = {path.name: path.read_bytes() for path in cli.state.parent.iterdir()}
    code, report, error = cli("status", plan=plan, extra=["--inspect-lock"])
    assert code == 0, error
    state_lock = next(item for item in report["locks"] if item["kind"] == "state")
    assert state_lock["path"] == str(lock.resolve())
    assert state_lock["pid"] == 424242
    assert state_lock["host"] == "release-host"
    assert owner not in json.dumps(report) + error
    assert before == {
        path.name: path.read_bytes() for path in cli.state.parent.iterdir()
    }
    assert not cli.remote.inventory_calls
    assert not cli.remote.policy_calls
    assert not cli.remote.build_calls
    assert not cli.remote.queued
    assert cli(plan=plan, apply=True)[0] == 2
    assert owner not in cli(plan=plan)[2]


def test_r3_new_locks_record_host_without_leaking_capability(cli):
    plan = release_plan(families=["upack"], repositories=["oss"])
    with ops.StateStore(cli.state, plan) as store:
        lock = json.loads(store.lock.read_text(encoding="utf-8"))
        assert isinstance(lock["host"], str) and lock["host"]
        code, _, error = cli(plan=plan)
        assert code == 2
        assert lock["owner"] not in error
        assert ".lock" in error and "host" in error


@pytest.mark.parametrize(
    "fault",
    [
        "absent",
        "present-later",
        "missing-versions",
        "bad-count",
        "loop",
        "http-401",
        "http-404",
    ],
)
def test_r3_absence_transport_requires_complete_authenticated_version_inventory(fault):
    plan = release_plan(families=["upack"], repositories=["oss"])
    actions = ops.build_actions(plan)
    destinations = {
        "upack": {
            "requested": matrix.UPACK_FEED,
            "id": PROD_UPACK_ID,
            "name": matrix.UPACK_FEED,
            "project": matrix.ADO_PROJECT,
            "project_id": PROJECT_ID,
        }
    }
    requests = []

    class Response(io.BytesIO):
        def __init__(self, value, headers):
            super().__init__(ops.canonical(value))
            self.headers = headers

    class Opener:
        def open(self, request, timeout):
            query = dict(
                urllib.parse.parse_qsl(urllib.parse.urlsplit(request.full_url).query)
            )
            requests.append(query)
            assert query["includeAllVersions"] == "true"
            assert query["includeDeleted"] == "true"
            assert query["protocolType"] == "upack"
            if fault.startswith("http-"):
                raise urllib.error.HTTPError(
                    request.full_url, int(fault.split("-")[1]), "unavailable", {}, None
                )
            version = actions[0]["version"] if fault == "present-later" else "0.0.1"
            package = {
                "name": "synapseml",
                "protocolType": "UPack",
                "versions": [{"version": version}],
            }
            if fault == "missing-versions":
                package.pop("versions")
            if len(requests) == 1:
                data = {"count": 0, "value": []}
                headers = {"x-ms-continuationtoken": "next-page"}
            else:
                data = {"count": 1, "value": [package]}
                headers = (
                    {"x-ms-continuationtoken": "next-page"} if fault == "loop" else {}
                )
            if fault == "bad-count":
                data["count"] = True
            if fault == "absent" and len(requests) > 2:
                data, headers = {"count": 0, "value": []}, {}
            return Response(data, headers)

    remote = ops.AzureRemote()
    remote._token = "test-only-token"
    remote._opener = Opener()
    if fault in {"absent", "present-later"}:
        proof = remote.absence(plan, actions, destinations)
        assert len(requests) == (3 if fault == "absent" else 2)
        assert proof["artifacts"][0]["status"] == (
            "present" if fault == "present-later" else "absent"
        )
        if fault == "absent":
            ops._validate_absence(plan, actions, destinations, proof)
        else:
            with pytest.raises(ops.ReleaseError):
                ops._validate_absence(plan, actions, destinations, proof)
    else:
        with pytest.raises(ops.ReleaseError):
            remote.absence(plan, actions, destinations)


@pytest.fixture
def controlled_clock(monkeypatch):
    class Clock(ops.datetime):
        current = ops.datetime.now(ops.timezone.utc)

        @classmethod
        def now(cls, tz=None):
            return (
                cls.current.astimezone(tz)
                if tz is not None
                else cls.current.replace(tzinfo=None)
            )

        @classmethod
        def advance(cls, seconds):
            cls.current += ops.timedelta(seconds=seconds)

    monkeypatch.setattr(ops, "datetime", Clock)
    monkeypatch.setattr(verify, "datetime", Clock)
    return Clock


@pytest.mark.parametrize("coordinate", ["present", "deleted", "absent"])
def test_r4_absence_reads_offset_pages_until_the_collection_ends(
    cli, monkeypatch, coordinate
):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli(plan=plan)
    state = saved(cli)
    actions = state["actions"]
    packages = [
        {"name": f"synapseml-extra-{index}", "protocolType": "UPack"}
        for index in range(100)
    ]
    if coordinate != "absent":
        packages.append(
            {
                "name": "synapseml",
                "protocolType": "UPack",
                "versions": [
                    {
                        "version": actions[0]["version"],
                        "isDeleted": coordinate == "deleted",
                    }
                ],
            }
        )
    requests = []

    def get(url, **_kwargs):
        query = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
        requests.append(query)
        offset = int(query.get("$skip", 0))
        page = packages[offset : offset + 100]
        return {"count": len(page), "value": page}, {}

    remote = ops.AzureRemote()
    monkeypatch.setattr(remote, "_get", get)
    proof = remote.absence(plan, actions, state["destinations"])
    assert [int(query.get("$skip", 0)) for query in requests] == [0, 100]
    assert all(query["$top"] == "100" for query in requests)
    assert proof["artifacts"][0]["status"] == (
        "absent" if coordinate == "absent" else "present"
    )
    if coordinate == "absent":
        ops._validate_absence(plan, actions, state["destinations"], proof)
    else:
        with pytest.raises(ops.ReleaseError):
            ops._validate_absence(plan, actions, state["destinations"], proof)


@pytest.mark.parametrize("fault", ["repeated-page", "page-bound"])
def test_r4_absence_never_accepts_unfinished_offset_enumeration(
    cli, monkeypatch, fault
):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli(plan=plan)
    state = saved(cli)
    calls = []

    def get(url, **_kwargs):
        calls.append(url)
        suffix = 0 if fault == "repeated-page" else len(calls)
        return {
            "count": 1,
            "value": [{"name": f"synapseml-extra-{suffix}", "protocolType": "UPack"}],
        }, {}

    remote = ops.AzureRemote()
    monkeypatch.setattr(remote, "_get", get)
    monkeypatch.setattr(ops, "MAX_ABSENCE_PAGES", 2)
    with pytest.raises(ops.ReleaseError, match="pagination"):
        remote.absence(plan, state["actions"], state["destinations"])
    assert len(calls) == 2


@pytest.mark.parametrize("seconds", [300, 301])
def test_r4_absence_freshness_uses_the_oldest_observation(
    cli, monkeypatch, controlled_clock, seconds
):
    plan = release_plan(families=["pip", "upack"], repositories=["oss"])
    cli(plan=plan)
    state = saved(cli)
    started = ops.now()
    calls = []

    def get(url, **_kwargs):
        calls.append(url)
        if len(calls) == 2:
            controlled_clock.advance(seconds)
        return {"count": 0, "value": []}, {}

    remote = ops.AzureRemote()
    monkeypatch.setattr(remote, "_get", get)
    proof = remote.absence(plan, state["actions"], state["destinations"])
    assert proof["checked_at"] == started
    if seconds == 300:
        ops._validate_absence(plan, state["actions"], state["destinations"], proof)
    else:
        with pytest.raises(ops.ReleaseError, match="stale"):
            ops._validate_absence(plan, state["actions"], state["destinations"], proof)
    assert len(calls) == 2


@pytest.mark.parametrize("phase", ["policy", "save"])
@pytest.mark.parametrize("seconds", [300, 301])
def test_r4_retry_rechecks_freshness_at_submission_and_preserves_failed_attempt(
    cli, monkeypatch, controlled_clock, phase, seconds
):
    plan = failed_publisher(cli)
    previous = copy.deepcopy(action(saved(cli), "oss", "upack"))
    original_policy = ops._policy
    original_save = ops.StateStore.save
    delayed = False

    def policy(*args):
        nonlocal delayed
        result = original_policy(*args)
        if cli.remote.absence_calls and not delayed:
            delayed = True
            controlled_clock.advance(seconds)
        return result

    def save(store):
        nonlocal delayed
        original_save(store)
        item = action(store.state, "oss", "upack")
        if item["attempts"] and item["build_id"] is None and not delayed:
            delayed = True
            controlled_clock.advance(seconds)

    monkeypatch.setattr(
        ops, "_policy", policy if phase == "policy" else original_policy
    )
    monkeypatch.setattr(
        ops.StateStore, "save", save if phase == "save" else original_save
    )
    code, _, error = cli(plan=plan, apply=True, extra=["--retry", previous["id"]])
    if seconds == 300:
        assert code == 1, error
        assert len(cli.remote.queued) == 2
        assert action(saved(cli), "oss", "upack")["build_id"] == 102
    else:
        assert code == 2 and "stale" in error, error
        assert len(cli.remote.queued) == 1
        assert action(saved(cli), "oss", "upack") == previous
        monkeypatch.setattr(ops, "_policy", original_policy)
        monkeypatch.setattr(ops.StateStore, "save", original_save)
        code, _, error = cli(plan=plan, apply=True, extra=["--retry", previous["id"]])
        assert code == 1, error
        assert len(cli.remote.queued) == 2


def test_r4_expiry_restore_failure_keeps_history_and_never_submits(
    cli, monkeypatch, controlled_clock
):
    plan = failed_publisher(cli)
    previous = copy.deepcopy(action(saved(cli), "oss", "upack"))
    original_save = ops.StateStore.save
    expired = False

    def save(store):
        nonlocal expired
        item = action(store.state, "oss", "upack")
        if expired and item["status"] == "failed":
            raise OSError("synthetic-private-restore-detail")
        original_save(store)
        if item["attempts"] and item["build_id"] is None and not expired:
            expired = True
            controlled_clock.advance(301)

    monkeypatch.setattr(ops.StateStore, "save", save)
    code, _, error = cli(plan=plan, apply=True, extra=["--retry", previous["id"]])
    assert code == 2
    assert "synthetic-private-restore-detail" not in error
    assert len(cli.remote.queued) == 1
    item = action(saved(cli), "oss", "upack")
    assert item["status"] == "unknown" and item["build_id"] is None
    assert item["attempts"][-1]["previous"] == {
        key: value for key, value in previous.items() if key != "attempts"
    }
    monkeypatch.setattr(ops.StateStore, "save", original_save)
    assert cli(plan=plan, apply=True)[0] == 1
    assert len(cli.remote.queued) == 1


@pytest.mark.parametrize("contents", ["invalid", "oversized", "missing"])
def test_r3_lock_inspection_never_reads_unbounded_or_mutates_metadata(cli, contents):
    plan = release_plan(families=["upack"], repositories=["oss"])
    lock = cli.state.with_name(cli.state.name + ".lock")
    if contents != "missing":
        lock.write_bytes(b"not-json" if contents == "invalid" else b"x" * 20000)
    before = lock.read_bytes() if lock.exists() else None
    code, report, error = cli("status", plan=plan, extra=["--inspect-lock"])
    assert code == 0, error
    record = next(item for item in report["locks"] if item["kind"] == "state")
    assert record["exists"] == (contents != "missing")
    if contents != "missing":
        assert record["metadata_valid"] is False
    assert (lock.read_bytes() if lock.exists() else None) == before
    assert not cli.remote.inventory_calls
    assert not cli.remote.queued


def test_r3_retry_and_adoption_cannot_be_combined(cli):
    plan = failed_publisher(cli)
    before = len(cli.remote.inventory_calls)
    assert (
        cli(
            plan=plan,
            apply=True,
            extra=[
                "--retry",
                "publisher.oss.master.upack",
                "--adopt",
                "publisher.oss.master.upack=101",
            ],
        )[0]
        == 2
    )
    assert len(cli.remote.inventory_calls) == before
    assert len(cli.remote.queued) == 1


def test_r3_retired_build_cannot_be_adopted_as_a_new_attempt(cli):
    plan = failed_publisher(cli)
    cli.remote.queue_error = RuntimeError("submission ambiguity")
    assert (
        cli(plan=plan, apply=True, extra=["--retry", "publisher.oss.master.upack"])[0]
        == 1
    )
    assert (
        cli(plan=plan, apply=True, extra=["--adopt", "publisher.oss.master.upack=101"])[
            0
        ]
        == 2
    )
    assert len(cli.remote.queued) == 2


def test_r3_second_retry_retains_both_failed_attempts(cli):
    plan = failed_publisher(cli)
    assert (
        cli(plan=plan, apply=True, extra=["--retry", "publisher.oss.master.upack"])[0]
        == 1
    )
    cli.remote.fail(102)
    assert cli("status", plan=plan)[0] == 1
    assert (
        cli(plan=plan, apply=True, extra=["--retry", "publisher.oss.master.upack"])[0]
        == 1
    )
    item = action(saved(cli), "oss", "upack")
    assert [entry["previous"]["build_id"] for entry in item["attempts"]] == [101, 102]
    assert item["build_id"] == 103
    assert len(cli.remote.queued) == 3


def test_r3_retry_limit_preserves_existing_history(cli, monkeypatch):
    plan = failed_publisher(cli)
    monkeypatch.setattr(ops, "MAX_RETRY_ATTEMPTS", 1)
    assert (
        cli(plan=plan, apply=True, extra=["--retry", "publisher.oss.master.upack"])[0]
        == 1
    )
    cli.remote.fail(102)
    assert cli("status", plan=plan)[0] == 1
    assert (
        cli(plan=plan, apply=True, extra=["--retry", "publisher.oss.master.upack"])[0]
        == 2
    )
    assert len(action(saved(cli), "oss", "upack")["attempts"]) == 1
    assert len(cli.remote.queued) == 2


@pytest.mark.parametrize("fault", ["path", "checksum", "version"])
def test_r3_corrupt_plan_claim_fails_before_service_calls(cli, fault):
    plan = failed_publisher(cli)
    claim = cli.state.parent / f".release-plan-{plan.plan_id}.json"
    value = json.loads(claim.read_text(encoding="utf-8"))
    if fault == "path":
        value["state_path"] = str(cli.state.with_name("other.json"))
    elif fault == "version":
        value["schema_version"] = True
    else:
        value["claim_id"] = "0" * 64
    if fault != "checksum":
        value["claim_id"] = ops._digest(value, "claim_id")
    claim.write_text(json.dumps(value), encoding="utf-8")
    before = len(cli.remote.inventory_calls)
    assert cli(plan=plan, apply=True)[0] == 2
    assert len(cli.remote.inventory_calls) == before
    assert len(cli.remote.queued) == 1


def test_r3_corrupt_legacy_state_is_not_upgraded_or_claimed(cli):
    plan = release_plan(families=["upack"], repositories=["oss"])
    assert cli(plan=plan)[0] == 1
    data = saved(cli)
    data["schema_version"] = 1
    for item in data["actions"]:
        item.pop("attempts")
    data["actions"][0]["unexpected"] = True
    data["state_id"] = ops._digest(data, "state_id")
    cli.state.write_text(json.dumps(data), encoding="utf-8")
    original = cli.state.read_bytes()
    claim = cli.state.parent / f".release-plan-{plan.plan_id}.json"
    claim.unlink()
    before = len(cli.remote.inventory_calls)
    assert cli("status", plan=plan)[0] == 2
    assert cli.state.read_bytes() == original
    assert not claim.exists()
    assert len(cli.remote.inventory_calls) == before


def test_r3_claimed_v2_ledger_cannot_downgrade_away_retry_history(cli):
    plan = failed_publisher(cli)
    assert (
        cli(plan=plan, apply=True, extra=["--retry", "publisher.oss.master.upack"])[0]
        == 1
    )
    data = saved(cli)
    data["schema_version"] = 1
    for item in data["actions"]:
        item.pop("attempts")
    data["state_id"] = ops._digest(data, "state_id")
    cli.state.write_text(json.dumps(data), encoding="utf-8")
    before = len(cli.remote.inventory_calls)
    code, _, error = cli("status", plan=plan)
    assert code == 2
    assert "downgrad" in error
    assert len(cli.remote.inventory_calls) == before
    assert len(cli.remote.queued) == 2


def test_r3_returned_build_id_is_acknowledged_before_post_queue_save(cli, monkeypatch):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}
    original_save = ops.StateStore.save

    def fail_after_return(store):
        if any(item["build_id"] == 101 for item in store.state["actions"]):
            raise OSError("disk failed with private diagnostic")
        original_save(store)

    monkeypatch.setattr(ops.StateStore, "save", fail_after_return)
    code, _, error = cli(plan=plan, apply=True)
    assert code == 2
    assert "Azure build 101" in error
    assert "operation" in error and "--adopt" in error
    assert "private diagnostic" not in error
    assert action(saved(cli), "oss", "upack")["build_id"] is None
    assert len(cli.remote.queued) == 1
    monkeypatch.setattr(ops.StateStore, "save", original_save)
    assert (
        cli(plan=plan, apply=True, extra=["--adopt", "publisher.oss.master.upack=101"])[
            0
        ]
        == 1
    )
    assert action(saved(cli), "oss", "upack")["build_id"] == 101
    assert len(cli.remote.queued) == 1


@pytest.mark.parametrize("phase", ["feed", "queue", "queue-validation", "refresh"])
def test_unexpected_adapter_bug_propagates_without_losing_submission(
    cli, monkeypatch, phase
):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}

    def broken(*_args, **_kwargs):
        raise TypeError("invalid adapter implementation")

    if phase == "feed":
        monkeypatch.setattr(cli.remote, "resolve_feed", broken)
    elif phase == "queue":
        cli.remote.queue_error = TypeError("invalid adapter implementation")
    elif phase == "queue-validation":
        monkeypatch.setattr(ops, "_validate_build", broken)
    else:
        monkeypatch.setattr(cli.remote, "build", broken)

    with pytest.raises(TypeError, match="invalid adapter implementation"):
        cli(plan=plan, apply=True)
    if phase == "feed":
        assert not cli.remote.queued
        return
    assert len(cli.remote.queued) == 1
    item = action(saved(cli), "oss", "upack")
    assert item["status"] == "unknown"
    assert item["operation"] is not None
    assert item["build_id"] == (None if phase == "queue" else 101)
    assert item["receipt"] is None


@pytest.mark.parametrize("failure", [KeyboardInterrupt, SystemExit, TypeError])
def test_state_entry_failure_releases_only_its_owned_locks(cli, monkeypatch, failure):
    plan = release_plan(families=["upack"], repositories=["oss"])
    store = ops.StateStore(cli.state, plan)

    def broken():
        raise failure("entry interrupted")

    monkeypatch.setattr(store, "_ensure_claim", broken)
    with pytest.raises(failure):
        with store:
            pytest.fail("An interrupted entry must not succeed")
    assert not store.lock.exists()
    assert not store.guard.exists()
    assert not cli.state.exists()


def test_lock_cleanup_failure_is_reported_without_private_diagnostics(
    cli, monkeypatch, capsys
):
    plan = release_plan(families=["upack"], repositories=["oss"])
    original_unlink = Path.unlink
    store = ops.StateStore(cli.state, plan)

    def remove(path, *args, **kwargs):
        if path == store.lock:
            raise OSError("synthetic-private-cleanup-detail")
        return original_unlink(path, *args, **kwargs)

    with store:
        monkeypatch.setattr(Path, "unlink", remove)
    error = capsys.readouterr().err
    assert str(store.lock) in error and "--inspect-lock" in error
    assert "synthetic-private-cleanup-detail" not in error
    assert store.lock.exists()
    assert not store.guard.exists()


@pytest.fixture
def legacy_iso_parser(monkeypatch):
    original = ops.datetime

    class LegacyISO(original):
        @classmethod
        def fromisoformat(cls, value):
            if isinstance(value, str):
                if value.endswith("Z"):
                    raise ValueError("legacy parser does not accept Z")
                fractions = re.findall(r"\.(\d+)", value)
                if any(len(fraction) not in (3, 6) for fraction in fractions):
                    raise ValueError(
                        "legacy parser needs millisecond or microsecond precision"
                    )
            return original.fromisoformat(value)

    monkeypatch.setattr(ops, "datetime", LegacyISO)


@pytest.mark.parametrize(
    "value,microseconds",
    [
        ("2020-01-02T03:04:05Z", 0),
        ("2020-01-02T03:04:05.1Z", 100000),
        ("2020-01-02T03:04:05.1234567Z", 123456),
    ],
)
def test_r6_azure_timestamps_work_with_legacy_iso_parsers(
    legacy_iso_parser, value, microseconds
):
    result = ops._time(value, "Azure time")
    assert result.utcoffset() == ops.timedelta(0)
    assert result.microsecond == microseconds


def test_r6_cli_reconciles_actual_azure_z_timestamps(cli, legacy_iso_parser):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "oss", ["upack"])
    for key in ("queueTime", "finishTime"):
        cli.remote.builds[101][key] = cli.remote.builds[101][key].replace("+00:00", "Z")
    code, report, error = cli("status", plan=plan)
    assert code == 0, error
    assert report["complete"]


@pytest.mark.parametrize("filename,body", [("notes.txt", "note"), ("broken.json", "{")])
def test_r6_ledger_discovery_names_the_offending_file(cli, filename, body):
    candidate = cli.state.parent / filename
    candidate.write_text(body, encoding="utf-8")
    code, _, error = cli(plan=release_plan(families=["upack"], repositories=["oss"]))
    assert code == 2 and str(candidate) in error
    assert candidate.read_text(encoding="utf-8") == body
    assert not cli.state.exists() and not cli.remote.queued


def test_r6_status_summarizes_attempts_without_repeating_the_plan(cli):
    plan = failed_publisher(cli, families=("pip", "upack"))
    identifier = action(saved(cli), "oss", "upack")["id"]
    assert cli(plan=plan, apply=True, extra=["--retry", identifier])[0] == 1
    state_before = cli.state.read_bytes()
    code, report, error = cli("status", plan=plan)
    assert code == 1, error
    state = saved(cli)
    for row in report["actions"]:
        full = action(state, row["repository"], row["family"])["attempts"][0]
        assert row["attempts"] == [
            {
                "number": full["number"],
                "retried_at": full["retried_at"],
                "build_id": full["previous"]["build_id"],
                "operation_id": full["previous"]["operation"]["id"],
                "status": full["previous"]["status"],
                "error": full["previous"]["error"],
                "attempt_id": full["attempt_id"],
                "absence_checked_at": full["proof"]["absence"]["checked_at"],
            }
        ]
        assert full["proof"]["build"]["templateParameters"]["release_plan_base64"]
    assert b"release_plan_base64" not in ops.canonical(report)
    assert len(ops.canonical(report)) < len(ops.canonical(state)) // 2
    assert (
        json.loads(state_before)["actions"][0]["attempts"]
        == state["actions"][0]["attempts"]
    )


def saved(cli):
    return json.loads(cli.state.read_text(encoding="utf-8"))


def test_dry_run_exercises_cli_and_never_queues(cli):
    cli.remote.missing = {("oss", "upack")}
    plan = release_plan(families=["upack"], repositories=["oss"])
    code, report, error = cli("preflight", plan=plan, state=False)
    assert code == 0, error
    assert report["plan_id"] == plan.plan_id
    assert report["complete"] is False
    assert not cli.state.exists()
    assert not cli.remote.queued
    code, report, error = cli(plan=plan)
    assert code == 1, error
    assert cli.state.exists()
    assert not cli.remote.queued
    assert action(saved(cli), "oss", "upack")["status"] == "planned"


@pytest.mark.parametrize(
    "extra",
    [
        ["--apply"],
        ["--approve-plan", "a" * 64],
        ["--apply", "--approve-plan", "a" * 64],
        ["--apply", "--approve-plan", "\u00e9" * 64],
    ],
)
def test_approval_requires_both_flags_and_exact_digest(cli, extra):
    code, _, error = cli(extra=extra)
    assert code == 2
    assert "approv" in error.lower()
    assert not cli.remote.queued
    assert not cli.remote.inventory_calls
    assert not cli.state.exists()


def test_mutated_and_unbound_plan_fail_before_any_probe(tmp_path, monkeypatch, capsys):
    remote = FakeRemote(monkeypatch)
    path = tmp_path / "plan.json"
    plan = release_plan()
    data = matrix.plan_to_dict(plan)
    data["targets"][0]["oss_commit"] = "e" * 40
    path.write_text(json.dumps(data), encoding="utf-8")
    assert ops.main(["preflight", "--plan", str(path)], remote=remote) == 2
    assert "digest" in capsys.readouterr().err
    path.write_text(
        json.dumps(matrix.plan_to_dict(matrix.build_plan("1.1.4"))),
        encoding="utf-8",
    )
    assert ops.main(["preflight", "--plan", str(path)], remote=remote) == 2
    assert "commit" in capsys.readouterr().err
    assert not remote.inventory_calls


@pytest.mark.parametrize("value", ["true", "TrUe", "TRUE"])
def test_full_release_skip_policy_blocks_all_actions(cli, value):
    cli.remote.variables = {
        "total_count": 1,
        "variables": [{"name": "SKIP_SPARK40", "value": value}],
    }
    cli.remote.missing = {("oss", "maven"), ("internal", "maven")}
    code, _, error = cli(apply=True)
    assert code == 2
    assert "SKIP_SPARK40" in error
    assert not cli.remote.queued


@pytest.mark.parametrize(
    "response",
    [
        RuntimeError("policy query failed"),
        {},
        {"variables": [], "total_count": 2},
        {"variables": [{"name": "SKIP_SPARK40"}], "total_count": 1},
    ],
)
def test_skip_policy_fails_closed_for_inaccessible_or_invalid_api(cli, response):
    cli.remote.variables = response
    assert cli(apply=True)[0] == 2
    assert not cli.remote.queued


def test_wrong_tag_commit_blocks_before_any_queue(cli):
    cli.remote.oss_commit = "e" * 40
    cli.remote.missing = {("oss", "maven")}
    code, _, error = cli(apply=True)
    assert code == 2
    assert "commit" in error
    assert not cli.remote.queued


def test_only_missing_selected_oss_family_is_enabled(cli):
    cli.remote.missing = {("oss", "upack")}
    plan = release_plan()
    code, _, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert len(cli.remote.queued) == 1
    command = cli.remote.queued[0]
    assert command[command.index("--id") + 1] == str(matrix.PUBLISH_PIPELINE_ID)
    parameters = command_values(command, "--parameters")
    assert boolean_parameters(parameters) == {"build_synapseml_upack_default"}
    assert parameters["release_plan_id"] == plan.plan_id
    assert parameters["publish_release"] == "true"
    assert json.loads(base64.b64decode(parameters["release_plan_base64"])) == (
        matrix.plan_to_dict(plan)
    )
    assert action(saved(cli), "oss", "upack")["build_id"] == 101
    assert action(saved(cli), "oss", "pip")["status"] == "existing"


def test_publisher_batches_missing_families_for_one_target_and_repo(cli):
    cli.remote.missing = {("internal", "pip"), ("internal", "upack")}
    plan = release_plan(internal_patch="2", scope="internal-only")
    code, _, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert len(cli.remote.queued) == 1
    parameters = command_values(cli.remote.queued[0], "--parameters")
    assert boolean_parameters(parameters) == {
        "build_internal_pip_py311",
        "build_internal_upack_default",
    }
    assert {
        action(saved(cli), "internal", family)["build_id"]
        for family in ("pip", "upack")
    } == {101}


@pytest.mark.parametrize("scope,patch", [("internal-only", "3"), ("full", "0")])
def test_internal_track_reuses_bound_base_without_oss_jobs(cli, scope, patch):
    cli.remote.missing = {("internal", "pip")}
    plan = release_plan(
        scope=scope,
        internal_patch=patch,
        repositories=["internal"],
        families=["pip"],
    )
    code, _, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert len(cli.remote.queued) == 1
    assert boolean_parameters(command_values(cli.remote.queued[0], "--parameters")) == {
        "build_internal_pip_py311"
    }
    assert all(item["repository"] == "internal" for item in saved(cli)["actions"])
    assert bool(cli.remote.policy_calls) == (scope == "full")
    assert any(
        checked.repositories == ["oss"] and checked.targets[0].oss_commit == OSS_SHA
        for checked in cli.remote.inventory_calls
    )


@pytest.mark.parametrize("scope,patch", [("full", "0"), ("internal-only", "2")])
def test_oss_first_staging_never_requires_internal_publication(
    tmp_path, monkeypatch, capsys, scope, patch
):
    remote = FakeRemote(monkeypatch, missing={("oss", "maven"), ("internal", "maven")})
    oss = matrix.build_plan(
        "1.1.4",
        target_keys=["master"],
        families=["maven"],
        repositories=["oss"],
        oss_commits={"master": OSS_SHA},
    )
    internal = release_plan(
        scope=scope, internal_patch=patch, families=["maven"], repositories=["internal"]
    )
    assert oss.targets[0].internal_commit is None
    assert oss.plan_id != internal.plan_id

    def invoke(plan, name):
        plan_path = tmp_path / (name + "-plan.json")
        state_path = tmp_path / (name + "-state.json")
        plan_path.write_text(json.dumps(matrix.plan_to_dict(plan)), encoding="utf-8")
        code = ops.main(
            [
                "resume",
                "--plan",
                str(plan_path),
                "--state",
                str(state_path),
                "--apply",
                "--approve-plan",
                plan.plan_id,
            ],
            remote=remote,
        )
        output = capsys.readouterr()
        assert output.out, output.err
        return (
            code,
            json.loads(output.out),
            json.loads(state_path.read_text(encoding="utf-8")),
        )

    original_tag = InventoryChecker.ado_tag
    original_maven = InventoryChecker.internal_maven

    def unavailable(*_args, **_kwargs):
        raise AssertionError(
            "OSS publication must not require Internal tags or outputs"
        )

    monkeypatch.setattr(InventoryChecker, "ado_tag", unavailable)
    monkeypatch.setattr(InventoryChecker, "internal_maven", unavailable)
    assert invoke(oss, "oss")[0] == 1
    assert [command[command.index("--id") + 1] for command in remote.queued] == [
        "17563"
    ]
    remote.succeed(101, oss, "oss", ["maven"])
    code, report, state = invoke(oss, "oss")
    assert code == 0
    assert report["complete"]
    assert {item["repository"] for item in state["actions"]} == {"oss"}
    assert all(plan.repositories == ["oss"] for plan in remote.inventory_calls)

    monkeypatch.setattr(InventoryChecker, "ado_tag", original_tag)
    monkeypatch.setattr(InventoryChecker, "internal_maven", original_maven)
    queued_before_internal = len(remote.queued)
    code, report, state = invoke(internal, "internal")
    assert code == 1
    assert {item["repository"] for item in state["actions"]} == {"internal"}
    assert [
        command[command.index("--id") + 1]
        for command in remote.queued[queued_before_internal:]
    ] == ["18453"]
    assert (
        command_values(remote.queued[-1], "--parameters")["release_plan_id"]
        == internal.plan_id
    )
    assert any(
        checked.repositories == ["oss"]
        and checked.targets[0].oss_commit == OSS_SHA
        and checked.targets[0].oss_maven_version == oss.targets[0].oss_maven_version
        for checked in remote.inventory_calls
    )
    remote.succeed(102, internal, "internal", ["maven"])
    code, report, state = invoke(internal, "internal")
    assert code == 0
    assert report["complete"]
    assert len(remote.queued) == 2
    assert all(item["source_commit"] == INTERNAL_SHA for item in state["actions"])


def test_internal_only_rejects_wrong_existing_oss_base(cli):
    cli.remote.oss_commit = "e" * 40
    plan = release_plan(internal_patch="1", scope="internal-only", families=["pip"])
    assert cli(plan=plan, apply=True)[0] == 2
    assert not cli.remote.queued


def test_missing_unselected_maven_is_dependency_not_implicit_queue(cli):
    cli.remote.missing = {("internal", "pip"), ("internal", "maven")}
    plan = release_plan(internal_patch="1", scope="internal-only", families=["pip"])
    code, report, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert not cli.remote.queued
    assert "maven" in json.dumps(report["actions"])


def test_maven_queue_has_exact_release_source_and_contract_parameters(cli):
    cli.remote.missing = {("oss", "maven"), ("internal", "maven")}
    plan = release_plan(families=["maven"])
    code, _, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert len(cli.remote.queued) == 1
    command = cli.remote.queued[0]
    assert command[command.index("--id") + 1] == "17563"
    assert command[command.index("--branch") + 1] == "refs/tags/v1.1.4"
    assert command[command.index("--commit-id") + 1] == OSS_SHA
    assert command_values(command, "--parameters")["publishRelease"] == "true"
    assert command_values(command, "--variables") == {
        "SYNAPSEML_RELEASE_PLAN_ID": plan.plan_id,
        "SYNAPSEML_RELEASE_COMMIT": OSS_SHA,
    }
    cli.remote.succeed(101, plan, "oss", ["maven"])
    code, _, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert len(cli.remote.queued) == 2
    command = cli.remote.queued[-1]
    assert command[command.index("--id") + 1] == "18453"
    assert command[command.index("--commit-id") + 1] == INTERNAL_SHA
    assert command[command.index("--branch") + 1] == "refs/tags/v1.1.4.0"
    assert command_values(command, "--parameters") == {
        "release_publish": "true",
        "release_tag": "v1.1.4.0",
        "release_commit": INTERNAL_SHA,
        "release_plan_id": plan.plan_id,
        "release_plan_base64": base64.b64encode(
            ops.canonical(matrix.plan_to_dict(plan))
        ).decode("ascii"),
    }


@pytest.mark.parametrize("target", ["master", "spark4.0"])
def test_internal_maven_preserves_v_in_parameter_and_branch(cli, target):
    plan = release_plan(
        scope="internal-only",
        internal_patch="1",
        repositories=["internal"],
        families=["maven"],
        target_keys=[target],
        oss_commits={target: OSS_SHA},
        internal_commits={target: INTERNAL_SHA},
    )
    cli.remote.missing = {("internal", "maven")}
    code, _, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert len(cli.remote.queued) == 1
    command = cli.remote.queued[0]
    tag = plan.targets[0].internal_maven_tag
    parameters = command_values(command, "--parameters")
    assert tag.startswith("v")
    assert parameters["release_tag"] == tag
    assert parameters["release_tag"] != plan.targets[0].internal_maven_version
    assert base64.b64decode(parameters["release_plan_base64"]) == ops.canonical(
        matrix.plan_to_dict(plan)
    )
    assert command[command.index("--branch") + 1] == "refs/tags/" + tag
    assert command[command.index("--commit-id") + 1] == INTERNAL_SHA


def produced_maven_receipt(plan, build_id, root):
    from release_guard import maven_receipt

    target = plan.targets[0]
    for module in verify.PUBLIC_MAVEN_MODULES:
        artifact = f"{module}_{target.scala}"
        directory = root / artifact
        directory.mkdir(parents=True)
        suffixes = [".pom", ".jar"]
        if module == "synapseml-core":
            suffixes.append("-tests.jar")
        for suffix in suffixes:
            (
                directory / (artifact + "-" + target.oss_maven_version + suffix)
            ).write_bytes(b"fixture output")
    wheel = None
    if target.key == "master":
        wheel = root.parent / "pypi" / verify.public_pypi_wheel_name(plan.oss_version)
        wheel.parent.mkdir()
        with zipfile.ZipFile(wheel, "w") as archive:
            archive.writestr(
                f"synapseml-{plan.oss_version}.dist-info/METADATA",
                f"Metadata-Version: 2.1\nName: synapseml\nVersion: {plan.oss_version}\n",
            )
    return maven_receipt(plan, target, root, build_id, pypi_wheel=wheel)


def test_public_maven_receipt_uses_the_actual_guard_producer(cli, tmp_path):
    plan = release_plan(families=["maven"], repositories=["oss"])
    cli.remote.missing = {("oss", "maven")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "oss", ["maven"])
    root = tmp_path / "maven-artifacts"
    produced = produced_maven_receipt(plan, 101, root)
    cli.remote.manifests[101] = [produced]
    code, report, error = cli("status", plan=plan)
    assert code == 0, error
    assert report["complete"]
    receipt = action(saved(cli), "oss", "maven")["receipt"]["provenance"][0]
    assert receipt == produced
    assert len(receipt["artifacts"]) == len(verify.PUBLIC_MAVEN_MODULES) * 2 + 2
    for item in receipt["artifacts"]:
        base = root.parent if item["path"].startswith("pypi/") else root
        content = base.joinpath(*item["path"].split("/")).read_bytes()
        assert item["sha256"] == hashlib.sha256(content).hexdigest()
        assert item["size"] == len(content)


def test_primary_maven_requires_published_wheel_provenance(cli):
    plan = release_plan(families=["maven"], repositories=["oss"])
    cli.remote.missing = {("oss", "maven")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "oss", ["maven"])
    receipt = cli.remote.manifests[101][0]
    receipt["artifacts"] = [
        item for item in receipt["artifacts"] if not item["path"].startswith("pypi/")
    ]
    assert cli("status", plan=plan)[0] == 1
    assert action(saved(cli), "oss", "maven")["status"] == "unknown"
    assert not ops.verified_evidence(plan, cli.state, remote=cli.remote)["complete"]
    assert len(cli.remote.queued) == 1


@pytest.mark.parametrize("repository", ["oss", "internal"])
@pytest.mark.parametrize(
    "corruption",
    [
        "schema",
        "plan",
        "build",
        "pipeline",
        "repository",
        "target",
        "tag",
        "commit",
        "version",
        "family",
        "hash",
        "size",
        "empty",
        "duplicate",
        "path",
    ],
)
def test_maven_receipt_rejects_wrong_identity_or_hashes(
    cli, tmp_path, repository, corruption
):
    plan = release_plan(families=["maven"], repositories=[repository])
    cli.remote.missing = {(repository, "maven")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, repository, ["maven"])
    receipt = (
        produced_maven_receipt(plan, 101, tmp_path / "maven-artifacts")
        if repository == "oss"
        else copy.deepcopy(cli.remote.manifests[101][0])
    )
    cli.remote.manifests[101] = [receipt]
    invalid_fields = {
        "schema": ("schema_version", 2),
        "plan": ("plan_id", "e" * 64),
        "build": ("build_id", True),
        "pipeline": ("pipeline_id", 35879),
        "repository": ("repository", "internal" if repository == "oss" else "oss"),
        "target": ("target", "spark4.0"),
        "tag": ("source_tag", "refs/tags/v1.1.4"),
        "commit": ("source_commit", "e" * 40),
        "version": ("version", "9.9.9"),
        "family": ("families", ["upack"]),
    }
    if corruption in invalid_fields:
        key, value = invalid_fields[corruption]
        receipt[key] = value
    elif corruption == "hash":
        receipt["artifacts"][0]["sha256"] = "invalid-hash"
    elif corruption == "size":
        receipt["artifacts"][0]["size"] = True
    elif corruption == "empty":
        receipt["artifacts"] = []
    elif corruption == "duplicate":
        receipt["artifacts"].append(copy.deepcopy(receipt["artifacts"][0]))
    else:
        receipt["artifacts"][0]["path"] = "../unrelated.jar"
    code, report, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert not report["complete"]
    item = action(saved(cli), repository, "maven")
    assert item["status"] == "unknown"
    assert item["build_id"] == 101
    assert item["receipt"] is None
    assert len(cli.remote.queued) == 1


def test_public_maven_and_publisher_queue_the_same_sealed_payload(cli):
    plan = release_plan(families=["maven", "upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "maven"), ("oss", "upack")}
    sealed = ops.canonical(matrix.plan_to_dict(plan))
    assert cli(plan=plan, apply=True)[0] == 1
    maven = command_values(cli.remote.queued[0], "--parameters")
    assert set(maven) == {"publishRelease", "release_plan_base64", "release_plan_id"}
    assert maven["publishRelease"] == "true"
    assert maven["release_plan_id"] == plan.plan_id
    assert base64.b64decode(maven["release_plan_base64"]) == sealed
    assert command_values(cli.remote.queued[0], "--variables") == {
        "SYNAPSEML_RELEASE_COMMIT": OSS_SHA,
        "SYNAPSEML_RELEASE_PLAN_ID": plan.plan_id,
    }
    cli.remote.succeed(101, plan, "oss", ["maven"])
    assert cli(plan=plan, apply=True)[0] == 1
    assert len(cli.remote.queued) == 2
    publisher = command_values(cli.remote.queued[1], "--parameters")
    assert publisher["release_plan_base64"] == maven["release_plan_base64"]
    assert publisher["release_plan_id"] == maven["release_plan_id"]
    assert publisher["publish_release"] == "true"
    assert ops.canonical(matrix.plan_to_dict(plan)) == sealed


def test_public_maven_waits_for_required_central_coordinates(cli):
    plan = release_plan(families=["maven"], repositories=["oss"])
    cli.remote.missing = {("oss", "maven-central")}
    assert cli(plan=plan, apply=True)[0] == 1
    assert len(cli.remote.queued) == 1
    cli.remote.succeed(101, plan, "oss", ["maven"])
    code, report, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert not report["complete"]
    assert action(saved(cli), "oss", "maven")["status"] == "pending"
    assert any(
        row["kind"] == "maven-central" and row["status"] == verify.MISSING
        for row in report["inventory"]["rows"]
    )
    cli.remote.missing.clear()
    assert cli(plan=plan, apply=True)[0] == 0
    assert len(cli.remote.queued) == 1


@pytest.mark.parametrize("cdn_present", [True, False])
def test_internal_base_requires_cdn_not_new_central_publication(cli, cdn_present):
    plan = release_plan(
        scope="internal-only",
        internal_patch="1",
        repositories=["internal"],
        families=["maven"],
    )
    cli.remote.missing = {
        ("internal", "maven"),
        ("oss", "maven-central"),
        ("oss", "pypi"),
    }
    if not cdn_present:
        cli.remote.missing.add(("oss", "maven"))
    code, report, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert all(item["repository"] == "internal" for item in report["actions"])
    assert any(
        row["kind"] == "maven-central" and row["status"] == verify.MISSING
        for dependency in report["dependency_inventory"]
        for row in dependency["rows"]
    )
    if not cdn_present:
        assert not cli.remote.queued
        assert "maven.oss.master" in " ".join(report["actions"][0]["blocked"])
        return
    assert len(cli.remote.queued) == 1
    command = cli.remote.queued[0]
    assert command[command.index("--id") + 1] == "18453"
    cli.remote.succeed(101, plan, "internal", ["maven"])
    evidence = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    assert evidence["complete"]
    verify.validate_evidence(plan, evidence)
    assert len(cli.remote.queued) == 1


def test_successful_maven_build_with_skipped_release_tasks_is_not_complete(cli):
    plan = release_plan(families=["maven"], repositories=["oss"])
    cli.remote.missing = {("oss", "maven")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "oss", ["maven"])
    cli.remote.manifests[101] = []
    cli.remote.timelines[101] = {
        "records": [
            {
                "id": "build-job",
                "type": "Job",
                "name": "Style and tests",
                "state": "completed",
                "result": "succeeded",
            },
            {
                "id": "release-task",
                "type": "Task",
                "name": "Publish Maven release",
                "state": "completed",
                "result": "skipped",
            },
        ]
    }
    code, report, error = cli("status", plan=plan)
    assert code == 1, error
    assert report["inventory"]["inventory_complete"]
    assert not report["inventory"]["complete"]
    assert not report["complete"]
    item = action(saved(cli), "oss", "maven")
    assert item["status"] == "unknown"
    assert item["build_id"] == 101
    assert item["receipt"] is None
    assert "provenance" in item["error"]
    assert cli(plan=plan, apply=True)[0] == 1
    assert len(cli.remote.queued) == 1


def test_counter_parameters_do_not_inherit_stale_pipeline_defaults(cli):
    cli.remote.missing = {("oss", "upack")}
    plan = release_plan(
        families=["upack"], repositories=["oss"], upack_iteration={"master": 2}
    )
    assert cli(plan=plan, apply=True)[0] == 1
    variables = command_values(cli.remote.queued[0], "--variables")
    assert variables == {
        "SYNAPSEML_PATCH_VERSION": "2",
        "SYNAPSEML_INTERNAL_PATCH_VERSION": "",
    }


def test_intent_is_durable_before_queue_and_unknown_is_never_retried(cli):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}

    def inspect_intent(_command):
        item = action(saved(cli), "oss", "upack")
        assert item["status"] == "unknown"
        assert item["intent_at"]
        assert item["build_id"] is None
        assert item["command"] == _command
        assert cli.state.with_name(cli.state.name + ".lock").exists()

    cli.remote.before_queue = inspect_intent
    cli.remote.queue_error = RuntimeError("ambiguous submission")
    code, _, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert action(saved(cli), "oss", "upack")["status"] == "unknown"
    cli.remote.queue_error = None
    cli.remote.before_queue = None
    assert cli(plan=plan, apply=True)[0] == 1
    assert len(cli.remote.queued) == 1


def test_returned_run_id_is_saved_before_first_followup_query(cli, monkeypatch):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}
    real_build = cli.remote.build

    def after_queue(build_id):
        assert action(saved(cli), "oss", "upack")["build_id"] == build_id
        return real_build(build_id)

    monkeypatch.setattr(cli.remote, "build", after_queue)
    assert cli(plan=plan, apply=True)[0] == 1
    assert action(saved(cli), "oss", "upack")["status"] == "pending"


def test_contradictory_queue_source_is_recorded_as_unknown(cli):
    plan = release_plan(families=["maven"], repositories=["oss"])
    cli.remote.missing = {("oss", "maven")}
    cli.remote.queue_response = {
        "id": 101,
        "sourceVersion": "e" * 40,
        "sourceBranch": "refs/tags/v1.1.4",
    }
    assert cli(plan=plan, apply=True)[0] == 1
    item = action(saved(cli), "oss", "maven")
    assert item["build_id"] == 101
    assert item["status"] == "unknown"
    assert len(cli.remote.queued) == 1


def test_minimal_queue_id_requires_authoritative_followup(cli):
    plan = release_plan(families=["maven"], repositories=["oss"])
    cli.remote.missing = {("oss", "maven")}
    cli.remote.queue_response = {"id": 101}
    assert cli(plan=plan, apply=True)[0] == 1
    assert action(saved(cli), "oss", "maven")["status"] == "pending"
    assert cli.remote.build_calls == [101]


def test_unknown_submission_stays_unknown_when_artifact_appears(cli):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}
    cli.remote.queue_error = RuntimeError("a secret must not be persisted")
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.missing.clear()
    code, report, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert not report["complete"]
    assert action(saved(cli), "oss", "upack")["status"] == "unknown"
    assert "a secret must not" not in cli.state.read_text(encoding="utf-8")
    assert len(cli.remote.queued) == 1


@pytest.mark.parametrize("result", ["pending", "failed", "canceled"])
def test_pending_and_failed_builds_are_not_queued_twice(cli, result):
    cli.remote.missing = {("oss", "upack")}
    plan = release_plan(families=["upack"], repositories=["oss"])
    assert cli(plan=plan, apply=True)[0] == 1
    if result != "pending":
        cli.remote.builds[101].update(
            status="completed", result=result, finishTime=ops.now()
        )
    code, _, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert len(cli.remote.queued) == 1
    item = action(saved(cli), "oss", "upack")
    assert item["status"] == ("pending" if result == "pending" else "failed")
    assert item["build_id"] == 101


def test_status_requires_successful_azure_receipt_and_reuses_completed_id(cli):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "oss", ["upack"])
    code, report, error = cli("status", plan=plan)
    assert code == 0, error
    assert report["complete"]
    item = action(saved(cli), "oss", "upack")
    assert item["status"] == "complete"
    assert item["receipt"]["build_id"] == 101
    assert item["receipt"]["publisher_commit"] == PUBLISHER_SHA
    assert item["source_commit"] == OSS_SHA
    assert cli(plan=plan, apply=True)[0] == 0
    assert len(cli.remote.queued) == 1
    assert cli.remote.build_calls.count(101) >= 3


def test_baseline_artifact_existence_is_not_source_provenance(cli):
    plan = release_plan(families=["upack"], repositories=["oss"])
    code, report, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert report["inventory"]["inventory_complete"]
    assert not report["inventory"]["complete"]
    assert not report["complete"]
    assert action(saved(cli), "oss", "upack")["status"] == "existing"
    assert action(saved(cli), "oss", "upack")["receipt"] is None
    assert not cli.remote.queued


def test_driver_inventory_uses_the_shared_evidence_report(cli):
    plan = release_plan(families=["upack"], repositories=["oss"])
    code, report, error = cli("preflight", plan=plan)
    assert code == 0, error
    verify.validate_inventory(plan, report["inventory"])
    with pytest.raises(ValueError, match="inventory alone"):
        verify.validate_evidence(plan, report["inventory"])
    assert report["inventory"]["repositories"] == ["oss"]
    assert report["inventory"]["families"] == ["upack"]
    assert report["inventory"]["coverage"]["skipped"] == 0
    assert not report["complete"], "Inventory alone is not a build provenance receipt"
    assert not cli.remote.queued


def test_only_validated_receipt_fields_are_persisted(cli):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "oss", ["upack"])
    cli.remote.manifests[101][0]["unrelated_diagnostic"] = "must-not-store"
    cli.remote.manifests[101][0]["artifacts"][0][
        "unrelated_diagnostic"
    ] = "must-not-store"
    assert cli("status", plan=plan)[0] == 0
    assert "must-not-store" not in cli.state.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "corruption",
    [
        "plan",
        "commit",
        "target",
        "version",
        "hash",
        "missing",
        "build",
        "family",
        "mode",
        "scope",
        "publisher-source",
        "feed",
        "project",
        "output",
    ],
)
def test_invalid_provenance_never_reports_complete_or_requeues(cli, corruption):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "oss", ["upack"])
    manifest = cli.remote.manifests[101][0]
    if corruption == "plan":
        manifest["plan_id"] = "e" * 64
    elif corruption == "commit":
        manifest["source_commit"] = "e" * 40
    elif corruption == "target":
        manifest["target"] = "spark4.0"
    elif corruption == "version":
        manifest["version"] = "9.9.9"
    elif corruption == "hash":
        manifest["artifacts"][0]["sha256"] = "not-a-hash"
    elif corruption == "missing":
        cli.remote.manifests[101] = []
    elif corruption == "build":
        manifest["build_id"] = 999
    elif corruption == "mode":
        manifest["mode"] = "rehearsal"
    elif corruption == "scope":
        manifest["scope"] = "internal-only"
    elif corruption == "publisher-source":
        manifest["publisher_source_commit"] = "e" * 40
    elif corruption == "feed":
        manifest["destination"]["feed_id"] = TEST_UPACK_ID
    elif corruption == "project":
        manifest["destination"]["project_id"] = TEST_UPACK_ID
    elif corruption == "output":
        manifest["outputs"][0]["version"] = "9.9.9"
    else:
        manifest["families"] = ["pip"]
    code, report, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert report["complete"] is False
    assert len(cli.remote.queued) == 1
    assert action(saved(cli), "oss", "upack")["status"] != "complete"


@pytest.mark.parametrize(
    "corruption",
    ["id", "definition", "source", "parameters", "status", "variables", "repository"],
)
def test_real_build_response_validation_is_fail_closed(cli, corruption):
    plan = release_plan(families=["maven"], repositories=["oss"])
    cli.remote.missing = {("oss", "maven")}
    assert cli(plan=plan, apply=True)[0] == 1
    build = cli.remote.builds[101]
    if corruption == "id":
        build["id"] = True
    elif corruption == "definition":
        build["definition"]["id"] = 18453
    elif corruption == "source":
        build["sourceVersion"] = "e" * 40
    elif corruption == "parameters":
        build["templateParameters"]["release_plan_id"] = "e" * 64
    elif corruption == "variables":
        build["parameters"] = "{}"
    elif corruption == "repository":
        build["repository"]["id"] = "different"
    else:
        build["status"] = "made-up"
    code, report, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert not report["complete"]
    assert action(saved(cli), "oss", "maven")["status"] == "unknown"
    assert len(cli.remote.queued) == 1


@pytest.mark.parametrize("bad_response", [{}, [], {"id": True}, {"id": "101"}])
def test_invalid_queue_responses_preserve_unknown_intent(cli, bad_response):
    plan = release_plan(families=["pip"], repositories=["oss"])
    cli.remote.missing = {("oss", "pip")}
    cli.remote.queue_response = bad_response
    assert cli(plan=plan, apply=True)[0] == 1
    assert action(saved(cli), "oss", "pip")["status"] == "unknown"
    assert cli(plan=plan, apply=True)[0] == 1
    assert len(cli.remote.queued) == 1


def test_adoption_reconciles_ambiguous_submission_without_another_queue(cli):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}
    cli.remote.queue_error = RuntimeError("lost queue response")
    assert cli(plan=plan, apply=True)[0] == 1
    item = action(saved(cli), "oss", "upack")
    cli.remote.register(item["command"], 712)
    cli.remote.succeed(712, plan, "oss", ["upack"])
    code, report, error = cli(
        plan=plan, apply=True, extra=["--adopt", item["id"] + "=712"]
    )
    assert code == 0, error
    assert report["complete"]
    assert action(saved(cli), "oss", "upack")["build_id"] == 712
    assert len(cli.remote.queued) == 1


def test_adoption_of_existing_artifact_validates_matching_run_and_plan(cli):
    plan = release_plan(families=["pip"], repositories=["oss"])
    assert cli(plan=plan)[0] == 1
    item = action(saved(cli), "oss", "pip")
    cli.remote.register(item["command"], 713)
    cli.remote.succeed(713, plan, "oss", ["pip"])
    cli.remote.builds[713]["templateParameters"]["release_plan_id"] = "e" * 64
    code, _, error = cli(plan=plan, apply=True, extra=["--adopt", item["id"] + "=713"])
    assert code == 2
    assert "param" in error.lower() or "plan" in error.lower()
    assert action(saved(cli), "oss", "pip")["build_id"] is None
    assert not cli.remote.queued


def test_conflicting_adoptions_of_batched_runs_are_rejected_atomically(cli):
    plan = release_plan(families=["pip", "upack"], repositories=["oss"])
    assert cli(plan=plan)[0] == 1
    first = action(saved(cli), "oss", "pip")
    second = action(saved(cli), "oss", "upack")
    command = ops._operation(plan, first, ["pip", "upack"])["command"]
    cli.remote.register(command, 712)
    cli.remote.register(command, 713)
    original = cli.state.read_bytes()
    code, _, error = cli(
        plan=plan,
        apply=True,
        extra=["--adopt", first["id"] + "=712", "--adopt", second["id"] + "=713"],
    )
    assert code == 2
    assert "conflict" in error.lower()
    assert cli.state.read_bytes() == original
    assert not cli.remote.queued


def test_concurrent_lock_and_corrupt_state_are_rejected_without_queue(cli):
    plan = release_plan(families=["pip"], repositories=["oss"])
    lock = cli.state.with_name(cli.state.name + ".lock")
    lock.write_text("owned by another process", encoding="utf-8")
    code, _, error = cli(plan=plan, apply=True)
    assert code == 2
    assert "lock" in error.lower()
    assert lock.read_text(encoding="utf-8") == "owned by another process"
    lock.unlink()
    cli.state.write_text("{broken json", encoding="utf-8")
    code, _, error = cli(plan=plan, apply=True)
    assert code == 2
    assert "state" in error.lower()
    assert cli.state.read_text(encoding="utf-8") == "{broken json"
    assert not cli.remote.queued


def test_state_bound_to_a_different_plan_is_not_overwritten(cli):
    first = release_plan(families=["pip"], repositories=["oss"])
    assert cli(plan=first)[0] == 1
    original = cli.state.read_bytes()
    second = release_plan(families=["upack"], repositories=["oss"])
    code, _, error = cli(plan=second, apply=True)
    assert code == 2
    assert "plan" in error.lower()
    assert cli.state.read_bytes() == original
    assert not cli.remote.queued


@pytest.mark.parametrize(
    "corruption",
    [
        "command",
        "source",
        "run",
        "receipt",
        "action",
        "status",
        "outcome",
        "status-type",
        "outcome-status-type",
        "destination-type",
    ],
)
def test_even_rehashed_state_cannot_change_approved_operations(cli, corruption):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}
    assert cli(plan=plan, apply=True)[0] == 1
    state = saved(cli)
    item = action(state, "oss", "upack")
    if corruption == "command":
        item["command"].append("--branch=unapproved")
    elif corruption == "source":
        item["source_commit"] = "e" * 40
    elif corruption == "run":
        item["build_id"] = True
    elif corruption == "receipt":
        item["receipt"] = {"plan_id": plan.plan_id}
    elif corruption == "action":
        state["actions"].append(copy.deepcopy(item))
    elif corruption == "outcome":
        item["outcome"] = "not an Azure outcome"
    elif corruption == "status-type":
        item["status"] = []
    elif corruption == "outcome-status-type":
        item["outcome"]["status"] = []
    elif corruption == "destination-type":
        state["destinations"] = []
    else:
        item["status"] = "complete"
    state["state_id"] = ops._digest(state, "state_id")
    cli.state.write_text(json.dumps(state), encoding="utf-8")
    original = cli.state.read_bytes()
    assert cli(plan=plan, apply=True)[0] == 2
    assert cli.state.read_bytes() == original
    assert len(cli.remote.queued) == 1


def test_stale_conflicting_state_is_not_overwritten(cli, monkeypatch):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}
    assert cli(plan=plan)[0] == 1
    original = cli.remote.inventory

    def conflict(value):
        result = original(value)
        cli.state.write_text('{"modified": true}', encoding="utf-8")
        return result

    monkeypatch.setattr(cli.remote, "inventory", conflict)
    code, _, error = cli(plan=plan, apply=True)
    assert code == 2
    assert "chang" in error.lower() or "conflict" in error.lower()
    assert cli.state.read_text(encoding="utf-8") == '{"modified": true}'
    assert not cli.remote.queued


@pytest.mark.parametrize(
    "fault", ["alias", "id", "error", "invalid", "project", "same-feed"]
)
def test_rehearsal_feed_guard_resolves_names_and_ids_fail_closed(cli, fault):
    plan = release_plan(
        families=["upack"],
        repositories=["oss"],
        mode="rehearsal",
        pip_feed="release-test-pip",
        upack_feed="release-test-upack",
    )
    cli.remote.missing = {("oss", "upack")}
    if fault == "alias":
        cli.remote.feeds["release-test-upack"]["name"] = matrix.UPACK_FEED
    elif fault == "id":
        cli.remote.feeds["release-test-upack"]["id"] = PROD_UPACK_ID
    elif fault == "error":
        cli.remote.feeds["release-test-upack"] = RuntimeError("feed denied")
    elif fault == "project":
        cli.remote.feeds["release-test-upack"]["project"]["id"] = TEST_UPACK_ID
    elif fault == "same-feed":
        cli.remote.feeds["release-test-upack"]["id"] = TEST_PIP_ID
    else:
        cli.remote.feeds["release-test-upack"] = {"name": "release-test-upack"}
    code, _, error = cli(plan=plan, apply=True)
    assert code == 2
    assert "feed" in error.lower()
    assert not cli.remote.queued


def test_rehearsal_only_uses_selected_nonproduction_destinations(cli):
    plan = release_plan(
        families=["upack"],
        repositories=["internal"],
        scope="internal-only",
        internal_patch="2",
        mode="rehearsal",
        pip_feed="release-test-pip",
        upack_feed="release-test-upack",
    )
    cli.remote.missing = {("internal", "upack")}
    code, report, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert len(cli.remote.queued) == 1
    assert report["destinations"]["upack"]["id"] == TEST_UPACK_ID
    assert boolean_parameters(command_values(cli.remote.queued[0], "--parameters")) == {
        "build_internal_upack_default"
    }


def test_selected_target_does_not_expand_to_other_targets(cli):
    plan = release_plan(
        target_keys=["spark4.0"],
        oss_commits={"spark4.0": OSS_SHA},
        internal_commits={"spark4.0": INTERNAL_SHA},
        repositories=["internal"],
        scope="internal-only",
        internal_patch="2",
        families=["upack"],
    )
    cli.remote.missing = {("internal", "upack")}
    assert cli(plan=plan, apply=True)[0] == 1
    assert boolean_parameters(command_values(cli.remote.queued[0], "--parameters")) == {
        "build_internal_upack_spark4"
    }
    assert {item["target"] for item in saved(cli)["actions"]} == {"spark4.0"}


def test_full_release_policy_is_rechecked_immediately_before_submission(
    cli, monkeypatch
):
    cli.remote.missing = {("oss", "upack")}
    plan = release_plan(families=["upack"], repositories=["oss"])
    real_inventory = cli.remote.inventory

    def change_policy(value):
        result = real_inventory(value)
        cli.remote.variables = {
            "total_count": 1,
            "variables": [{"name": "SKIP_SPARK40", "value": "true"}],
        }
        return result

    monkeypatch.setattr(cli.remote, "inventory", change_policy)
    code, _, error = cli(plan=plan, apply=True)
    assert code == 2
    assert "SKIP_SPARK40" in error
    assert not cli.remote.queued


def test_all_skipped_azure_jobs_cannot_prove_completion(cli):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "oss", ["upack"])
    cli.remote.timelines[101]["records"][0]["result"] = "skipped"
    assert cli("status", plan=plan)[0] == 1
    assert action(saved(cli), "oss", "upack")["status"] == "unknown"
    assert len(cli.remote.queued) == 1


def test_partial_artifact_visibility_keeps_succeeded_build_pending(cli):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "oss", ["upack"])
    cli.remote.missing.add(("oss", "upack"))
    assert cli(plan=plan, apply=True)[0] == 1
    assert action(saved(cli), "oss", "upack")["status"] == "pending"
    assert action(saved(cli), "oss", "upack")["receipt"]
    cli.remote.missing.clear()
    assert cli(plan=plan, apply=True)[0] == 0
    assert len(cli.remote.queued) == 1


@pytest.mark.parametrize("corruption", ["missing", "duplicate", "skip", "complete"])
def test_invalid_verifier_coverage_fails_before_queue(cli, monkeypatch, corruption):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}
    real_inventory = cli.remote.inventory

    def incomplete(value):
        rows, complete = real_inventory(value)
        if corruption == "missing":
            rows.pop()
        elif corruption == "duplicate":
            rows.append(copy.deepcopy(rows[0]))
        elif corruption == "skip":
            rows[-1]["status"] = verify.SKIPPED
        else:
            complete = not complete
        return rows, complete

    monkeypatch.setattr(cli.remote, "inventory", incomplete)
    assert cli(plan=plan, apply=True)[0] == 2
    assert not cli.remote.queued


def test_temp_git_tags_supply_the_exact_commits_used_by_cli(cli, tmp_path, monkeypatch):
    repo = tmp_path / "source"
    repo.mkdir()

    def git(*arguments):
        result = subprocess.run(
            ["git", "-C", str(repo), *arguments],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    git("init", "--quiet")
    git(
        "-c",
        "user.name=Release fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "commit",
        "--allow-empty",
        "--quiet",
        "-m",
        "Local release fixture",
    )
    commit = git("rev-parse", "HEAD")
    plan = release_plan(
        families=["upack"], repositories=["oss"], oss_commits={"master": commit}
    )
    for tag in plan.targets[0].oss_tags:
        git("tag", tag, commit)
    monkeypatch.setattr(
        InventoryChecker,
        "github_tag",
        lambda _checker, tag: (
            verify.OK,
            git("rev-parse", f"refs/tags/{tag}^{{commit}}"),
        ),
    )
    cli.remote.missing = {("oss", "upack")}
    assert cli(plan=plan, apply=True)[0] == 1
    item = action(saved(cli), "oss", "upack")
    assert item["source_commit"] == commit
    payload = command_values(cli.remote.queued[0], "--parameters")[
        "release_plan_base64"
    ]
    assert json.loads(base64.b64decode(payload))["targets"][0]["oss_commit"] == commit


def test_real_cli_entrypoint_rejects_missing_approval_without_service_access(tmp_path):
    plan = release_plan(families=["upack"], repositories=["oss"])
    plan_path = tmp_path / "plan.json"
    state_path = tmp_path / "state.json"
    plan_path.write_text(json.dumps(matrix.plan_to_dict(plan)), encoding="utf-8")
    environment = {
        key: value
        for key, value in os.environ.items()
        if key not in {"ADO_TOKEN", "GH_TOKEN", "GITHUB_TOKEN"}
    }
    environment["PATH"] = ""
    result = subprocess.run(
        [
            sys.executable,
            str(Path(ops.__file__)),
            "resume",
            "--plan",
            str(plan_path),
            "--state",
            str(state_path),
            "--apply",
        ],
        capture_output=True,
        text=True,
        env=environment,
        check=False,
        timeout=30,
    )
    assert result.returncode == 2
    assert "Approval" in result.stderr
    assert not result.stdout
    assert not state_path.exists()


def test_subprocess_runner_never_invokes_cmd_shell_or_leaks_stderr(
    monkeypatch, tmp_path
):
    az = tmp_path / "CLI2" / "wbin" / "az.cmd"
    az.parent.mkdir(parents=True)
    az.write_text("not executed", encoding="utf-8")
    python = az.parent.parent / "python.exe"
    python.write_text("not executed", encoding="utf-8")
    monkeypatch.setattr(ops.shutil, "which", lambda _name: str(az))
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, '{"id": 42}', "")

    monkeypatch.setattr(ops.subprocess, "run", run)
    value = ops.CommandRunner().json(["az", "pipelines", "run", "--id", "35879"])
    assert value == {"id": 42}
    assert calls[0][0][:4] == [str(python), "-I", "-m", "azure.cli"]
    assert isinstance(calls[0][0], list)
    assert calls[0][1].get("shell") is False

    def fail(command, **kwargs):
        return subprocess.CompletedProcess(
            command, 1, "", "Bearer do-not-print-this-secret"
        )

    monkeypatch.setattr(ops.subprocess, "run", fail)
    with pytest.raises(RuntimeError) as error:
        ops.CommandRunner().json(["az", "pipelines", "run"])
    assert "do-not-print" not in str(error.value)


def test_manifest_zip_is_read_without_extracting_files():
    content = {"schema_version": 1, "plan_id": "a" * 64}
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(
            "release-provenance/release-provenance.json", json.dumps(content)
        )
    assert ops.read_provenance_zip(stream.getvalue()) == [content]
    with pytest.raises(RuntimeError, match="artifact|ZIP"):
        ops.read_provenance_zip(b"this is not an artifact")


@pytest.mark.parametrize("body", [b"not json", b"{}", b'{"value": false}'])
def test_azure_provenance_api_rejects_real_invalid_http_responses(body):
    class Response(io.BytesIO):
        pass

    class Opener:
        def open(self, _request, timeout):
            return Response(body)

    remote = ops.AzureRemote()
    remote._token = "test-only-token"
    remote._opener = Opener()
    with pytest.raises(RuntimeError):
        remote.provenance(712)


def test_azure_http_error_never_prints_signed_url_or_token():
    class Opener:
        def open(self, request, timeout):
            raise urllib.error.HTTPError(
                "https://example.invalid?sig=do-not-leak",
                403,
                "Bearer do-not-leak",
                {},
                None,
            )

    remote = ops.AzureRemote()
    remote._token = "test-only-token"
    remote._opener = Opener()
    with pytest.raises(RuntimeError) as error:
        remote.build(712)
    assert "403" in str(error.value)
    assert "do-not-leak" not in str(error.value)


def test_artifact_redirect_drops_auth_and_rejects_non_azure_hosts():
    request = urllib.request.Request(
        "https://dev.azure.com/msdata/artifact",
        headers={"Authorization": "Bearer test-only-token"},
    )
    redirect = ops._AzureRedirects()
    safe = redirect.redirect_request(
        request,
        None,
        302,
        "Found",
        {},
        "https://account.blob.core.windows.net/artifact?sig=not-a-real-signature",
    )
    assert not safe.has_header("Authorization")
    with pytest.raises(RuntimeError, match="destination"):
        redirect.redirect_request(
            request, None, 302, "Found", {}, "https://example.invalid/artifact"
        )


@pytest.mark.parametrize(
    "artifact_name",
    [
        "drop_SynapseML_Upack_spark4_Build_spark4",
        "release-provenance-SynapseML_Upack_spark4-Build_spark4",
        "Publisher output (selected job)",
    ],
)
def test_pipeline_artifact_reads_only_receipt_not_large_onebranch_drop(artifact_name):
    manifest_id = "A" * 64 + "01"
    receipt_id = "B" * 64 + "01"
    jar_id = "C" * 64 + "01"
    receipt = {"schema_version": 1, "plan_id": "d" * 64}
    receipt_bytes = ops.canonical(receipt)
    requests = []

    class Opener:
        def open(self, request, timeout):
            query = dict(
                urllib.parse.parse_qsl(urllib.parse.urlsplit(request.full_url).query)
            )
            requests.append(query)
            file_id = query.get("fileId")
            if file_id is None:
                data = {
                    "count": 1,
                    "value": [
                        {
                            "name": artifact_name,
                            "resource": {
                                "type": "PipelineArtifact",
                                "data": manifest_id,
                            },
                        }
                    ],
                }
                return io.BytesIO(ops.canonical(data))
            assert query["artifactName"] == artifact_name
            if file_id == manifest_id:
                return io.BytesIO(
                    ops.canonical(
                        {
                            "manifestFormat": "1.1.0",
                            "items": [
                                {
                                    "path": "/jars/huge.jar",
                                    "blob": {"id": jar_id, "size": 2**30},
                                },
                                {
                                    "path": "/tests/release-provenance.json",
                                    "blob": {"id": jar_id, "size": 100},
                                },
                                {
                                    "path": "/release-provenance.json",
                                    "blob": {
                                        "id": receipt_id,
                                        "size": len(receipt_bytes),
                                    },
                                },
                            ],
                            "manifestReferences": [],
                        }
                    )
                )
            assert file_id == receipt_id, "Must not download the full drop or its jars"
            assert query["fileName"] == "release-provenance.json"
            return io.BytesIO(receipt_bytes)

    remote = ops.AzureRemote()
    remote._token = "test-only-token"
    remote._opener = Opener()
    assert remote.provenance(712) == [receipt]
    assert len(requests) == 3


@pytest.mark.parametrize(
    "artifact_name",
    [
        "drop_SynapseML_Upack_spark4_Build_spark4_failed_1",
        "release-provenance-SynapseML_Upack_spark4-Build_spark4_failed_2",
        "drop_SynapseML_Upack_spark4_Build_spark4_sdl_analysis",
        "drop_SynapseML_Upack_spark4_Build_spark4_signingLogs",
        "drop_SynapseML_Upack_spark4_Build_spark4_signingLogs_1",
        "1espt-autobaseline-123",
        "drop_ReleasePolicy_Validate",
    ],
)
def test_auxiliary_artifact_receipts_are_not_read(monkeypatch, artifact_name):
    remote = ops.AzureRemote()
    calls = []

    def get(url, **_kwargs):
        calls.append(url)
        assert len(calls) == 1, "Auxiliary artifacts must not be downloaded"
        return {
            "value": [
                {
                    "name": artifact_name,
                    "resource": {"type": "PipelineArtifact", "data": "not-primary"},
                }
            ]
        }

    monkeypatch.setattr(remote, "_get", get)
    assert remote.provenance(712) == []
    assert len(calls) == 1


def test_compact_publisher_receipt_is_preferred_to_its_payload_drops(monkeypatch):
    remote = ops.AzureRemote()
    primary = "release-provenance-SynapseML_Upack_spark4-Build_spark4"
    names = [
        "drop_SynapseML_Upack_spark4_Build_spark4",
        "drop_SynapseML_Upack_spark4_Receipt_Build_spark4",
        primary,
    ]
    receipt = {"schema_version": 1, "plan_id": "d" * 64}
    fetched = []
    monkeypatch.setattr(
        remote,
        "_get",
        lambda *_args, **_kwargs: {
            "value": [
                {"name": name, "resource": {"type": "PipelineArtifact"}}
                for name in names
            ]
        },
    )

    def read(_build_id, artifact):
        fetched.append(artifact["name"])
        return [receipt]

    monkeypatch.setattr(remote, "_pipeline_provenance", read)
    assert remote.provenance(712) == [receipt]
    assert fetched == [primary]


@pytest.mark.parametrize("conflicting", [False, True])
def test_primary_receipt_copies_deduplicate_but_conflicts_do_not_approve(
    cli, monkeypatch, conflicting
):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "oss", ["upack"])
    first = copy.deepcopy(cli.remote.manifests[101][0])
    second = copy.deepcopy(first)
    if conflicting:
        second["artifacts"][0]["sha256"] = "f" * 64
    names = ["drop_SynapseML_Upack_default_Build_default", "Producer receipt archive"]
    transport = ops.AzureRemote()
    monkeypatch.setattr(
        transport,
        "_get",
        lambda *_args, **_kwargs: {
            "value": [
                {"name": name, "resource": {"type": "PipelineArtifact"}}
                for name in names
            ]
        },
    )
    monkeypatch.setattr(
        transport,
        "_pipeline_provenance",
        lambda _build_id, artifact: [first if artifact["name"] == names[0] else second],
    )
    monkeypatch.setattr(cli.remote, "provenance", transport.provenance)
    code, report, error = cli("status", plan=plan)
    assert code == (1 if conflicting else 0), error
    assert report["complete"] is not conflicting
    item = action(saved(cli), "oss", "upack")
    assert item["status"] == ("unknown" if conflicting else "complete")
    if not conflicting:
        assert item["receipt"]["provenance"] == [first]
    assert cli(plan=plan, apply=True)[0] == (1 if conflicting else 0)
    assert len(cli.remote.queued) == 1


def test_unrelated_container_artifact_is_not_downloaded(monkeypatch):
    remote = ops.AzureRemote()
    calls = []

    def get(url, **_kwargs):
        calls.append(url)
        return {
            "value": [
                {
                    "name": "unit-test-logs",
                    "resource": {
                        "type": "Container",
                        "downloadUrl": "https://example.invalid/do-not-download",
                    },
                }
            ]
        }

    monkeypatch.setattr(remote, "_get", get)
    assert remote.provenance(712) == []
    assert len(calls) == 1


@pytest.mark.parametrize("repository,family", [("oss", "upack"), ("internal", "maven")])
def test_verified_evidence_revalidates_live_runs_without_queueing(
    cli, monkeypatch, capsys, repository, family
):
    plan = release_plan(
        families=[family],
        repositories=[repository],
        scope="internal-only" if repository == "internal" else "full",
        internal_patch="1" if repository == "internal" else "0",
    )
    cli.remote.missing = {(repository, family)}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, repository, [family])
    before = len(cli.remote.build_calls)
    report = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
    assert report["complete"]
    assert report["inventory_complete"]
    assert report["evidence_kind"] == "producer-verified"
    assert len(cli.remote.build_calls) > before
    assert len(cli.remote.queued) == 1
    assert report["producer_evidence"]["runs"][0]["build"]["id"] == 101

    def forbidden(*_args, **_kwargs):
        raise AssertionError(
            "Offline evidence validation must not authenticate or query"
        )

    monkeypatch.setattr(ops, "AzureRemote", forbidden)
    verify.validate_evidence(plan, report)
    ops.validate_producer_evidence(plan, report)


def test_inventory_cannot_be_promoted_by_setting_complete(cli):
    plan = release_plan(families=["upack"], repositories=["oss"])
    assert cli(plan=plan)[0] == 1
    report = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    assert not report["complete"]
    report["complete"] = True
    report["evidence_kind"] = "producer-verified"
    with pytest.raises((ValueError, RuntimeError)):
        verify.validate_evidence(plan, report)
    assert not cli.remote.queued


def test_public_notes_export_fits_github_and_passes_the_real_guard(
    cli, monkeypatch, capsys
):
    import release_guard

    keys = [target.key for target in matrix.TARGETS]
    plan = release_plan(
        target_keys=keys,
        oss_commits={key: OSS_SHA for key in keys},
        repositories=["oss"],
        families=["maven"],
    )
    assert all(target.internal_commit is None for target in plan.targets)
    cli.remote.missing = {("oss", "maven")}
    assert cli(plan=plan, apply=True)[0] == 1
    for current in saved(cli)["actions"]:
        build_id = current["build_id"]
        cli.remote.succeed(build_id, plan, "oss", ["maven"], target=current["target"])
        cli.remote.manifests[build_id][0]["artifacts"].extend(
            {
                "path": f"module-{index}/artifact.jar",
                "sha256": hashlib.sha256(f"{build_id}-{index}".encode()).hexdigest(),
                "size": index + 1,
            }
            for index in range(140)
        )
    monkeypatch.setattr(ops, "AzureRemote", lambda: cli.remote)
    assert (
        verify.main(
            [
                "--plan",
                str(cli.plan),
                "--state",
                str(cli.state),
                "--github-evidence",
            ]
        )
        == 0
    )
    encoded = capsys.readouterr().out.strip()
    assert len(encoded) <= verify.MAX_GITHUB_EVIDENCE_CHARS
    report = verify.decode_evidence(encoded)
    assert 65535 < len(ops.canonical(report)) <= verify.MAX_EVIDENCE_BYTES
    verify.validate_evidence(plan, report)
    for run in report["producer_evidence"]["runs"]:
        sealed = json.loads(
            base64.b64decode(run["operation"]["parameters"]["release_plan_base64"])
        )
        assert all(target["internal_commit"] is None for target in sealed["targets"])
    payload = {
        "ref": "v1.1.4",
        "inputs": {
            "plan_json": cli.plan.read_text(encoding="utf-8"),
            "evidence_base64": encoded,
            "approve_plan": plan.plan_id,
        },
    }
    assert len(json.dumps(payload)) < 65535
    monkeypatch.setenv("RELEASE_EVIDENCE_BASE64", encoded)
    assert (
        release_guard.main(
            [
                "notes",
                "--plan",
                str(cli.plan),
                "--evidence-base64-env",
                "--approve-plan",
                plan.plan_id,
                "--tag",
                "v1.1.4",
                "--commit",
                OSS_SHA,
            ]
        )
        == 0
    )
    assert len(cli.remote.queued) == len(keys)


def test_batched_families_export_one_run_with_exact_action_coverage(cli):
    plan = release_plan(
        scope="internal-only",
        internal_patch="1",
        repositories=["internal"],
        families=["pip", "upack"],
    )
    cli.remote.missing = {("internal", "pip"), ("internal", "upack")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "internal", ["pip", "upack"])
    report = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    verify.validate_evidence(plan, report)
    runs = report["producer_evidence"]["runs"]
    assert len(runs) == 1
    assert set(runs[0]["action_ids"]) == {
        "publisher.internal.master.pip",
        "publisher.internal.master.upack",
    }
    assert len(runs[0]["provenance"]) == 2
    incomplete = copy.deepcopy(report)
    incomplete["producer_evidence"]["runs"][0]["action_ids"].pop()
    with pytest.raises((ValueError, RuntimeError)):
        verify.validate_evidence(plan, incomplete)
    assert len(cli.remote.queued) == 1


def test_export_does_not_copy_service_credentials_or_signed_urls(cli):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "oss", ["upack"])
    build = cli.remote.builds[101]
    build["_links"] = {
        "artifact": {"href": "https://example.invalid?sig=must-not-export"}
    }
    build["variables"] = {
        "unrelated_secret": {"value": "must-not-export", "isSecret": True}
    }
    build["unrelated_diagnostic"] = "must-not-export"
    report = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    verify.validate_evidence(plan, report)
    assert "must-not-export" not in json.dumps(report)
    assert "_links" not in report["producer_evidence"]["runs"][0]["build"]


@pytest.mark.parametrize(
    "corruption",
    [
        "coverage",
        "duplicate",
        "parameters",
        "variables",
        "source",
        "result",
        "jobs",
        "receipt",
        "hash",
        "destination",
        "time",
        "extra",
    ],
)
def test_pure_producer_evidence_rejects_incomplete_or_forged_details(cli, corruption):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "oss", ["upack"])
    report = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    evidence = report["producer_evidence"]
    run = evidence["runs"][0]
    if corruption == "coverage":
        evidence["runs"] = []
    elif corruption == "duplicate":
        evidence["runs"].append(copy.deepcopy(run))
    elif corruption == "parameters":
        run["build"]["templateParameters"]["build_internal_upack_default"] = True
    elif corruption == "variables":
        run["build"]["parameters"] = "{}"
    elif corruption == "source":
        run["build"]["sourceVersion"] = "e" * 40
    elif corruption == "result":
        run["build"]["result"] = "failed"
    elif corruption == "jobs":
        run["jobs"][0]["result"] = "skipped"
    elif corruption == "receipt":
        run["provenance"] = []
    elif corruption == "hash":
        run["provenance"][0]["artifacts"][0]["sha256"] = "bad"
    elif corruption == "destination":
        evidence["destinations"]["upack"]["id"] = TEST_UPACK_ID
    elif corruption == "time":
        report["checked_at"] = "2000-01-01T00:00:00+00:00"
    else:
        run["build"]["unrelated_diagnostic"] = "must-not-export"
    with pytest.raises((ValueError, RuntimeError)):
        verify.validate_evidence(plan, report)
    assert len(cli.remote.queued) == 1


def test_internal_maven_without_producer_receipt_cannot_approve(cli):
    plan = release_plan(
        scope="internal-only",
        internal_patch="1",
        repositories=["internal"],
        families=["maven"],
    )
    cli.remote.missing = {("internal", "maven")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "internal", ["maven"])
    cli.remote.manifests[101] = []
    report = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    assert report["inventory_complete"]
    assert not report["complete"]
    assert action(saved(cli), "internal", "maven")["status"] == "unknown"
    code, resumed, error = cli(plan=plan, apply=True)
    assert code == 1, error
    assert not resumed["complete"]
    item = action(saved(cli), "internal", "maven")
    assert item["status"] == "unknown"
    assert item["build_id"] == 101
    assert item["receipt"] is None
    assert len(cli.remote.queued) == 1


def test_internal_maven_does_not_grandfather_empty_saved_provenance(cli):
    plan = release_plan(
        scope="internal-only",
        internal_patch="1",
        repositories=["internal"],
        families=["maven"],
    )
    cli.remote.missing = {("internal", "maven")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "internal", ["maven"])
    assert cli("status", plan=plan)[0] == 0
    state = saved(cli)
    action(state, "internal", "maven")["receipt"]["provenance"] = []
    state["state_id"] = ops._digest(state, "state_id")
    cli.state.write_text(json.dumps(state), encoding="utf-8")
    original = cli.state.read_bytes()
    with pytest.raises(ops.ReleaseError, match="release-provenance"):
        ops.verified_evidence(plan, cli.state, remote=cli.remote)
    code, _, error = cli(plan=plan, apply=True)
    assert code == 2
    assert "release-provenance" in error
    assert cli.state.read_bytes() == original
    assert len(cli.remote.queued) == 1


def test_verify_cli_consumes_driver_state_as_producer_evidence(
    cli, monkeypatch, capsys
):
    plan = release_plan(families=["upack"], repositories=["oss"])
    cli.remote.missing = {("oss", "upack")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "oss", ["upack"])
    monkeypatch.setattr(ops, "AzureRemote", lambda: cli.remote)
    assert (
        verify.main(["--plan", str(cli.plan), "--state", str(cli.state), "--json"]) == 0
    )
    report = json.loads(capsys.readouterr().out)
    verify.validate_evidence(plan, report)
    assert len(cli.remote.queued) == 1


@pytest.mark.parametrize("corruption", ["format", "items", "id", "size", "references"])
def test_pipeline_artifact_manifest_validation_fails_closed(monkeypatch, corruption):
    remote = ops.AzureRemote()
    artifact = {
        "name": "release-provenance",
        "resource": {"type": "PipelineArtifact", "data": "A" * 64 + "01"},
    }
    manifest = {
        "manifestFormat": "1.1.0",
        "items": [
            {
                "path": "/release-provenance.json",
                "blob": {"id": "B" * 64 + "01", "size": 100},
            }
        ],
        "manifestReferences": [],
    }
    if corruption == "format":
        manifest["manifestFormat"] = "999.0.0"
    elif corruption == "items":
        manifest["items"] = "bad"
    elif corruption == "id":
        manifest["items"][0]["blob"]["id"] = "not-an-id"
    elif corruption == "size":
        manifest["items"][0]["blob"]["size"] = ops.MAX_RECEIPT_BYTES + 1
    else:
        manifest["manifestReferences"] = ["unchecked nested manifest"]
    calls = []

    def get(url, **_kwargs):
        calls.append(url)
        return {"value": [artifact]} if len(calls) == 1 else manifest

    monkeypatch.setattr(remote, "_get", get)
    with pytest.raises(RuntimeError, match="manifest|artifact|provenance"):
        remote.provenance(712)
    assert len(calls) == 2
