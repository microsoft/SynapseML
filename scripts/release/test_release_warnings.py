# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import copy
import hashlib
import json
import os
from pathlib import Path
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import yaml

import release_matrix as matrix
import release_ops as ops
import verify_release as verify
from test_release_ops import cli, no_network  # noqa: F401
from test_release_ops import private_profile, release_plan  # noqa: F401

CACHE_ID = "d53ccab4-555e-4494-9d06-11db043fb4a9"
BASH_ID = "6c731c3c-3c68-459a-a5c9-bde6e6595b5b"
KEY_VAULT_ID = "1e244d32-2dd4-4165-96fb-b7441ca9331e"
AZURE_CLI_ID = "46e4be58-730b-4389-8a2f-ea10b3e5e815"


def identity(number):
    return f"{number:08x}-1111-4111-8111-111111111111"


def partial_build(cli, warning_job="Release", warning="cache", plan=None):
    plan = plan or matrix.build_plan(
        "1.2.0", target_keys=["master"], oss_commits={"master": "a" * 40}
    )
    cli.remote.missing = {("oss", "maven")}
    code, report, error = cli(plan=plan, apply=True)
    assert code == 1, error or report
    for build_id, target in enumerate(plan.targets, 101):
        cli.remote.succeed(build_id, plan, "oss", ["maven"], target=target.key)
    cli.remote.builds[101]["result"] = "partiallySucceeded"
    timing = {
        "startTime": cli.remote.builds[101]["queueTime"],
        "finishTime": cli.remote.builds[101]["finishTime"],
    }
    records = []
    for index, name in enumerate(("Publish", "UnitTests", "Release"), 1):
        records.extend(
            [
                {
                    "id": identity(index),
                    "type": "Job",
                    "name": name,
                    "state": "completed",
                    "result": (
                        "succeededWithIssues" if name == warning_job else "succeeded"
                    ),
                    "attempt": 1,
                },
                {
                    "id": identity(index + 10),
                    "parentId": identity(index),
                    "type": "Task",
                    "name": "synthetic-private-task-label",
                    "state": "completed",
                    "result": "succeeded",
                    "attempt": 1,
                    "task": {"id": AZURE_CLI_ID, "version": "2.279.1"},
                },
            ]
        )
    names = {
        "cache": ("Cache sbt launcher boot", CACHE_ID, "2.279.0"),
        "cache-save": ("Post-job: Cache sbt ivy dependencies", CACHE_ID, "2.279.0"),
        "codecov": ("Upload Coverage Report To Codecov.io", BASH_ID, "3.279.0"),
        "token": ("Load Codecov token", KEY_VAULT_ID, "2.280.2"),
    }
    name, task_id, version = names[warning]
    parent = next(r for r in records if r.get("name") == warning_job)
    task = {
        "id": identity(100),
        "parentId": parent["id"],
        "type": "Task",
        "name": name,
        "state": "completed",
        "result": "succeededWithIssues",
        "attempt": 1,
        "task": {"id": task_id, "version": version},
    }
    records.append(task)
    for index, name in enumerate(sorted(ops.REQUIRED_RELEASE_TASKS), 200):
        records.append(
            {
                "id": identity(index),
                "parentId": identity(3),
                "type": "Task",
                "name": name,
                "state": "completed",
                "result": "succeeded",
                "attempt": 1,
                "task": {"id": BASH_ID, "version": "3.279.0"},
            }
        )
    cli.remote.timelines[101] = {"records": records}
    for record in records:
        record.update(timing)
    return plan, records, task


@pytest.mark.parametrize(
    "job,warning",
    [
        ("UnitTests", "codecov"),
        ("UnitTests", "token"),
        ("Publish", "cache"),
        ("Release", "cache"),
        ("Release", "cache-save"),
    ],
)
@pytest.mark.parametrize("task_result", ["failed", "succeededWithIssues"])
@pytest.mark.parametrize("job_attempt", [1, 2])
def test_verified_nonpublishing_warnings_complete_without_republication(
    cli, job, warning, task_result, job_attempt
):
    plan, records, task = partial_build(cli, job, warning)
    next(record for record in records if record["id"] == task["parentId"])[
        "attempt"
    ] = job_attempt
    task["result"] = task_result
    for _ in range(2):
        code, report, error = cli("status", plan=plan)
        assert code == 0 and report["complete"], error or report
        assert report["actions"][0]["outcome"]["result"] == "partiallySucceeded"
    state = json.loads(cli.state.read_text())
    receipt = state["actions"][0]["receipt"]
    assert receipt["schema_version"] == 2
    assert receipt["result"] == "partiallySucceeded"
    evidence = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    assert evidence["producer_evidence"]["schema_version"] == 3
    assert evidence["complete"] and evidence["evidence_kind"] == "producer-verified"
    exported = verify.decode_evidence(verify.encode_evidence(evidence))
    ops.validate_producer_evidence(plan, exported)
    run = exported["producer_evidence"]["runs"][0]
    assert (
        run["job_records_sha256"]
        == hashlib.sha256(ops.canonical(receipt["jobs"])).hexdigest()
    )
    for raw, compact in zip(
        receipt["jobs"], exported["producer_evidence"]["runs"][0]["jobs"]
    ):
        assert "tasks" not in compact
        summary = compact["task_summary"]
        assert sum(summary["results"].values()) == len(raw["tasks"])
    assert "synthetic-private-task-label" not in json.dumps(exported)
    assert len(cli.remote.queued) == 1


@pytest.mark.parametrize(
    "fault",
    [
        "publication-failed",
        "unknown-warning",
        "wrong-task-id",
        "wrong-task-major",
        "no-task-reference",
        "no-warning-task",
        "no-successful-task",
        "missing-job-tasks",
        "orphan-task",
        "duplicate-task",
        "incomplete-task",
        "canceled-task",
        "stale-task",
        "failed-job",
        "unexplained-build",
        "publication-skipped",
        "publication-omitted",
        "publication-duplicated",
        "invalid-job-attempt",
        "invalid-task-attempt",
        "zero-task-id",
        "invalid-task-version",
        "non-cache-post-job",
        "missing-task-times",
        "reversed-task-times",
        "future-task",
        "missing-job-times",
    ],
)
def test_unapproved_or_incomplete_warning_proof_cannot_complete(cli, fault):
    plan, records, task = partial_build(cli)
    parent = next(r for r in records if r["id"] == task["parentId"])
    if fault == "publication-failed":
        records[5]["result"] = "failed"
    elif fault == "unknown-warning":
        task["name"] = "Publish release artifacts"
    elif fault == "wrong-task-id":
        task["task"]["id"] = AZURE_CLI_ID
    elif fault == "wrong-task-major":
        task["task"]["version"] = "3.279.0"
    elif fault == "no-task-reference":
        task["task"] = None
    elif fault == "no-warning-task":
        records.remove(task)
    elif fault == "no-successful-task":
        for record in records:
            if record.get("parentId") == identity(3) and record != task:
                record["result"] = "skipped"
    elif fault == "missing-job-tasks":
        records.pop(1)
    elif fault == "orphan-task":
        task["parentId"] = identity(999)
    elif fault == "duplicate-task":
        records.append(copy.deepcopy(task))
    elif fault == "incomplete-task":
        task["state"] = "inProgress"
    elif fault == "canceled-task":
        task["result"] = "canceled"
    elif fault == "stale-task":
        parent["attempt"] = 2
        task["startTime"] = task["finishTime"] = "2020-01-01T00:00:00Z"
    elif fault == "failed-job":
        parent["result"] = "failed"
    elif fault == "unexplained-build":
        parent["result"] = task["result"] = "succeeded"
    elif fault == "publication-skipped":
        records[-1]["result"] = "skipped"
    elif fault == "publication-omitted":
        records.pop()
    elif fault == "publication-duplicated":
        duplicate = copy.deepcopy(records[-1])
        duplicate["id"] = identity(900)
        records.append(duplicate)
    elif fault == "invalid-job-attempt":
        parent["attempt"] = True
    elif fault == "invalid-task-attempt":
        task["attempt"] = True
    elif fault == "zero-task-id":
        task["id"] = "00000000-0000-0000-0000-000000000000"
    elif fault == "invalid-task-version":
        task["task"]["version"] = ["2", "279", "0"]
    elif fault == "non-cache-post-job":
        task["name"] = "Post-job: Upload Coverage Report To Codecov.io"
        task["task"] = {"id": BASH_ID, "version": "3.279.0"}
    elif fault == "missing-task-times":
        task["startTime"] = None
    elif fault == "reversed-task-times":
        task["startTime"] = "2099-01-01T00:00:00Z"
    elif fault == "future-task":
        task["startTime"] = task["finishTime"] = "2099-01-01T00:00:00Z"
    elif fault == "missing-job-times":
        parent["startTime"] = parent["finishTime"] = None
    code, report, _ = cli("status", plan=plan)
    assert code == 1 and not report["complete"]
    assert not ops.verified_evidence(plan, cli.state, remote=cli.remote)["complete"]
    assert len(cli.remote.queued) == 1


@pytest.mark.parametrize(
    "fault",
    [
        "downgrade-evidence",
        "downgrade-build",
        "remove-tasks",
        "remove-warning",
        "extra-task-field",
        "private-task-name",
        "publication-failed",
        "duplicate-job-id",
        "duplicate-warning-group",
        "wrong-task-id",
        "wrong-task-major",
        "negative-count",
        "boolean-count",
        "unexplained-count",
        "no-required-success",
        "bad-digest",
        "missing-digest",
        "task-limit",
        "missing-window",
    ],
)
def test_exported_warning_evidence_rejects_tampering(cli, fault):
    plan, _, _ = partial_build(cli)
    evidence = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    assert evidence["complete"]
    producer = evidence["producer_evidence"]
    run = producer["runs"][0]
    release = next(j for j in run["jobs"] if j["name"] == "Release")
    summary = release["task_summary"]
    if fault == "downgrade-evidence":
        producer["schema_version"] = 1
    elif fault == "downgrade-build":
        run["build"]["result"] = "succeeded"
    elif fault == "remove-tasks":
        del release["task_summary"]
    elif fault == "remove-warning":
        summary["advisory_failures"] = []
    elif fault == "extra-task-field":
        summary["log"] = "unapproved metadata"
    elif fault == "private-task-name":
        summary["advisory_failures"][0]["name"] = "synthetic-private-task-label"
    elif fault == "publication-failed":
        summary["publications"].pop()
    elif fault == "duplicate-job-id":
        release["id"] = run["jobs"][0]["id"]
    elif fault == "duplicate-warning-group":
        summary["advisory_failures"].append(
            copy.deepcopy(summary["advisory_failures"][0])
        )
    elif fault == "wrong-task-id":
        summary["advisory_failures"][0]["task_id"] = AZURE_CLI_ID
    elif fault == "wrong-task-major":
        summary["advisory_failures"][0]["task_major"] = "2.0"
    elif fault == "negative-count":
        summary["results"]["failed"] = -1
    elif fault == "boolean-count":
        summary["results"]["failed"] = False
    elif fault == "unexplained-count":
        summary["results"]["failed"] += 1
    elif fault == "no-required-success":
        summary["non_advisory_succeeded"] = 0
    elif fault == "bad-digest":
        run["job_records_sha256"] = "unverified"
    elif fault == "missing-digest":
        del run["job_records_sha256"]
    elif fault == "task-limit":
        summary["results"]["skipped"] = ops.MAX_PUBLIC_TASKS
    elif fault == "missing-window":
        release["started_at"] = None
    with pytest.raises((ValueError, RuntimeError)):
        ops.validate_producer_evidence(plan, evidence)
    with pytest.raises(ValueError):
        verify.encode_evidence(evidence)


def test_old_failed_warning_state_can_be_reconciled_without_a_new_build(cli):
    plan, _, _ = partial_build(cli)
    with ops.StateStore(cli.state, plan, must_exist=True) as store:
        action = store.state["actions"][0]
        action["status"] = "failed"
        action["outcome"] = ops._validate_build(
            plan, action, cli.remote.builds[101], cli.remote
        )
        store.save()
    code, report, error = cli("status", plan=plan)
    assert code == 0 and report["complete"], error or report
    assert len(cli.remote.queued) == 1


def test_warning_task_bound_cannot_be_exceeded(cli, monkeypatch):
    plan, records, _ = partial_build(cli)
    count = sum(record["type"] == "Task" for record in records)
    monkeypatch.setattr(ops, "MAX_PUBLIC_TASKS", count)
    assert ops.verified_evidence(plan, cli.state, remote=cli.remote)["complete"]
    monkeypatch.setattr(ops, "MAX_PUBLIC_TASKS", count - 1)
    with pytest.raises(ops.ReleaseError, match="public task limit"):
        ops.verified_evidence(plan, cli.state, remote=cli.remote)


def test_clean_build_keeps_legacy_receipt_with_content_evidence(cli):
    plan, records, task = partial_build(cli)
    cli.remote.builds[101]["result"] = "succeeded"
    for record in records:
        if record["result"] == "succeededWithIssues":
            record["result"] = "succeeded"
    evidence = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    assert evidence["complete"]
    assert evidence["producer_evidence"]["schema_version"] == 3
    receipt = json.loads(cli.state.read_text())["actions"][0]["receipt"]
    assert receipt["schema_version"] == 1
    assert all(set(job) == {"id", "name", "state", "result"} for job in receipt["jobs"])


def test_task_retry_counter_is_independent_of_the_job_retry_counter(cli):
    plan, _, task = partial_build(cli)
    task["attempt"] = 3
    assert ops.verified_evidence(plan, cli.state, remote=cli.remote)["complete"]


def test_mixed_clean_and_warning_targets_keep_their_own_proof_formats(cli):
    plan = matrix.build_plan(
        "1.2.0",
        target_keys=["master", "spark4.1"],
        oss_commits={"master": "a" * 40, "spark4.1": "a" * 40},
    )
    partial_build(cli, plan=plan)
    evidence = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    assert evidence["complete"]
    producer = evidence["producer_evidence"]
    assert producer["schema_version"] == 3
    runs = {run["build"]["id"]: run for run in producer["runs"]}
    assert runs[101]["build"]["result"] == "partiallySucceeded"
    assert all(
        "task_summary" in job and "tasks" not in job for job in runs[101]["jobs"]
    )
    assert runs[102]["build"]["result"] == "succeeded"
    assert "job_records_sha256" not in runs[102]
    assert all("tasks" not in job for job in runs[102]["jobs"])
    ops.validate_producer_evidence(plan, evidence)
    assert len(cli.remote.queued) == 2


@pytest.mark.parametrize("include_spark40", [False, True])
@pytest.mark.parametrize("widespread_warnings", [False, True])
def test_production_sized_warning_evidence_fits_and_passes_notes_guard(
    cli, monkeypatch, tmp_path, include_spark40, widespread_warnings
):
    import release_dbc
    import release_guard

    keys = (
        ["master", "spark4.0", "spark4.1"]
        if include_spark40
        else ["master", "spark4.1"]
    )
    plan = matrix.build_plan(
        "1.2.0", target_keys=keys, oss_commits={key: "a" * 40 for key in keys}
    )
    _, initial, _ = partial_build(cli, plan=plan)
    advisory_names = list(ops.ADVISORY_TASKS)
    for build_id, target in enumerate(plan.targets, 101):
        records = copy.deepcopy(initial)
        build = cli.remote.builds[build_id]
        build["result"] = "partiallySucceeded"
        timing = {"startTime": build["queueTime"], "finishTime": build["finishTime"]}
        for record in records:
            record.update(timing)
        for i in range(67):
            job_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{build_id}/job/{i}"))
            records.append(
                {
                    "id": job_id,
                    "type": "Job",
                    "name": f"UnitTests part {i}",
                    "state": "completed",
                    "result": "succeededWithIssues",
                    "attempt": 1,
                    **timing,
                }
            )
            for j in range(20):
                warning = j > 0 if widespread_warnings else j == 19
                name = (
                    advisory_names[j % len(advisory_names)]
                    if warning
                    else "Run required work"
                )
                task_id, major = ops.ADVISORY_TASKS[name] if warning else (BASH_ID, "3")
                records.append(
                    {
                        "id": str(
                            uuid.uuid5(
                                uuid.NAMESPACE_URL, f"{build_id}/job/{i}/task/{j}"
                            )
                        ),
                        "parentId": job_id,
                        "type": "Task",
                        "name": name,
                        "state": "completed",
                        "result": "succeededWithIssues" if warning else "succeeded",
                        "attempt": 1,
                        **timing,
                        "task": {"id": task_id, "version": f"{major}.279.0"},
                    }
                )
        cli.remote.timelines[build_id] = {"records": records}
        base = datetime.now(timezone.utc) - timedelta(hours=5)

        def timestamp(value):
            return value.isoformat(timespec="microseconds").replace("+00:00", "0Z")

        build["queueTime"] = timestamp(base)
        build["finishTime"] = timestamp(base + timedelta(hours=4))
        for index, job in enumerate(r for r in records if r["type"] == "Job"):
            seed = int(
                hashlib.sha256(f"{build_id}/{index}".encode()).hexdigest()[:12], 16
            )
            start = base + timedelta(seconds=107 * index, microseconds=seed % 1000000)
            duration = timedelta(
                seconds=1200 + seed % 1800,
                microseconds=(seed // 1000000) % 1000000,
            )
            job.update(
                startTime=timestamp(start), finishTime=timestamp(start + duration)
            )
            tasks = [r for r in records if r.get("parentId") == job["id"]]
            for task_index, task in enumerate(tasks):
                task.update(
                    startTime=timestamp(start + duration * task_index / len(tasks)),
                    finishTime=timestamp(
                        start + duration * (task_index + 1) / len(tasks)
                    ),
                )
        paths = (
            f"{module}_{target.scala}/{module}_{target.scala}-"
            f"{target.oss_maven_version}{classifier}{suffix}"
            for module in verify.PUBLIC_MAVEN_MODULES
            for classifier in ("", "-sources", "-javadoc", "-tests", "-tests-sources")
            for suffix in (".jar.asc", ".jar.sha1", ".jar.sha256", ".jar.sha512")
        )
        cli.remote.manifests[build_id][0]["artifacts"].extend(
            {
                "path": path,
                "sha256": hashlib.sha256(f"{build_id}-{index}".encode()).hexdigest(),
                "size": index + 1,
            }
            for index, path in enumerate(paths)
        )
    evidence = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    assert evidence["complete"]
    encoded = verify.encode_evidence(evidence)
    assert len(encoded) <= verify.MAX_GITHUB_EVIDENCE_CHARS
    decoded = verify.decode_evidence(encoded)
    verify.validate_evidence(plan, decoded)
    payload = {
        "ref": "v1.2.0",
        "inputs": {
            "plan_json": cli.plan.read_text(encoding="utf-8"),
            "evidence_base64": encoded,
            "approve_plan": plan.plan_id,
        },
    }
    assert len(json.dumps(payload)) < 65535
    state = json.loads(cli.state.read_text())
    assert all(len(action["receipt"]["jobs"]) == 70 for action in state["actions"])
    assert all(
        sum(len(job["tasks"]) for job in action["receipt"]["jobs"]) > 1300
        for action in state["actions"]
    )
    for action, run in zip(state["actions"], evidence["producer_evidence"]["runs"]):
        assert (
            run["job_records_sha256"]
            == hashlib.sha256(ops.canonical(action["receipt"]["jobs"])).hexdigest()
        )
    if os.name == "nt":
        # Windows limits a single environment variable to 32,767 characters.
        evidence_file = tmp_path / "evidence.json"
        evidence_file.write_text(json.dumps(decoded), encoding="utf-8")
        evidence_args = ["--evidence", str(evidence_file)]
    else:
        monkeypatch.setenv("RELEASE_EVIDENCE_BASE64", encoded)
        evidence_args = ["--evidence-base64-env"]
    monkeypatch.setattr(
        release_dbc,
        "fetch_public_archive",
        lambda *_: (b"x" * 321, {"sha256": "f" * 64}),
    )
    header = tmp_path / "installation.md"
    assert (
        release_guard.main(
            [
                "notes",
                "--plan",
                str(cli.plan),
                *evidence_args,
                "--approve-plan",
                plan.plan_id,
                "--tag",
                "v1.2.0",
                "--commit",
                "a" * 40,
                "--installation-output",
                str(header),
            ]
        )
        == 0
    )
    assert "| 4.1 | 3.13 |" in header.read_text()
    assert len(cli.remote.queued) == len(keys)


def test_private_release_does_not_gain_partial_success_authorization(cli):
    plan = release_plan(
        scope="internal-only",
        repositories=["internal"],
        families=["maven"],
        internal_patch="1",
    )
    cli.remote.missing = {("internal", "maven")}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "internal", ["maven"])
    cli.remote.builds[101]["result"] = "partiallySucceeded"
    code, report, _ = cli("status", plan=plan)
    assert code == 1 and not report["complete"]
    assert not ops.verified_evidence(plan, cli.state, remote=cli.remote)["complete"]
    assert len(cli.remote.queued) == 1


def test_warning_task_proof_cannot_change_during_evidence_collection(cli, monkeypatch):
    plan, _, _ = partial_build(cli)
    original = cli.remote.timeline
    calls = 0

    def changed_timeline(build_id):
        nonlocal calls
        calls += 1
        result = original(build_id)
        if calls > 1:
            next(
                record for record in result["records"] if record["id"] == identity(100)
            )["task"]["version"] = "2.280.0"
        return result

    monkeypatch.setattr(cli.remote, "timeline", changed_timeline)
    with pytest.raises(ops.ReleaseError, match="jobs changed"):
        ops.verified_evidence(plan, cli.state, remote=cli.remote)
    assert len(cli.remote.queued) == 1


def test_pipeline_advisory_steps_and_publication_requirements_match_warning_policy():
    root = Path(__file__).resolve().parents[2]
    names = {}
    kinds = {"Cache@2": CACHE_ID, "AzureKeyVault@2": KEY_VAULT_ID, "Bash@3": BASH_ID}

    def inspect(node):
        if isinstance(node, list):
            for value in node:
                inspect(value)
        elif isinstance(node, dict):
            if "steps" not in node and node.get("continueOnError") is True:
                name = node["displayName"]
                kind = node.get("task", "Bash@3" if "bash" in node else None)
                assert kind in kinds, (name, kind)
                specification = (kinds[kind], kind.rsplit("@", 1)[1])
                assert names.setdefault(name, specification) == specification
            for value in node.values():
                inspect(value)

    pipeline = yaml.safe_load((root / "pipeline.yaml").read_text(encoding="utf-8"))
    inspect(pipeline)
    for template in ("sbt_cache", "conda", "codecov"):
        inspect(
            yaml.safe_load(
                (root / "templates" / f"{template}.yml").read_text(encoding="utf-8")
            )
        )
    assert names == ops.ADVISORY_TASKS

    def release_steps(node):
        if isinstance(node, list):
            for value in node:
                yield from release_steps(value)
        elif isinstance(node, dict):
            if node.get("job") == "Release":
                yield from node["steps"]
            else:
                for value in node.values():
                    yield from release_steps(value)

    required = {
        step["displayName"]: step
        for step in release_steps(pipeline)
        if step.get("displayName") in ops.REQUIRED_RELEASE_TASKS
    }
    assert set(required) == ops.REQUIRED_RELEASE_TASKS
    assert all(
        "condition" not in step and not step.get("continueOnError")
        for step in required.values()
    )


@pytest.mark.parametrize("position", ["before-build", "after-build"])
def test_fleet_r5_warning_job_windows_must_belong_to_the_build(cli, position):
    plan, records, _ = partial_build(cli)
    build = cli.remote.builds[101]
    if position == "before-build":
        start = finish = datetime(2020, 1, 1, tzinfo=timezone.utc)
    else:
        start = finish = ops._time(build["finishTime"], "Build finish") + timedelta(
            seconds=1
        )
    for record in records:
        record.update(startTime=start.isoformat(), finishTime=finish.isoformat())
    code, report, _ = cli("status", plan=plan)
    assert code == 1 and not report["complete"]
    assert "build" in report["actions"][0]["error"].lower()
    assert not ops.verified_evidence(plan, cli.state, remote=cli.remote)["complete"]
    assert len(cli.remote.queued) == 1


@pytest.mark.parametrize("fault", ["before-build", "after-build", "reversed-build"])
def test_fleet_r5_exported_warning_windows_are_checked_against_the_build(cli, fault):
    plan, _, _ = partial_build(cli)
    evidence = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    run = evidence["producer_evidence"]["runs"][0]
    if fault == "reversed-build":
        run["build"]["finishTime"] = "2020-01-01T00:00:00Z"
    else:
        stamp = (
            "2020-01-01T00:00:00Z"
            if fault == "before-build"
            else (
                ops._time(run["build"]["finishTime"], "Build finish")
                + timedelta(seconds=1)
            ).isoformat()
        )
        for job in run["jobs"]:
            job.update(started_at=stamp, finished_at=stamp)
    with pytest.raises((ValueError, ops.ReleaseError), match="build"):
        verify.validate_evidence(plan, evidence)
