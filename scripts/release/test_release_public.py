# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import base64
import copy
import gzip
import json
from dataclasses import asdict

import pytest

from test_release_config import private_profile  # noqa: F401

import release_config as config
import release_guard as guard
import release_matrix as matrix
import release_ops as ops
import verify_release as verify
from test_release_ops import BASE_CHECKER, cli, no_network  # noqa: F401

SHA = "a" * 40
PLAN_KEYS = {
    "schema_version",
    "plan_id",
    "oss_version",
    "scope",
    "families",
    "repositories",
    "mode",
    "ado_org",
    "ado_project",
    "oss_maven_pipeline_id",
    "targets",
}
TARGET_KEYS = {
    "key",
    "branch",
    "base_branch",
    "spark",
    "python",
    "scala",
    "oss_tags",
    "oss_maven_tag",
    "oss_maven_version",
    "oss_commit",
}


def public_plan():
    return matrix.build_plan(
        "1.2.0",
        target_keys=[target.key for target in matrix.TARGETS],
        oss_commits={target.key: SHA for target in matrix.TARGETS},
    )


def legacy_document(combined=False):
    data = asdict(
        matrix.build_plan(
            "1.2.0",
            target_keys=[target.key for target in matrix.TARGETS],
            repositories=["oss", "internal"],
            families=["maven"],
            oss_commits={target.key: SHA for target in matrix.TARGETS},
            internal_commits={target.key: "b" * 40 for target in matrix.TARGETS},
        )
    )
    del data["private_profile"]
    data.update(
        schema_version=1,
        pip_feed="synthetic-private-pip",
        upack_feed="synthetic-private-upack",
        internal_maven_pipeline_id=900001,
        publish_pipeline_id=900002,
    )
    if combined:
        data["repositories"] = ["oss", "internal"]
    else:
        data["repositories"] = ["oss"]
        for target in data["targets"]:
            target["internal_commit"] = None
    data["plan_id"] = matrix.plan_digest(data)
    return data


def encoded(data):
    return base64.b64encode(ops.canonical(data)).decode("ascii")


def assert_public_document(data):
    assert data["schema_version"] == 2
    assert set(data) == PLAN_KEYS
    assert all(set(target) == TARGET_KEYS for target in data["targets"])
    assert data["repositories"] == ["oss"]
    assert data["families"] == ["maven"]


def test_public_wire_format_is_an_exact_allowlist():
    data = matrix.plan_to_dict(public_plan())
    assert_public_document(data)
    assert matrix.plan_to_dict(matrix.load_plan(data, require_bound=True)) == data
    assert matrix.plan_to_dict(public_plan()) == data


@pytest.mark.parametrize("profile_path", [None, "does-not-exist.json"])
def test_public_generation_and_transport_never_load_private_configuration(
    monkeypatch, profile_path
):
    if profile_path is None:
        monkeypatch.delenv(config.PROFILE_ENV)
    else:
        monkeypatch.setenv(config.PROFILE_ENV, profile_path)
    plan = public_plan()
    assert_public_document(matrix.plan_to_dict(plan))
    assert len(ops.build_actions(plan)) == len(matrix.TARGETS)


def test_private_profile_changes_require_new_approval_before_any_probe(
    cli, private_profile
):
    plan = matrix.build_plan(
        "1.2.0",
        target_keys=["master"],
        families=["upack"],
        oss_commits={"master": SHA},
    )
    profile = json.loads(private_profile.read_text())
    profile["publish_pipeline_id"] += 1
    private_profile.write_text(json.dumps(profile))
    code, _, error = cli(plan=plan, apply=True)
    assert code == 2 and "profile differs" in error
    assert not cli.remote.queued
    assert not cli.remote.inventory_calls
    assert not cli.state.exists()
    replacement = matrix.build_plan(
        "1.2.0",
        target_keys=["master"],
        families=["upack"],
        oss_commits={"master": SHA},
    )
    assert replacement.plan_id != plan.plan_id


def test_private_profile_cannot_select_public_maven_pipeline(private_profile):
    profile = json.loads(private_profile.read_text())
    profile["publish_pipeline_id"] = matrix.OSS_MAVEN_PIPELINE_ID
    private_profile.write_text(json.dumps(profile))
    with pytest.raises(ValueError, match="public Maven pipeline"):
        matrix.build_plan("1.2.0", families=["upack"])


@pytest.mark.parametrize("location", ["root", "target", "tags"])
def test_public_rehashed_extra_fields_are_rejected(location):
    data = matrix.plan_to_dict(public_plan())
    if location == "root":
        data["private_feed"] = "synthetic-private-value"
    elif location == "target":
        data["targets"][0]["internal_commit"] = "b" * 40
    else:
        data["targets"][0]["oss_tags"].append("synthetic-private-value")
    data["plan_id"] = matrix.plan_digest(data)
    with pytest.raises(ValueError):
        matrix.load_plan(data, require_bound=True)


def test_public_creation_rejects_unselected_private_inputs():
    with pytest.raises(ValueError, match="public|unselected"):
        matrix.build_plan("1.2.0", internal_commits={"master": "b" * 40})


@pytest.mark.parametrize(
    "change",
    [
        lambda plan: setattr(plan, "pip_feed", "synthetic-private-pip"),
        lambda plan: setattr(plan.targets[0], "internal_commit", "b" * 40),
        lambda plan: plan.publish_parameters.update(build_internal_pip_py311=False),
    ],
)
def test_public_object_cannot_silently_discard_private_mutation(change):
    plan = public_plan()
    change(plan)
    with pytest.raises(ValueError, match="regenerate"):
        matrix.plan_to_dict(plan)


def test_combined_document_is_rejected_by_public_maven_guard():
    plan = matrix.build_plan(
        "1.2.0",
        target_keys=[target.key for target in matrix.TARGETS],
        repositories=["oss", "internal"],
        families=["maven"],
        oss_commits={target.key: SHA for target in matrix.TARGETS},
        internal_commits={target.key: "b" * 40 for target in matrix.TARGETS},
    )
    with pytest.raises(ValueError, match="public|regenerat"):
        guard.maven_plan(
            encoded(matrix.plan_to_dict(plan)), plan.plan_id, "refs/tags/v1.2.0", SHA
        )


@pytest.mark.parametrize("sink", ["evidence", "maven"])
def test_public_nested_json_is_a_controlled_non_echoing_refusal(sink):
    marker = "synthetic-nested-public-value"
    raw = ('{"' + marker + '":' + "[" * 20000 + "0" + "]" * 20000 + "}").encode("utf-8")
    if sink == "evidence":
        assert len(raw) < verify.MAX_EVIDENCE_BYTES
        payload = base64.b64encode(gzip.compress(raw)).decode("ascii")
        assert len(payload) < verify.MAX_GITHUB_EVIDENCE_CHARS
        with pytest.raises(ValueError) as error:
            verify.decode_evidence(payload)
    else:
        payload = base64.b64encode(raw).decode("ascii")
        assert len(payload) < 65536
        with pytest.raises(ValueError) as error:
            guard.maven_plan(payload, "b" * 64, "refs/tags/v1.2.0", SHA)
    assert marker not in str(error.value)
    assert payload not in str(error.value)


def test_public_driver_commands_only_transport_the_approved_public_document():
    plan = public_plan()
    for action in ops.build_actions(plan):
        operation = ops._operation(plan, action, ["maven"])
        payload = operation["parameters"]["release_plan_base64"]
        data = json.loads(base64.b64decode(payload))
        assert_public_document(data)
        assert data == matrix.plan_to_dict(plan)
        assert data["plan_id"] == plan.plan_id
        assert "synthetic-private" not in json.dumps(operation)


def test_private_action_cannot_be_redirected_to_public_pipeline():
    plan = matrix.build_plan(
        "1.2.0",
        target_keys=["master"],
        families=["upack"],
        oss_commits={"master": SHA},
    )
    action = ops.build_actions(plan)[0]
    action["pipeline_id"] = matrix.OSS_MAVEN_PIPELINE_ID
    with pytest.raises(ops.ReleaseError, match="pipeline destination"):
        ops._operation(plan, action, ["upack"])


def test_legacy_local_read_preserves_identity_but_cannot_export_or_execute():
    data = legacy_document()
    plan = matrix.load_plan(data, require_bound=True)
    assert matrix.plan_to_dict(plan) == data
    assert plan.plan_id != public_plan().plan_id
    with pytest.raises(ValueError, match="regenerat|public"):
        guard.maven_plan(encoded(data), plan.plan_id, "refs/tags/v1.2.0", SHA)
    with pytest.raises(ValueError, match="regenerat|public"):
        guard.notes_plan(plan, "v1.2.0", SHA, plan.plan_id)
    with pytest.raises((ValueError, ops.ReleaseError), match="regenerat|public"):
        ops.build_actions(plan)
    with pytest.raises(ValueError, match="approval"):
        guard.maven_plan(
            encoded(matrix.plan_to_dict(public_plan())),
            plan.plan_id,
            "refs/tags/v1.2.0",
            SHA,
        )


def producer_report(cli):
    plan = public_plan()
    cli.remote.missing = {("oss", "maven")}
    assert cli(plan=plan, apply=True)[0] == 1
    for build_id, target in zip(sorted(cli.remote.builds), plan.targets):
        cli.remote.succeed(build_id, plan, "oss", ["maven"], target=target.key)
    assert cli(plan=plan, apply=True)[0] == 0
    return plan, ops.verified_evidence(plan, cli.state, remote=cli.remote)


def test_public_evidence_roundtrip_contains_only_public_plans(cli):
    plan, report = producer_report(cli)
    assert "internal_patch" not in report
    restored = verify.decode_evidence(verify.encode_evidence(report))
    assert restored == report
    verify.validate_evidence(plan, restored)
    for run in restored["producer_evidence"]["runs"]:
        payload = run["operation"]["parameters"]["release_plan_base64"]
        assert_public_document(json.loads(base64.b64decode(payload)))


@pytest.mark.parametrize(
    "location",
    [
        "root",
        "row",
        "job",
        "repository",
        "repository-name",
        "parameters",
        "provenance",
        "payload",
        "combined-payload",
    ],
)
def test_public_evidence_rejects_extra_private_data_at_every_layer(cli, location):
    plan, report = producer_report(cli)
    run = report["producer_evidence"]["runs"][0]
    destination = {
        "root": report,
        "row": report["rows"][0],
        "job": run["jobs"][0],
        "repository": run["definition"]["repository"],
        "parameters": run["build"]["templateParameters"],
        "provenance": run["provenance"][0]["artifacts"][0],
    }
    if location == "combined-payload":
        run["operation"]["parameters"]["release_plan_base64"] = encoded(
            legacy_document(combined=True)
        )
    elif location == "repository-name":
        run["build"]["repository"]["name"] = "synthetic-private-source"
        run["definition"]["repository"]["name"] = "synthetic-private-source"
    elif location == "payload":
        data = matrix.plan_to_dict(plan)
        data["private_feed"] = "synthetic-private-value"
        data["plan_id"] = matrix.plan_digest(data)
        run["operation"]["parameters"]["release_plan_base64"] = encoded(data)
    else:
        destination[location]["private_field"] = "synthetic-private-value"
    with pytest.raises(ValueError):
        verify.encode_evidence(report)
    raw = base64.b64encode(gzip.compress(ops.canonical(report))).decode("ascii")
    with pytest.raises(ValueError):
        verify.decode_evidence(raw)


def test_public_evidence_rejects_duplicate_json_members(cli):
    _, report = producer_report(cli)
    raw = ops.canonical(report).decode("ascii")
    raw = '{"plan_id":"duplicate",' + raw[1:]
    payload = base64.b64encode(gzip.compress(raw.encode("ascii"))).decode("ascii")
    with pytest.raises(ValueError):
        verify.decode_evidence(payload)


@pytest.mark.parametrize(
    "field,value",
    [
        ("id", "synthetic-nonpublic-marker"),
        ("name", "synthetic-nonpublic-marker"),
        pytest.param("id", "f" * 4096, id="oversized-id"),
        ("id", "00000000-0000-0000-0000-000000000000"),
        ("id", 1),
        ("name", ["Release"]),
    ],
)
def test_public_job_values_reject_unapproved_metadata(cli, field, value):
    _, report = producer_report(cli)
    report["producer_evidence"]["runs"][0]["jobs"][0][field] = value
    with pytest.raises(ValueError):
        verify.encode_evidence(report)
    payload = base64.b64encode(gzip.compress(ops.canonical(report))).decode("ascii")
    with pytest.raises(ValueError):
        verify.decode_evidence(payload)


def test_public_timeline_labels_are_replaced_without_dropping_jobs(cli):
    plan, _ = producer_report(cli)
    extra = {
        "id": "22222222-2222-4222-8222-222222222222",
        "type": "Job",
        "name": "synthetic-nonpublic-marker",
        "state": "completed",
        "result": "succeeded",
    }
    cli.remote.timelines[101]["records"].append(extra)
    report = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    exported = verify.decode_evidence(verify.encode_evidence(report))
    jobs = exported["producer_evidence"]["runs"][0]["jobs"]
    assert len(jobs) == 2
    assert {job["id"] for job in jobs} == {
        record["id"] for record in cli.remote.timelines[101]["records"]
    }
    assert [job["name"] for job in jobs] == ["Release", "Maven pipeline job"]
    assert "synthetic-nonpublic-marker" not in json.dumps(exported)
    assert all(job["result"] == "succeeded" for job in jobs)


@pytest.mark.parametrize(
    "fault", ["missing-release", "skipped-release", "duplicate-id"]
)
def test_public_job_contract_requires_unique_jobs_and_successful_release(cli, fault):
    _, report = producer_report(cli)
    jobs = report["producer_evidence"]["runs"][0]["jobs"]
    other = {
        **jobs[0],
        "id": "22222222-2222-4222-8222-222222222222",
        "name": "Maven pipeline job",
    }
    jobs.append(other)
    if fault == "missing-release":
        jobs[0]["name"] = "Maven pipeline job"
    elif fault == "skipped-release":
        jobs[0]["result"] = "skipped"
    else:
        other["id"] = jobs[0]["id"]
    with pytest.raises(ValueError):
        verify.encode_evidence(report)
    payload = base64.b64encode(gzip.compress(ops.canonical(report))).decode("ascii")
    with pytest.raises(ValueError):
        verify.decode_evidence(payload)


@pytest.mark.parametrize("result", ["failed", "canceled", "skipped"])
def test_public_job_export_does_not_hide_unsuccessful_required_jobs(cli, result):
    plan, _ = producer_report(cli)
    records = cli.remote.timelines[101]["records"]
    records.append(
        {
            **records[0],
            "id": "22222222-2222-4222-8222-222222222222",
            "name": "synthetic-nonpublic-marker",
        }
    )
    records[0]["result"] = result
    code, status, _ = cli("status", plan=plan)
    assert code == 1 and not status["complete"]
    report = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    assert not report["complete"]
    with pytest.raises(ValueError):
        verify.encode_evidence(report)


@pytest.mark.parametrize("result", ["failed", "canceled", "skipped"])
def test_public_secondary_job_outcomes_are_not_filtered_by_label(cli, result):
    plan, _ = producer_report(cli)
    cli.remote.timelines[101]["records"].append(
        {
            "id": "22222222-2222-4222-8222-222222222222",
            "type": "Job",
            "name": "synthetic-nonpublic-marker",
            "state": "completed",
            "result": result,
        }
    )
    code, status, error = cli("status", plan=plan)
    report = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    if result == "skipped":
        assert code == 0 and status["complete"], error
        exported = verify.decode_evidence(verify.encode_evidence(report))
        jobs = exported["producer_evidence"]["runs"][0]["jobs"]
        assert len(jobs) == 2
        assert jobs[1]["name"] == "Maven pipeline job"
        assert jobs[1]["result"] == "skipped"
    else:
        assert code == 1 and not status["complete"]
        assert not report["complete"]
        with pytest.raises(ValueError):
            verify.encode_evidence(report)


@pytest.mark.parametrize("count", [256, 257])
def test_public_job_coverage_is_bounded_without_truncation(count):
    jobs = [
        {
            "id": f"00000000-0000-4000-8000-{index + 1:012x}",
            "name": "Release" if index == 0 else "Maven pipeline job",
            "state": "completed",
            "result": "succeeded",
        }
        for index in range(count)
    ]
    if count == 256:
        assert ops._public_jobs(jobs) == jobs
    else:
        with pytest.raises(ops.ReleaseError, match="coverage"):
            ops._public_jobs(jobs)


@pytest.mark.parametrize("actual", [True, "true", "TRUE", "TrUe"])
def test_public_completed_boolean_representations_export_canonical_evidence(
    cli, actual
):
    plan, _ = producer_report(cli)
    cli.remote.builds[101]["templateParameters"]["publishRelease"] = actual
    queued = copy.deepcopy(cli.remote.queued)
    code, status, error = cli("status", plan=plan)
    assert code == 0 and status["complete"], error
    report = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    exported = verify.decode_evidence(verify.encode_evidence(report))
    verify.validate_evidence(plan, exported)
    for run in exported["producer_evidence"]["runs"]:
        parameters = run["build"]["templateParameters"]
        assert parameters == run["operation"]["parameters"]
        assert parameters["publishRelease"] is True
        assert set(parameters) == {
            "publishRelease",
            "release_plan_base64",
            "release_plan_id",
        }
    assert cli.remote.queued == queued
    assert matrix.plan_to_dict(plan)["plan_id"] == exported["plan_id"]


def test_public_wire_booleans_remain_canonical_and_keys_remain_allowlisted(cli):
    _, report = producer_report(cli)
    parameters = report["producer_evidence"]["runs"][0]["build"]["templateParameters"]
    parameters["publishRelease"] = "true"
    with pytest.raises(ValueError, match="unapproved parameters"):
        verify.encode_evidence(report)
    parameters["publishRelease"] = True
    parameters["unapproved"] = "synthetic-nonpublic-marker"
    with pytest.raises(ValueError, match="unapproved parameters"):
        verify.encode_evidence(report)


@pytest.mark.parametrize("actual", [1, "false", " true", "synthetic-nonpublic-marker"])
def test_public_completed_boolean_parameter_still_rejects_wrong_values(cli, actual):
    plan, _ = producer_report(cli)
    cli.remote.builds[101]["templateParameters"]["publishRelease"] = actual
    code, status, _ = cli("status", plan=plan)
    assert code == 1 and not status["complete"]
    report = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    assert not report["complete"]
    with pytest.raises(ValueError):
        verify.encode_evidence(report)


def test_public_text_contains_only_public_release_sections(cli, monkeypatch):
    monkeypatch.delenv(config.PROFILE_ENV)
    plan = public_plan()
    text = matrix.render_text(plan)
    headings = [line for line in text.splitlines() if line and not line.startswith(" ")]
    assert headings == [
        f"SynapseML release plan  OSS v{plan.oss_version}  scope=full",
        f"Plan {plan.plan_id}  schema=2  mode=production",
        "Repositories: oss; families: maven",
        "GIT TAGS",
        "MAVEN TAG BUILDS",
        "PRIVATE-FEED PUBLICATION: not selected",
        "GUARDED EXECUTION",
        "UPACK: not selected",
        "PIP: not selected",
    ]
    _, report, error = cli("preflight", plan=plan, state=False)
    assert report, error
    assert report["human_gates"] == (
        "Pipeline approvals remain manual. "
        "This command never creates tags, GitHub releases or deployment configuration changes."
    )


def configured_private_plan(private_profile):
    profile = json.loads(private_profile.read_text())
    profile["internal_packages"] = {
        "maven": "synthetic-extra-library",
        "pip": "synthetic-extra-wheel",
        "upack": "synthetic_extra_bundle",
    }
    private_profile.write_text(json.dumps(profile))
    return matrix.build_plan(
        "1.2.0",
        target_keys=["master"],
        repositories=["internal"],
        families=["maven", "pip", "upack"],
        oss_commits={"master": SHA},
        internal_commits={"master": "b" * 40},
    )


def test_private_package_names_drive_inventory_and_text(private_profile, monkeypatch):
    plan = configured_private_plan(private_profile)
    packages = plan.private_profile["internal_packages"]
    calls = []
    monkeypatch.setattr(verify, "Checker", BASE_CHECKER)
    monkeypatch.setattr(BASE_CHECKER, "ado_tag", lambda *_args: (verify.OK, "b" * 40))

    def maven(_checker, module, scala, version):
        calls.append(("maven", module))
        return verify.OK

    def versions(_checker, feed, protocol, name):
        family = "pip" if protocol == "pypi" else "upack"
        calls.append((family, name))
        return [getattr(plan.targets[0], "internal_" + family + "_version")]

    monkeypatch.setattr(BASE_CHECKER, "_maven", maven)
    monkeypatch.setattr(BASE_CHECKER, "_feed_versions", versions)
    rows, complete = verify.run_plan(plan, token="synthetic-token")
    assert complete
    assert dict(calls) == packages
    assert {row["name"] for row in rows if row["kind"] in plan.families} == {
        packages["maven"] + "_2.12",
        packages["pip"],
        packages["upack"],
    }
    assert {ops._row_key(row) for row in rows} == set(ops._required_rows(plan))
    text = matrix.render_text(plan)
    assert all(name in text for name in packages.values())
    assert "setup.sh" not in text


def test_private_package_names_bind_receipts_inventory_and_absence(
    cli, private_profile
):
    plan = configured_private_plan(private_profile)
    packages = plan.private_profile["internal_packages"]
    cli.remote.missing = {("internal", family) for family in plan.families}
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(101, plan, "internal", ["maven"])
    assert cli(plan=plan, apply=True)[0] == 1
    cli.remote.succeed(102, plan, "internal", ["pip", "upack"])
    code, status, error = cli("status", plan=plan)
    assert code == 0 and status["complete"], error
    report = ops.verified_evidence(plan, cli.state, remote=cli.remote)
    verify.validate_evidence(plan, report)
    run = next(
        run for run in report["producer_evidence"]["runs"] if run["build"]["id"] == 102
    )
    assert {doc["family"]: doc["package_name"] for doc in run["provenance"]} == {
        family: packages[family] for family in ("pip", "upack")
    }
    actions = [
        action for action in ops.build_actions(plan) if action["kind"] == "publisher"
    ]
    descriptors = ops._absence_descriptors(
        plan, actions, report["producer_evidence"]["destinations"]
    )
    assert {row["family"]: row["name"] for row in descriptors} == {
        family: packages[family] for family in ("pip", "upack")
    }
    doc = cli.remote.manifests[102][0]
    doc["package_name"] = "synthetic-unapproved-package"
    doc["outputs"][0]["package_name"] = doc["package_name"]
    code, status, _ = cli("status", plan=plan)
    assert code == 1 and not status["complete"]


def test_legacy_identity_is_readable_but_cannot_infer_private_package_names(
    monkeypatch,
):
    data = legacy_document(combined=True)
    monkeypatch.delenv(config.PROFILE_ENV)
    legacy = matrix.load_plan(data, require_bound=True)
    assert matrix.plan_to_dict(legacy) == data

    def unexpected(*_args, **_kwargs):
        raise AssertionError(
            "profile-dependent legacy inventory must fail before authentication"
        )

    monkeypatch.setattr(verify, "Checker", unexpected)
    with pytest.raises(ValueError, match="profile|legacy"):
        verify.run_plan(legacy)
    with pytest.raises(ValueError, match="profile|legacy"):
        matrix.render_text(legacy)


def test_public_inventory_does_not_load_private_package_configuration(monkeypatch):
    monkeypatch.delenv(config.PROFILE_ENV)
    plan = public_plan()

    def unexpected(*_args, **_kwargs):
        raise AssertionError(
            "public inventory must not load a private profile or token"
        )

    monkeypatch.setattr(verify, "load_profile", unexpected)
    monkeypatch.setattr(verify, "_get_ado_token", unexpected)
    monkeypatch.setattr(verify, "Checker", BASE_CHECKER)
    monkeypatch.setattr(BASE_CHECKER, "github_tag", lambda *_args: (verify.OK, SHA))
    monkeypatch.setattr(BASE_CHECKER, "_maven", lambda *_args, **_kwargs: verify.OK)
    monkeypatch.setattr(BASE_CHECKER, "public_pypi", lambda *_args: verify.OK)
    rows, complete = verify.run_plan(plan)
    assert complete and rows
    assert_public_document(matrix.plan_to_dict(plan))


def test_private_plan_cannot_infer_missing_package_identity_from_current_profile(
    private_profile,
):
    plan = configured_private_plan(private_profile)
    data = matrix.plan_to_dict(plan)
    del data["private_profile"]["internal_packages"]
    data["plan_id"] = matrix.plan_digest(data)
    with pytest.raises(ValueError, match="profile"):
        matrix.load_plan(data, require_bound=True)


def test_private_package_identity_change_requires_reapproval(cli, private_profile):
    plan = configured_private_plan(private_profile)
    profile = json.loads(private_profile.read_text())
    profile["internal_packages"]["pip"] = "synthetic-replacement-wheel"
    private_profile.write_text(json.dumps(profile))
    code, _, error = cli(plan=plan, apply=True)
    assert code == 2 and "profile differs" in error
    assert not cli.remote.inventory_calls and not cli.remote.queued
    assert not cli.state.exists()
    replacement = matrix.build_plan(
        plan.oss_version,
        target_keys=["master"],
        repositories=["internal"],
        families=plan.families,
        oss_commits={"master": SHA},
        internal_commits={"master": "b" * 40},
    )
    assert replacement.plan_id != plan.plan_id
