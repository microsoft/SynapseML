# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import base64
import json
import hashlib
import os
import sys
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_guard as guard  # noqa: E402
import release_matrix as matrix  # noqa: E402
from test_esrp_staging import ivy_fixture, staging  # noqa: E402

SHA = "a" * 40


def public_plan(**changes):
    options = {
        "repositories": ["oss"],
        "families": ["maven"],
        "oss_commits": {target.key: SHA for target in matrix.TARGETS},
    }
    options.update(changes)
    return matrix.build_plan("1.1.4", **options)


def test_full_release_cannot_silently_skip_a_supported_target():
    assert len(guard.full_release("1.1.4", "false").targets) == len(matrix.TARGETS)
    with pytest.raises(ValueError, match="SKIP_SPARK40"):
        guard.full_release("1.1.4", "true")


@pytest.mark.parametrize("missing", [None, "refs/heads/spark4.1"])
def test_full_release_cli_checks_actual_source_branches(tmp_path, monkeypatch, missing):
    observed = []

    def check_ref(_repo, *arguments):
        assert arguments[:-1] == ("ls-remote", "--exit-code", "--heads", "origin")
        ref = arguments[-1]
        observed.append(ref)
        if ref == missing:
            raise ValueError("required release branch is missing")
        return SHA

    monkeypatch.setattr(guard, "_git", check_ref)
    result = guard.main(["full-release", "--version", "1.1.4", "--repo", str(tmp_path)])
    assert observed == [
        "refs/heads/master",
        "refs/heads/spark4.0",
        "refs/heads/spark4.1",
    ]
    assert result == (2 if missing else 0)


def test_notes_require_an_explicit_complete_public_plan():
    plan = public_plan()
    guard.notes_plan(plan, "v1.1.4", SHA, plan.plan_id)
    with pytest.raises(ValueError, match="approval"):
        guard.notes_plan(plan, "v1.1.4", SHA, "b" * 64)
    with pytest.raises(ValueError, match="commit"):
        guard.notes_plan(plan, "v1.1.4", "b" * 40, plan.plan_id)
    partial = public_plan(target_keys=["master"], oss_commits={"master": SHA})
    with pytest.raises(ValueError, match="target"):
        guard.notes_plan(partial, "v1.1.4", SHA, partial.plan_id)


def test_notes_cannot_confuse_public_and_fabric_completion():
    for options in ({"families": ["upack"]}, {"repositories": ["oss", "internal"]}):
        if "repositories" in options:
            options["internal_commits"] = {
                target.key: "b" * 40 for target in matrix.TARGETS
            }
        plan = public_plan(**options)
        with pytest.raises(ValueError):
            guard.notes_plan(plan, "v1.1.4", SHA, plan.plan_id)


def test_maven_payload_checks_plan_tag_source_and_family():
    plan = public_plan()
    payload = base64.b64encode(json.dumps(matrix.plan_to_dict(plan)).encode()).decode()
    loaded, target = guard.maven_plan(payload, plan.plan_id, "refs/tags/v1.1.4", SHA)
    assert loaded.plan_id == plan.plan_id
    assert target.key == "master"
    for ref, commit in (("refs/heads/master", SHA), ("refs/tags/v1.1.4", "b" * 40)):
        with pytest.raises(ValueError):
            guard.maven_plan(payload, plan.plan_id, ref, commit)
    with pytest.raises(ValueError):
        guard.maven_plan("not-base64!", plan.plan_id, "refs/tags/v1.1.4", SHA)


def test_missing_maven_files_cannot_produce_a_success_receipt(tmp_path):
    plan = public_plan()
    with pytest.raises(ValueError, match="artifact"):
        guard.maven_receipt(plan, plan.targets[0], tmp_path, 123)


@pytest.fixture
def staged_maven(tmp_path, request):
    key = getattr(request, "param", "master")
    plan = public_plan(target_keys=[key], oss_commits={key: SHA})
    target = plan.targets[0]
    ivy, output = tmp_path / "ivy", tmp_path / "published"
    ivy_fixture(ivy, target.oss_maven_version, target.scala)
    staging.stage_release(ivy, output, target.oss_maven_version, target.scala)
    wheel = None
    if key == "master":
        wheel = tmp_path / guard.public_pypi_wheel_name(plan.oss_version)
        with zipfile.ZipFile(wheel, "w") as archive:
            archive.writestr(
                f"synapseml-{plan.oss_version}.dist-info/METADATA",
                f"Metadata-Version: 2.1\nName: synapseml\nVersion: {plan.oss_version}\n",
            )
    return plan, target, ivy, output, wheel


@pytest.mark.parametrize(
    "staged_maven", [target.key for target in matrix.TARGETS], indirect=True
)
def test_maven_receipt_cli_hashes_the_actual_esrp_output(
    tmp_path, monkeypatch, staged_maven
):
    plan, target, ivy, output, wheel = staged_maven
    module = f"synapseml_{target.scala}"
    jar = output / module / f"{module}-{target.oss_maven_version}.jar"
    with zipfile.ZipFile(jar, "a") as archive:
        archive.writestr("META-INF/staged-marker", "staged output, not the Ivy copy")
    signature = jar.with_name(jar.name + ".asc")
    signature.write_bytes(b"controlled signature fixture\n")
    original = ivy / module / target.oss_maven_version / "artifacts" / f"{module}.jar"
    assert jar.read_bytes() != original.read_bytes()
    for key, value in {
        "RELEASE_PLAN_BASE64": base64.b64encode(
            json.dumps(matrix.plan_to_dict(plan)).encode()
        ).decode(),
        "RELEASE_PLAN_ID": plan.plan_id,
        "BUILD_SOURCEBRANCH": f"refs/tags/{target.oss_maven_tag}",
        "BUILD_SOURCEVERSION": SHA,
        "BUILD_BUILDID": "123",
    }.items():
        monkeypatch.setenv(key, value)
    checked = []

    def check_source(repo, selected):
        assert repo == tmp_path and selected == target
        checked.append(selected.oss_commit)

    monkeypatch.setattr(guard, "validate_checkout", check_source)
    destination = tmp_path / "receipt" / "release-provenance.json"
    arguments = [
        "maven",
        "--repo",
        str(tmp_path),
        "--artifact-root",
        str(output),
        "--receipt",
        str(destination),
    ]
    if wheel is not None:
        arguments.extend(["--pypi-wheel", str(wheel)])
    assert guard.main(arguments) == 0
    assert checked == [SHA]
    receipt = json.loads(destination.read_text())
    assert receipt["plan_id"] == plan.plan_id
    assert receipt["source_commit"] == SHA
    expected = {
        path.relative_to(output).as_posix(): path.read_bytes()
        for path in output.rglob("*")
        if path.is_file()
    }
    if wheel is not None:
        expected[f"pypi/{wheel.name}"] = wheel.read_bytes()
    assert {item["path"] for item in receipt["artifacts"]} == set(expected)
    for item in receipt["artifacts"]:
        assert item["sha256"] == hashlib.sha256(expected[item["path"]]).hexdigest()
        assert item["size"] == len(expected[item["path"]])


@pytest.mark.parametrize(
    "corruption",
    ["missing-pom", "missing-tests", "empty", "wrong-version", "unexpected", "nested"],
)
def test_maven_receipt_rejects_incomplete_or_unexpected_staged_outputs(
    staged_maven, corruption
):
    plan, target, _, output, wheel = staged_maven
    module = f"synapseml_{target.scala}"
    jar = output / module / f"{module}-{target.oss_maven_version}.jar"
    if corruption == "missing-pom":
        jar.with_suffix(".pom").unlink()
    elif corruption == "missing-tests":
        core = f"synapseml-core_{target.scala}"
        (output / core / f"{core}-{target.oss_maven_version}-tests.jar").unlink()
    elif corruption == "empty":
        jar.write_bytes(b"")
    elif corruption == "wrong-version":
        jar.rename(jar.with_name(f"{module}-0.0.0.jar"))
    elif corruption == "unexpected":
        (output / "unexpected.txt").write_text("not a release artifact")
    else:
        nested = output / module / "nested"
        nested.mkdir()
        (nested / jar.name).write_bytes(jar.read_bytes())
    with pytest.raises(ValueError, match="artifact"):
        guard.maven_receipt(plan, target, output, 123, pypi_wheel=wheel)


def test_maven_receipt_refuses_the_unpublished_ivy_layout(staged_maven):
    plan, target, ivy, _, wheel = staged_maven
    with pytest.raises(ValueError, match="artifact"):
        guard.maven_receipt(plan, target, ivy, 123, pypi_wheel=wheel)


def test_maven_receipt_rejects_output_changed_during_hashing(staged_maven, monkeypatch):
    plan, target, _, output, wheel = staged_maven
    module = f"synapseml_{target.scala}"
    jar = output / module / f"{module}-{target.oss_maven_version}.jar"
    original_open = Path.open

    def change_output(path, *args, **kwargs):
        stream = original_open(path, *args, **kwargs)
        if path == jar and args == ("rb",):
            current = path.stat()
            os.utime(
                path, ns=(current.st_atime_ns, current.st_mtime_ns + 1_000_000_000)
            )
        return stream

    monkeypatch.setattr(Path, "open", change_output)
    with pytest.raises(ValueError, match="artifact changed"):
        guard.maven_receipt(plan, target, output, 123, pypi_wheel=wheel)


@pytest.mark.parametrize("version", ["1.1.4", "1.1.3"])
def test_pypi_receipt_requires_exact_package_metadata_and_records_bytes(
    tmp_path, version
):
    wheel = tmp_path / guard.public_pypi_wheel_name("1.1.4")
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(
            "synapseml-1.1.4.dist-info/METADATA",
            f"Metadata-Version: 2.1\nName: synapseml\nVersion: {version}\n",
        )
    if version != "1.1.4":
        with pytest.raises(ValueError, match="approved version"):
            guard.pypi_wheel_receipt(wheel, "1.1.4")
    else:
        receipt = guard.pypi_wheel_receipt(wheel, "1.1.4")
        assert receipt["sha256"] == hashlib.sha256(wheel.read_bytes()).hexdigest()
        assert receipt["size"] == wheel.stat().st_size


def test_pypi_upload_never_swallows_immutable_collision():
    build = (Path(__file__).resolve().parents[2] / "build.sbt").read_text(
        encoding="utf-8"
    )
    publish = build.split("publishPypi := {", 1)[1].split("val publishDocs", 1)[0]
    assert "--skip-existing" not in publish
    assert "TWINE_PASSWORD" in publish
    assert '"--password"' not in publish
