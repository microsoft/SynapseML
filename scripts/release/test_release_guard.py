# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import base64
import json
import hashlib
import os
import subprocess
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_release_config import private_profile  # noqa: F401

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


@pytest.fixture
def primary_integration(tmp_path, monkeypatch):
    remote = tmp_path / "remote.git"
    repo = tmp_path / "checkout"
    subprocess.run(
        ["git", "init", "--bare", str(remote)], check=True, capture_output=True
    )
    subprocess.run(
        ["git", "clone", str(remote), str(repo)], check=True, capture_output=True
    )

    def git(*args):
        return (
            subprocess.check_output(
                ["git", "-C", str(repo), *args], stderr=subprocess.PIPE
            )
            .decode()
            .strip()
        )

    git("config", "user.name", "Release test")
    git("config", "user.email", "release@example.invalid")
    git("checkout", "-b", "master")
    (repo / "source.txt").write_text("base\n")
    git("add", "source.txt")
    git("commit", "-m", "base")
    git("push", "origin", "master")
    git("checkout", "-b", "release-candidate/v1.2.0-master")
    (repo / "source.txt").write_text("release\n")
    git("commit", "-am", "release")
    commit = git("rev-parse", "HEAD")
    git("tag", "v1.2.0")
    git("push", "origin", "HEAD", "refs/tags/v1.2.0")
    original_git = guard._git

    def public_origin(path, *args, **kwargs):
        if args == ("remote", "get-url", "origin"):
            return "https://github.com/microsoft/SynapseML.git"
        return original_git(path, *args, **kwargs)

    monkeypatch.setattr(guard, "_git", public_origin)
    return repo, git, commit


def integrated_pr(commit, merge_commit, **changes):
    result = {
        "number": 7,
        "state": "closed",
        "merged": True,
        "head_sha": commit,
        "head_ref": "release-candidate/v1.2.0-master",
        "head_repo": "microsoft/SynapseML",
        "base_ref": "master",
        "base_repo": "microsoft/SynapseML",
        "merge_commit_sha": merge_commit,
    }
    result.update(changes)
    return result


@pytest.mark.parametrize("method", ["merge", "squash", "rebase"])
def test_primary_integration_accepts_reviewed_merge_methods(
    primary_integration, monkeypatch, method
):
    repo, git, commit = primary_integration
    git("checkout", "master")
    if method == "merge":
        git("merge", "--no-ff", "--no-edit", commit)
    elif method == "squash":
        git("merge", "--squash", commit)
        git("commit", "-m", "squashed release")
    else:
        (repo / "unrelated.txt").write_text("mainline change\n")
        git("add", "unrelated.txt")
        git("commit", "-m", "advance mainline")
        git("cherry-pick", commit)
    merge_commit = git("rev-parse", "HEAD")
    git("push", "origin", "master")
    git("checkout", "--detach", commit)
    observed = []

    def pulls(source):
        observed.append(source)
        return [integrated_pr(commit, merge_commit)]

    monkeypatch.setattr(guard, "_associated_pull_requests", pulls, raising=False)
    result = guard.verify_primary_integration(repo, "v1.2.0", commit)
    assert result == ("master ancestry" if method == "merge" else "merged PR #7")
    assert observed == ([] if method == "merge" else [commit])


@pytest.mark.parametrize(
    "changes",
    [
        {"state": "open"},
        {"merged": False},
        {"merged": "true"},
        {"head_sha": "b" * 40},
        {"head_ref": "unreviewed"},
        {"head_repo": "example/SynapseML"},
        {"base_ref": "spark4.1"},
        {"base_repo": "example/SynapseML"},
        {"merge_commit_sha": "b" * 40},
        {"merge_commit_sha": "not-a-commit"},
        {"number": True},
    ],
)
def test_primary_integration_rejects_unbound_merge_proof(
    primary_integration, monkeypatch, changes
):
    repo, git, commit = primary_integration
    git("checkout", "master")
    git("merge", "--squash", commit)
    git("commit", "-m", "squashed release")
    merged = git("rev-parse", "HEAD")
    git("push", "origin", "master")
    monkeypatch.setattr(
        guard,
        "_associated_pull_requests",
        lambda _: [integrated_pr(commit, merged, **changes)],
        raising=False,
    )
    with pytest.raises(ValueError):
        guard.verify_primary_integration(repo, "v1.2.0", commit)


@pytest.mark.parametrize("claimed_merge", [False, True])
def test_primary_integration_rejects_unmerged_candidate(
    primary_integration, monkeypatch, claimed_merge
):
    repo, _git, commit = primary_integration
    records = [integrated_pr(commit, commit)] if claimed_merge else []
    monkeypatch.setattr(
        guard, "_associated_pull_requests", lambda _: records, raising=False
    )
    with pytest.raises(ValueError, match="integrated"):
        guard.verify_primary_integration(repo, "v1.2.0", commit)


def test_primary_integration_requires_canonical_tag(primary_integration):
    repo, _git, _commit = primary_integration
    with pytest.raises(ValueError, match="Remote"):
        guard.verify_primary_integration(repo, "v1.2.0", "b" * 40)


@pytest.mark.parametrize(
    "origin", ["https://github.com/example/SynapseML", "/local/repo"]
)
def test_primary_integration_refuses_noncanonical_origin(monkeypatch, tmp_path, origin):
    monkeypatch.setattr(guard, "_git", lambda *_: origin)
    with pytest.raises(ValueError, match="canonical origin"):
        guard.verify_primary_integration(tmp_path, "v1.2.0", SHA)


@pytest.mark.parametrize("accepted", [True, False])
def test_primary_integration_cli_is_a_read_only_precheck(monkeypatch, capsys, accepted):
    def verify(repo, tag, commit):
        assert repo == Path(".")
        assert (tag, commit) == ("v1.2.0", SHA)
        if not accepted:
            raise ValueError("source not integrated")
        return "merged PR #7"

    monkeypatch.setattr(guard, "verify_primary_integration", verify)
    assert guard.main(
        ["verify-primary-integration", "--tag", "v1.2.0", "--commit", SHA]
    ) == (0 if accepted else 2)
    captured = capsys.readouterr()
    if accepted:
        assert json.loads(captured.out)["integration"] == "merged PR #7"
    else:
        assert "source not integrated" in captured.err


@pytest.mark.parametrize(
    "output",
    ["not JSON", "{}", "[null]", "[0]", " " * 60001, json.dumps([{}] * 101)],
    ids=[
        "non-json",
        "object-root",
        "null-item",
        "scalar-item",
        "oversized",
        "too-many",
    ],
)
def test_primary_integration_refuses_bad_service_data(monkeypatch, output):
    monkeypatch.setattr(
        guard.subprocess,
        "run",
        lambda *a, **kw: SimpleNamespace(returncode=0, stdout=output),
    )
    with pytest.raises(ValueError):
        guard._associated_pull_requests(SHA)


def test_primary_integration_api_projects_only_needed_fields(monkeypatch):
    def run(command, **kwargs):
        assert command[:3] == [
            "gh",
            "api",
            f"repos/microsoft/SynapseML/commits/{SHA}/pulls?per_page=100",
        ]
        assert "--jq" in command
        assert command[command.index("--hostname") + 1] == "github.com"
        assert command[command.index("--header") + 1] == (
            "X-GitHub-Api-Version: 2022-11-28"
        )
        assert kwargs["timeout"] == 60
        return SimpleNamespace(returncode=0, stdout="[]")

    monkeypatch.setattr(guard.subprocess, "run", run)
    assert guard._associated_pull_requests(SHA) == []


def test_primary_integration_api_timeout_does_not_echo_command(monkeypatch):
    def timeout(*_args, **_kwargs):
        raise subprocess.TimeoutExpired(["gh", "payload-must-not-be-echoed"], 60)

    monkeypatch.setattr(guard.subprocess, "run", timeout)
    with pytest.raises(ValueError, match="cannot query") as error:
        guard._associated_pull_requests(SHA)
    assert "payload-must-not-be-echoed" not in str(error.value)


@pytest.mark.parametrize(
    "tag,commit", [(None, SHA), ("v1.2.0", None), ("v1.2.0", "short")]
)
def test_primary_integration_rejects_invalid_inputs_without_io(
    monkeypatch, tag, commit
):
    def unexpected(*_args):
        pytest.fail("Git must not run for invalid identifiers")

    monkeypatch.setattr(guard, "_git", unexpected)
    with pytest.raises(ValueError):
        guard.verify_primary_integration(Path("."), tag, commit)


def test_primary_integration_rejects_shallow_history(monkeypatch):
    def git(_repo, *args):
        if args == ("remote", "get-url", "origin"):
            return "https://github.com/microsoft/SynapseML.git"
        assert args == ("rev-parse", "--is-shallow-repository")
        return "true"

    monkeypatch.setattr(guard, "_git", git)
    with pytest.raises(ValueError, match="non-shallow"):
        guard.verify_primary_integration(Path("."), "v1.2.0", SHA)


def test_release_git_timeout_is_bounded_and_does_not_echo_arguments(monkeypatch):
    def timeout(_command, **kwargs):
        assert kwargs["timeout"] == 180
        raise subprocess.TimeoutExpired(["git", "payload-must-not-be-echoed"], 180)

    monkeypatch.setattr(guard.subprocess, "run", timeout)
    with pytest.raises(ValueError, match="inspect remote state") as error:
        guard._git(Path("."), "fetch", "payload-must-not-be-echoed")
    assert "payload-must-not-be-echoed" not in str(error.value)


@pytest.mark.parametrize("missing", [None, "refs/heads/spark4.1"])
def test_full_release_cli_checks_actual_source_branches(tmp_path, monkeypatch, missing):
    observed = []

    def check_ref(_repo, *arguments):
        assert arguments[:-1] == ("ls-remote", "--heads", "origin")
        ref = arguments[-1]
        observed.append(ref)
        if ref == missing:
            return ""
        return f"{SHA}\t{ref}"

    monkeypatch.setattr(guard, "_git", check_ref)
    result = guard.main(["full-release", "--version", "1.1.4", "--repo", str(tmp_path)])
    assert observed == [
        "refs/heads/master",
        "refs/heads/spark4.0",
        "refs/heads/spark4.1",
    ]
    assert result == (2 if missing else 0)


@pytest.mark.parametrize(
    "records,accepted",
    [
        ([(SHA, "refs/tags/v1.1.4")], True),
        (
            [("b" * 40, "refs/tags/v1.1.4"), (SHA, "refs/tags/v1.1.4^{}")],
            True,
        ),
        (
            [(SHA, "refs/tags/v1.1.4"), ("b" * 40, "refs/tags/refs/tags/v1.1.4")],
            True,
        ),
        ([], False),
        ([(SHA, "refs/tags/refs/tags/v1.1.4")], False),
        ([(SHA, "refs/tags/v1.1.4^{}")], False),
        (
            [("b" * 40, "refs/tags/v1.1.4"), (SHA, "refs/tags/refs/tags/v1.1.4")],
            False,
        ),
        (
            [(SHA, "refs/tags/v1.1.4"), ("b" * 40, "refs/tags/v1.1.4^{}")],
            False,
        ),
        ([(SHA, "refs/tags/v1.1.4"), (SHA, "refs/tags/v1.1.4")], False),
        ([("invalid", "refs/tags/v1.1.4")], False),
    ],
)
def test_remote_tag_cli_requires_exact_refs_and_peeled_identity(
    tmp_path, monkeypatch, capsys, records, accepted
):
    def read(_repo, *arguments):
        if arguments[0] == "check-ref-format":
            assert arguments == ("check-ref-format", "refs/tags/v1.1.4")
            return ""
        assert arguments == (
            "ls-remote",
            "--tags",
            "origin",
            "refs/tags/v1.1.4",
            "refs/tags/v1.1.4^{}",
        )
        return "\n".join(f"{oid}\t{ref}" for oid, ref in records)

    monkeypatch.setattr(guard, "_git", read)
    result = guard.main(
        ["verify-tag", "--repo", str(tmp_path), "--tag", "v1.1.4", "--commit", SHA]
    )
    assert result == (0 if accepted else 2)
    output = capsys.readouterr()
    if accepted:
        assert json.loads(output.out) == {"tag": "v1.1.4", "commit": SHA}
        assert not output.err
    else:
        assert "error:" in output.err
        assert not output.out


@pytest.mark.parametrize("commit", ["", "a" * 39, "A" * 40])
def test_remote_tag_rejects_invalid_expected_commit_before_git(
    tmp_path, monkeypatch, commit
):
    def unexpected(*_args):
        pytest.fail("invalid expected commit must fail before Git is invoked")

    monkeypatch.setattr(guard, "_git", unexpected)
    with pytest.raises(ValueError, match="commit ID"):
        guard.verify_remote_tag(tmp_path, "v1.1.4", commit)


@pytest.mark.parametrize("tags", [[], ["v1.1.4", "v1.1.4"]])
def test_push_tags_rejects_empty_or_duplicate_selection_before_git(
    tmp_path, monkeypatch, tags
):
    def unexpected(*_args, **_kwargs):
        pytest.fail("invalid selection must not invoke Git")

    monkeypatch.setattr(guard, "_git", unexpected)
    with pytest.raises(ValueError, match="unique tag selection"):
        guard.push_tags(tmp_path, tags, SHA)


@pytest.mark.parametrize("failure", ["check-ref-format", "show-ref"])
def test_push_tags_validation_failure_precedes_staging(tmp_path, monkeypatch, failure):
    calls = []

    def run(_repo, *arguments, **_kwargs):
        calls.append(arguments[0])
        if arguments[0] == failure:
            raise ValueError("invalid or missing exact tag")
        return ""

    monkeypatch.setattr(guard, "_git", run)
    with pytest.raises(ValueError, match="invalid or missing"):
        guard.push_tags(tmp_path, ["v1.1.4"], SHA)
    assert "update-ref" not in calls and "push" not in calls


@pytest.mark.parametrize("failure", [None, "push", "cleanup"])
def test_push_tags_cli_preserves_objects_and_always_cleans_staging(
    tmp_path, monkeypatch, capsys, failure
):
    tags = {"v1.1.4": SHA, "v1.1.4-python3.11": "b" * 40}
    prefix = "refs/synapseml-release-push/" + "c" * 32 + "/"
    transactions = []

    def run(_repo, *arguments, input_text=None):
        if arguments[0] == "check-ref-format":
            return ""
        if arguments[0] == "show-ref":
            return tags[arguments[-1].removeprefix("refs/tags/")]
        if arguments[0] == "rev-parse":
            return SHA
        if arguments[0] == "update-ref":
            transactions.append(input_text)
            if failure == "cleanup" and "\ndelete " in input_text:
                raise ValueError("synthetic cleanup rejection")
            return ""
        assert arguments == (
            "push",
            "--atomic",
            "--no-follow-tags",
            "--porcelain",
            "origin",
            prefix + "*:refs/tags/*",
        )
        if failure == "push":
            raise ValueError("synthetic atomic push rejection")
        return ""

    monkeypatch.setattr(guard, "_git", run)
    monkeypatch.setattr(guard.uuid, "uuid4", lambda: SimpleNamespace(hex="c" * 32))
    result = guard.main(
        [
            "push-tags",
            "--repo",
            str(tmp_path),
            "--tag",
            "v1.1.4",
            "--tag",
            "v1.1.4-python3.11",
            "--commit",
            SHA,
        ]
    )
    assert transactions == [
        "start\n"
        + "".join(f"{operation} {prefix}{tag} {oid}\n" for tag, oid in tags.items())
        + "prepare\ncommit\n"
        for operation in ("create", "delete")
    ]
    output = capsys.readouterr()
    assert result == (2 if failure else 0)
    if failure:
        if failure == "push":
            assert "synthetic atomic push rejection" in output.err
        else:
            assert "remote tags may already exist" in output.err
            assert prefix in output.err
        assert not output.out
    else:
        assert json.loads(output.out) == {"pushed_tags": list(tags)}
        assert not output.err


def test_git_transactions_use_lf_bytes_without_echoing_stderr(tmp_path, monkeypatch):
    calls = []

    def run(*_args, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(returncode=0, stdout=b"start: ok\n")

    monkeypatch.setattr(guard.subprocess, "run", run)
    assert (
        guard._git(
            tmp_path, "update-ref", "--stdin", input_text="start\nprepare\nabort\n"
        )
        == "start: ok"
    )
    assert calls[0]["input"] == b"start\nprepare\nabort\n"
    assert not calls[0].get("text", False)
    monkeypatch.setattr(
        guard.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=1, stdout=b"", stderr=b"synthetic-do-not-echo"
        ),
    )
    with pytest.raises(ValueError) as error:
        guard._git(tmp_path, "update-ref", "--stdin", input_text="start\n")
    assert "synthetic-do-not-echo" not in str(error.value)


def test_push_tags_rejects_changed_source_before_staging(tmp_path, monkeypatch):
    calls = []

    def run(_repo, *arguments, **_kwargs):
        calls.append(arguments[0])
        if arguments[0] == "show-ref":
            return SHA
        if arguments[0] == "rev-parse":
            return "b" * 40
        return ""

    monkeypatch.setattr(guard, "_git", run)
    with pytest.raises(ValueError, match="approved commit"):
        guard.push_tags(tmp_path, ["v1.1.4"], SHA)
    assert "update-ref" not in calls and "push" not in calls


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


@pytest.mark.parametrize("member", ["scope", "oss_commit"])
def test_maven_payload_rejects_duplicate_members_before_checkout(
    member, monkeypatch, tmp_path, capsys
):
    plan = public_plan()
    raw = json.dumps(matrix.plan_to_dict(plan), separators=(",", ":"))
    original = f'"{member}":{json.dumps("full" if member == "scope" else SHA)}'
    raw = raw.replace(original, f'"{member}":"unapproved",{original}', 1)
    payload = base64.b64encode(raw.encode()).decode()
    with pytest.raises(ValueError, match="duplicate"):
        guard.maven_plan(payload, plan.plan_id, "refs/tags/v1.1.4", SHA)
    for key, value in {
        "RELEASE_PLAN_BASE64": payload,
        "RELEASE_PLAN_ID": plan.plan_id,
        "BUILD_SOURCEBRANCH": "refs/tags/v1.1.4",
        "BUILD_SOURCEVERSION": SHA,
    }.items():
        monkeypatch.setenv(key, value)
    checked = []
    monkeypatch.setattr(
        guard, "validate_checkout", lambda *arguments: checked.append(arguments)
    )
    assert guard.main(["maven", "--repo", str(tmp_path)]) == 2
    assert not checked
    output = capsys.readouterr()
    assert not output.out
    assert "duplicate" in output.err and "unapproved" not in output.err


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


@pytest.mark.parametrize(
    "suffix",
    ["-9.9.9.jar", ".1.jar", "-SNAPSHOT.jar", "-spark4.0.jar", ".jar.exe"],
)
def test_maven_receipt_rejects_unapproved_artifact_names(staged_maven, suffix):
    plan, target, _, output, wheel = staged_maven
    module = f"synapseml_{target.scala}"
    (output / module / f"{module}-{target.oss_maven_version}{suffix}").write_bytes(
        b"unexpected output"
    )
    with pytest.raises(ValueError, match="artifact"):
        guard.maven_receipt(plan, target, output, 123, pypi_wheel=wheel)


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
