# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import copy
import http.client
import io
import json
import os
import subprocess
import urllib.error
import urllib.request
import zipfile
import zlib
from pathlib import Path

import pytest

import release_dbc as dbc
import release_matrix as matrix


def notebook():
    return {
        "nbformat": 4,
        "nbformat_minor": 4,
        "metadata": {"kernelspec": {"language": "python"}},
        "cells": [
            {"cell_type": "markdown", "source": ["# Example\n"], "metadata": {}},
            {
                "cell_type": "code",
                "source": ["print(1)\n"],
                "metadata": {"execution": {"timestamp": "old"}},
                "execution_count": 9,
                "outputs": [{"output_type": "stream", "name": "stdout", "text": "1"}],
            },
        ],
    }


def archive_bytes(version, notebooks=None, directories=()):
    notebooks = notebooks or {"Example.ipynb": notebook()}
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.mf", '{"version":"Manifest"}')
        for directory in directories:
            archive.writestr(directory, b"")
        for path, value in notebooks.items():
            commands = [
                {
                    "command": ("%md\n" if cell["cell_type"] == "markdown" else "")
                    + "".join(cell["source"]),
                    "results": {"type": "listResults", "data": []},
                }
                for cell in value["cells"]
            ]
            archive.writestr(
                f"SynapseMLExamplesv{version}/{path}.python",
                json.dumps(
                    {
                        "version": "NotebookV1",
                        "language": "python",
                        "commands": commands,
                    }
                ),
            )
    return output.getvalue()


@pytest.mark.parametrize(
    "directory",
    [
        "../outside/",
        "outside/",
        "/SynapseMLExamplesv1.2.0/outside/",
        "SynapseMLExamplesv1.2.0/../outside/",
        "SynapseMLExamplesv1.2.0/nested/../../outside/",
        "SynapseMLExamplesv1.2.0-extra/",
        "SynapseMLExamplesv1.2.0-spark4.1/",
    ],
)
def test_archive_rejects_unsafe_directory_entries(directory):
    data = archive_bytes("1.2.0", directories=[directory])
    with pytest.raises(ValueError, match="unexpected|unsafe"):
        dbc.validate_archive(data, "1.2.0")


def test_backslash_directory_validation_matches_zip_normalization():
    directory = "SynapseMLExamplesv1.2.0/nested\\outside/"
    data = archive_bytes("1.2.0", directories=[directory])
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        assert archive.infolist()[1].filename == directory.replace(os.sep, "/")
    # Windows zipfile normalizes the fixture into a valid nested directory.
    if os.sep == "\\":
        assert dbc.validate_archive(data, "1.2.0") == 1
    else:
        with pytest.raises(ValueError, match="unsafe"):
            dbc.validate_archive(data, "1.2.0")


@pytest.mark.parametrize("version", ["1.2.0", "1.2.0-spark4.0", "1.2.0-spark4.1"])
def test_archive_accepts_root_and_nested_directory_entries(version):
    root = f"SynapseMLExamplesv{version}/"
    notebooks = {"nested/Example.ipynb": notebook()}
    data = archive_bytes(version, notebooks, [root, root + "nested/"])
    assert dbc.validate_archive(data, version, notebooks) == 1


def write_bundle(directory, plan, target):
    directory.mkdir()
    data = archive_bytes(target.oss_maven_version)
    name = dbc.public_dbc_name(target.oss_maven_version)
    (directory / name).write_bytes(data)
    record = {
        "plan_id": plan.plan_id,
        "source_commit": target.oss_commit,
        "version": target.oss_maven_version,
        "source_digest": "f" * 64,
        "path": f"dbcs/{name}",
        "sha256": dbc.digest(data),
        "size": len(data),
        "notebook_count": 1,
        "databricks_roundtrip": True,
        "notebook_execution": False,
    }
    (directory / "dbc-provenance.json").write_text(json.dumps(record))
    return data, record


@pytest.fixture
def selected():
    plan = matrix.build_plan(
        "1.2.0", target_keys=["master"], oss_commits={"master": "a" * 40}
    )
    return plan, plan.targets[0]


@pytest.fixture
def staged(tmp_path, selected):
    plan, target = selected
    directory = tmp_path / "archive"
    data, record = write_bundle(directory, plan, target)
    return directory, plan, target, data, record


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("Notebook unit tests must not use network or Azure CLI")

    monkeypatch.setattr(urllib.request.OpenerDirector, "open", forbidden)
    monkeypatch.setattr(dbc, "azure", forbidden)


class FakeWorkspace:
    def __init__(self, corrupt=False, fail_export=False):
        self.notebooks = {}
        self.calls = []
        self.corrupt = corrupt
        self.fail_export = fail_export

    def call(self, endpoint, body):
        self.calls.append((endpoint, body))
        if endpoint == "delete":
            self.notebooks.clear()
        return {}

    def import_content(self, path, fmt, data):
        self.calls.append(("import", {"path": path, "format": fmt}))
        if fmt == "JUPYTER":
            self.notebooks[path] = json.loads(data)
        else:
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                for name in archive.namelist():
                    if name == "manifest.mf":
                        continue
                    relative = name.split("/", 1)[1][: -len(".python")]
                    obj = json.loads(archive.read(name))
                    restored = []
                    for command in obj["commands"]:
                        content = command["command"]
                        markdown = content.startswith("%md\n")
                        restored.append(
                            {
                                "cell_type": "markdown" if markdown else "code",
                                "source": content[4:] if markdown else content,
                                "outputs": [],
                            }
                        )
                    self.notebooks[f"{path}/{relative}"] = {"cells": restored}

    def export(self, path, fmt):
        if self.fail_export:
            raise ValueError("synthetic export failure")
        if fmt == "DBC":
            return archive_bytes(
                path.rsplit("/", 1)[1].removeprefix("SynapseMLExamplesv"),
                {p[len(path) + 1 :]: n for p, n in self.notebooks.items()},
            )
        result = copy.deepcopy(self.notebooks[path])
        if self.corrupt:
            result["cells"][0]["source"] = "corrupted source"
        return json.dumps(result).encode()


def test_unsafe_directory_is_rejected_before_databricks_import(monkeypatch):
    workspace = FakeWorkspace()
    data = archive_bytes("1.2.0", directories=["SynapseMLExamplesv1.2.0/../outside/"])

    def forbidden_import(*_):
        pytest.fail("An unsafe archive must not reach Databricks import")

    monkeypatch.setattr(workspace, "import_content", forbidden_import)
    with pytest.raises(ValueError, match="unsafe"):
        dbc.roundtrip_archive(
            workspace, "1.2.0", {"Example.ipynb": notebook()}, existing=data
        )
    assert workspace.calls[-1][0] == "delete"
    assert workspace.calls[-1][1]["path"].startswith("/Shared/synapseml-dbc-release-")


def test_every_notebook_roundtrips_and_temporary_folder_is_removed():
    workspace = FakeWorkspace()
    notebooks = {"a/Example.ipynb": notebook(), "b/Other.ipynb": notebook()}
    data = dbc.roundtrip_archive(workspace, "1.2.0", notebooks)
    assert dbc.validate_archive(data, "1.2.0", notebooks) == 2
    assert workspace.calls[-1][0] == "delete"
    assert workspace.calls[-1][1]["path"].startswith("/Shared/synapseml-dbc-release-")
    assert workspace.calls[-1][1]["recursive"] is True
    assert not workspace.notebooks
    assert not any(
        endpoint == "mkdirs" and body["path"].endswith("/roundtrip")
        for endpoint, body in workspace.calls
    )


@pytest.mark.parametrize("failure", ["changed-cell", "export-failed"])
def test_roundtrip_failure_still_cleans_only_its_owned_folder(failure):
    workspace = FakeWorkspace(
        corrupt=failure == "changed-cell", fail_export=failure == "export-failed"
    )
    with pytest.raises(ValueError):
        dbc.roundtrip_archive(workspace, "1.2.0", {"Example.ipynb": notebook()})
    assert workspace.calls[-1][0] == "delete"
    assert not workspace.notebooks


def test_roundtrip_preserves_significant_markdown_whitespace():
    original = notebook()
    original["cells"][0]["source"] = "    print(1)\n"

    class DedentingWorkspace(FakeWorkspace):
        def export(self, path, fmt):
            result = super().export(path, fmt)
            if fmt == "JUPYTER":
                changed = json.loads(result)
                changed["cells"][0]["source"] = "print(1)\n"
                return json.dumps(changed).encode()
            return result

    workspace = DedentingWorkspace()
    with pytest.raises(ValueError, match="changed notebook content"):
        dbc.roundtrip_archive(workspace, "1.2.0", {"Example.ipynb": original})
    assert workspace.calls[-1][0] == "delete"


def test_source_is_read_from_commit_and_saved_outputs_are_removed(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args):
        return subprocess.check_output(["git", "-C", str(repo), *args])

    git("init", "-q")
    docs = repo / "docs"
    docs.mkdir()
    path = docs / "Example.ipynb"
    path.write_text(json.dumps(notebook()))
    git("add", ".")
    git(
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "-qm",
        "source",
    )
    commit = git("rev-parse", "HEAD").decode().strip()
    path.write_text("not the approved notebook")
    prepared = dbc.prepare_notebooks(repo, commit)
    assert prepared["Example.ipynb"]["cells"][1]["outputs"] == []
    assert prepared["Example.ipynb"]["cells"][1]["execution_count"] is None
    assert prepared["Example.ipynb"]["cells"][1]["metadata"] == {}
    assert dbc.cells(prepared["Example.ipynb"]) == dbc.cells(notebook())


@pytest.mark.parametrize(
    "mutation", ["wrong-plan", "wrong-source", "changed-file", "not-validated"]
)
def test_staging_refuses_unapproved_or_changed_archives(staged, mutation):
    directory, plan, target, data, record = staged
    if mutation == "wrong-plan":
        record["plan_id"] = "0" * 64
    elif mutation == "wrong-source":
        record["source_commit"] = "b" * 40
    elif mutation == "changed-file":
        record["sha256"] = "0" * 64
    else:
        record["databricks_roundtrip"] = False
    (directory / "dbc-provenance.json").write_text(json.dumps(record))
    with pytest.raises(ValueError, match="validated release identity"):
        dbc.staged_archive(directory, plan, target)


def test_publication_is_no_overwrite_and_checks_anonymous_download(staged, monkeypatch):
    directory, plan, target, data, record = staged
    lookups = iter([None, (data, record)])
    monkeypatch.setattr(dbc, "fetch_public_archive", lambda *_: next(lookups))
    calls = []
    monkeypatch.setattr(dbc, "azure", lambda *args: calls.append(args))
    result = dbc.publish_archive(directory, plan, target)
    assert result == {k: record[k] for k in ("path", "sha256", "size")}
    command = calls[0]
    assert command[command.index("--overwrite") + 1] == "false"
    assert command[command.index("--auth-mode") + 1] == "login"
    assert "--account-key" not in command
    assert f"plan_id={plan.plan_id}" in command


def test_unsafe_directory_is_rejected_before_publication(staged):
    directory, plan, target, _, record = staged
    data = archive_bytes(
        target.oss_maven_version,
        directories=[f"SynapseMLExamplesv{target.oss_maven_version}/../outside/"],
    )
    (directory / dbc.public_dbc_name(target.oss_maven_version)).write_bytes(data)
    record.update(sha256=dbc.digest(data), size=len(data))
    (directory / "dbc-provenance.json").write_text(json.dumps(record))
    with pytest.raises(ValueError, match="unsafe"):
        dbc.publish_archive(directory, plan, target)


@pytest.mark.parametrize("changed", [False, True])
def test_existing_archive_is_never_overwritten(staged, monkeypatch, changed):
    directory, plan, target, data, record = staged
    monkeypatch.setattr(
        dbc,
        "fetch_public_archive",
        lambda *_: (data + (b"x" if changed else b""), record),
    )
    if changed:
        with pytest.raises(ValueError, match="does not match"):
            dbc.publish_archive(directory, plan, target)
    else:
        assert (
            dbc.publish_archive(directory, plan, target)["sha256"] == record["sha256"]
        )


def test_public_lookup_requires_source_binding_and_matching_bytes(monkeypatch):
    data = archive_bytes("1.2.0")

    class Response(io.BytesIO):
        headers = {
            "x-ms-meta-sha256": dbc.digest(data),
            "x-ms-meta-source_commit": "a" * 40,
        }

    monkeypatch.setattr(
        urllib.request.OpenerDirector, "open", lambda *_args, **_kwargs: Response(data)
    )
    assert dbc.fetch_public_archive("1.2.0", "a" * 40)[0] == data
    with pytest.raises(ValueError, match="source binding"):
        dbc.fetch_public_archive("1.2.0", "b" * 40)
    Response.headers["x-ms-meta-sha256"] = "0" * 64
    with pytest.raises(ValueError, match="source binding"):
        dbc.fetch_public_archive("1.2.0", "a" * 40)


@pytest.mark.parametrize("approved", [False, True])
def test_build_reuses_existing_bytes_only_for_approved_plan_and_roundtrip(
    tmp_path, selected, monkeypatch, approved
):
    plan, target = selected
    notebooks = {"Example.ipynb": notebook()}
    data = archive_bytes("1.2.0")
    metadata = {"plan_id": plan.plan_id, "source_digest": dbc.source_digest(notebooks)}
    if not approved:
        metadata["plan_id"] = "other"
    monkeypatch.setattr(dbc, "prepare_notebooks", lambda *_: notebooks)
    monkeypatch.setattr(dbc, "fetch_public_archive", lambda *_: (data, metadata))
    workspace = FakeWorkspace()
    if not approved:
        with pytest.raises(ValueError, match="different plan"):
            dbc.build_archive(tmp_path, plan, target, tmp_path / "out", workspace)
        assert not (tmp_path / "out").exists()
        assert not workspace.calls
        return
    record = dbc.build_archive(tmp_path, plan, target, tmp_path / "out", workspace)
    assert record["sha256"] == dbc.digest(data)
    assert [
        body["format"] for endpoint, body in workspace.calls if endpoint == "import"
    ] == ["DBC"]
    assert workspace.calls[-1][0] == "delete"


def test_untrusted_workspace_host_is_rejected_before_authentication():
    with pytest.raises(ValueError, match="hostname"):
        dbc.Workspace("example.invalid")


def test_redirect_is_rejected_without_forwarding_authorization():
    with pytest.raises(ValueError, match="redirect"):
        dbc.NoRedirect().redirect_request(
            None, None, 302, "", {}, "https://example.invalid"
        )


@pytest.mark.parametrize("corrupt", [False, True])
def test_cleanup_failure_keeps_primary_error_and_names_owned_path(corrupt, capsys):
    class BrokenCleanup(FakeWorkspace):
        def call(self, endpoint, body):
            if endpoint == "delete":
                raise ValueError("synthetic cleanup failure")
            return super().call(endpoint, body)

    match = "changed notebook content" if corrupt else "workspace cleanup failed"
    with pytest.raises(ValueError, match=match):
        dbc.roundtrip_archive(
            BrokenCleanup(corrupt=corrupt), "1.2.0", {"Example.ipynb": notebook()}
        )
    assert "/Shared/synapseml-dbc-release-" in capsys.readouterr().err


@pytest.mark.parametrize("visible", [True, False])
@pytest.mark.parametrize("timeout", [True, False])
def test_upload_error_is_reconciled_only_with_identical_public_bytes(
    staged, monkeypatch, visible, timeout
):
    directory, plan, target, data, record = staged
    lookups = iter([None, (data, record) if visible else None])
    monkeypatch.setattr(dbc, "fetch_public_archive", lambda *_: next(lookups))

    def fail(*_):
        if timeout:
            raise subprocess.TimeoutExpired("az", 180)
        raise ValueError("synthetic upload failure")

    monkeypatch.setattr(dbc, "azure", fail)
    if visible:
        assert (
            dbc.publish_archive(directory, plan, target)["sha256"] == record["sha256"]
        )
    else:
        with pytest.raises(ValueError, match="No public DBC archive exists"):
            dbc.publish_archive(directory, plan, target)


@pytest.mark.parametrize("operation", ["public", "DBC", "JUPYTER"])
@pytest.mark.parametrize(
    "error",
    [
        urllib.error.URLError("private detail"),
        OSError("private detail"),
        http.client.IncompleteRead(b"private detail"),
    ]
    + [
        urllib.error.HTTPError(
            "https://example.invalid", status, "private detail", {}, None
        )
        for status in (400, 401, 403, 429, 500)
    ],
)
def test_transport_errors_name_operation_without_response_or_token(
    monkeypatch, operation, error
):
    workspace = dbc.Workspace.__new__(dbc.Workspace)
    workspace.host = "https://example.invalid"
    workspace.token = "test-only-auth-value"

    def fail(*_, **__):
        raise error

    monkeypatch.setattr(urllib.request.OpenerDirector, "open", fail)
    public = operation == "public"
    with pytest.raises(
        ValueError,
        match="Public DBC lookup failed"
        if public
        else f"Databricks export {operation}",
    ) as raised:
        if public:
            dbc.fetch_public_archive("1.2.0", "a" * 40)
        else:
            workspace.export("/Shared/Example.ipynb", operation)
    message = str(raised.value)
    if not public:
        assert "/Shared/Example.ipynb" in message
    if isinstance(error, urllib.error.HTTPError):
        assert f"HTTP {error.code}" in message
    assert "private detail" not in message
    assert workspace.token not in message


@pytest.mark.parametrize(
    "obj",
    [[]]
    + [
        {"version": "NotebookV1", "language": "python", "commands": value}
        for value in [None, {}, ["command"], [{"results": []}], [{"results": "text"}]]
    ],
)
def test_malformed_notebook_objects_fail_explicitly(obj):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(
            "SynapseMLExamplesv1.2.0/Example.ipynb.python", json.dumps(obj)
        )
    with pytest.raises(ValueError, match="DBC contains"):
        dbc.validate_archive(output.getvalue(), "1.2.0")


@pytest.mark.parametrize(
    "error",
    [
        zlib.error("deflate"),
        EOFError(),
        RuntimeError("encrypted"),
        NotImplementedError("compression"),
    ],
)
def test_zip_errors_are_reported_as_invalid_archives(monkeypatch, error):
    data = archive_bytes("1.2.0")

    def fail(_):
        raise error

    monkeypatch.setattr(zipfile.ZipFile, "testzip", fail)
    with pytest.raises(ValueError, match="Invalid Databricks archive"):
        dbc.validate_archive(data, "1.2.0")


def test_current_committed_notebooks_are_admissible():
    repo = Path(__file__).resolve().parents[2]
    prepared = dbc.prepare_notebooks(repo, "HEAD")
    assert prepared
    assert all(
        not cell.get("outputs") for n in prepared.values() for cell in n["cells"]
    )


@pytest.mark.parametrize("command", ["full-release", "push-tags"])
def test_notebook_preflight_blocks_release_tags(command, tmp_path, monkeypatch, capsys):
    import release_guard as guard

    checked = []

    def reject(repo, commit):
        checked.append((repo, commit))
        raise ValueError("Unsupported notebook cell")

    def no_push(*_):
        pytest.fail("Tag publication must not run after notebook rejection")

    monkeypatch.setattr(dbc, "prepare_notebooks", reject)
    monkeypatch.setattr(guard, "push_tags", no_push)
    monkeypatch.setattr(
        guard, "_remote_refs", lambda _r, _k, refs: {r: "a" * 40 for r in refs}
    )
    args = [command, "--repo", str(tmp_path)]
    args += (
        ["--version", "1.2.0"]
        if command == "full-release"
        else ["--tag", "v1.2.0-spark4.1", "--commit", "a" * 40]
    )
    assert guard.main(args) == 2
    assert checked == [(tmp_path, "HEAD" if command == "full-release" else "a" * 40)]
    assert "Unsupported notebook cell" in capsys.readouterr().err
