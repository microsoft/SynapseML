# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
"""Build, round-trip and publish source-bound Databricks notebook archives."""

import argparse
import base64
import hashlib
import http.client
import io
import json
import os
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile
import zlib
from pathlib import Path, PurePosixPath

from release_config import strict_json
from verify_release import DBC_BASE, public_dbc_name

MAX_BYTES = 10 * 1024 * 1024
MAX_EXPANDED_BYTES = 100 * 1024 * 1024
ADB_RESOURCE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("Release notebook requests must not redirect")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def run(command, cwd=None):
    result = subprocess.run(
        command, cwd=cwd, capture_output=True, check=False, timeout=180
    )
    if result.returncode:
        # CLI output can contain credentials or request bodies.
        raise ValueError(f"{Path(command[0]).stem} command failed")
    return result.stdout


def azure(*arguments):
    executable = shutil.which("az")
    if executable is None:
        raise ValueError("Azure CLI is required for notebook release operations")
    return run([executable, *arguments, "--only-show-errors"])


def fetch_public_archive(version, commit):
    url = f"{DBC_BASE}/{public_dbc_name(version)}"
    try:
        with urllib.request.build_opener(NoRedirect()).open(
            url, timeout=60
        ) as response:
            data = response.read(MAX_BYTES + 1)
            metadata = {
                key: response.headers.get(f"x-ms-meta-{key}")
                for key in ("sha256", "source_commit", "plan_id", "source_digest")
            }
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise ValueError(f"Public DBC lookup failed: HTTP {error.code}") from None
    except (OSError, http.client.HTTPException) as error:
        raise ValueError(f"Public DBC lookup failed: {type(error).__name__}") from None
    if (
        not data
        or len(data) > MAX_BYTES
        or metadata["source_commit"] != commit
        or metadata["sha256"] != digest(data)
    ):
        raise ValueError("Published DBC content or source binding is invalid")
    validate_archive(data, version)
    return data, metadata


def text(cell):
    source = cell["source"]
    if isinstance(source, list) and all(isinstance(item, str) for item in source):
        return "".join(source)
    if isinstance(source, str):
        return source
    raise ValueError("Notebook cell source must be text")


def cells(notebook):
    return [
        (cell["cell_type"], text(cell).replace("\r\n", "\n"))
        for cell in notebook["cells"]
        if text(cell).strip()
    ]


def prepare_notebooks(repo, commit):
    paths = (
        run(
            ["git", "ls-tree", "-r", "-z", "--name-only", commit, "--", "docs"],
            cwd=repo,
        )
        .decode("utf-8")
        .split("\0")
    )
    notebooks = {}
    for path in paths:
        if not path.endswith(".ipynb"):
            continue
        relative = str(PurePosixPath(path).relative_to("docs"))
        if "\\" in relative or ".." in PurePosixPath(relative).parts:
            raise ValueError("Unsafe source notebook path")
        raw = run(["git", "show", f"{commit}:{path}"], cwd=repo)
        source = strict_json(raw)
        if source.get("nbformat") != 4 or not isinstance(source.get("cells"), list):
            raise ValueError(f"Unsupported notebook format: {relative}")
        metadata = source.get("metadata", {})
        language = metadata.get("language_info", {}).get("name") or metadata.get(
            "kernelspec", {}
        ).get("language")
        if language != "python":
            raise ValueError(f"Unsupported notebook language: {relative}")
        prepared = []
        for cell in source["cells"]:
            if cell.get("cell_type") not in ("code", "markdown") or cell.get(
                "attachments"
            ):
                raise ValueError(f"Unsupported notebook cell: {relative}")
            content = text(cell)
            if re.search(
                r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"
                r"|AccountKey=[A-Za-z0-9+/]{40,}={0,2}"
                r"|[?&]sig=[A-Za-z0-9%+/]{30,}"
                r"|\b(?:sk-|ghp_|github_pat_)[A-Za-z0-9_-]{30,}",
                content,
            ):
                raise ValueError(f"Potential credential in notebook: {relative}")
            clean = {"cell_type": cell["cell_type"], "source": content, "metadata": {}}
            if cell["cell_type"] == "code":
                clean.update(outputs=[], execution_count=None)
            prepared.append(clean)
        notebooks[relative] = {
            "nbformat": 4,
            "nbformat_minor": 4,
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3",
                },
                "language_info": {"name": "python"},
            },
            "cells": prepared,
        }
    if not notebooks:
        raise ValueError("Release source contains no notebooks")
    return notebooks


def source_digest(notebooks):
    return digest(json.dumps(notebooks, sort_keys=True, ensure_ascii=True).encode())


def validate_archive(data, version, notebooks=None):
    if not data or len(data) > MAX_BYTES:
        raise ValueError("DBC archive is empty or exceeds the supported size")
    expected_prefix = public_dbc_name(version)[:-4] + "/"
    observed = set()
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            entries = archive.infolist()
            if (
                len(entries) > 10000
                or sum(entry.file_size for entry in entries) > MAX_EXPANDED_BYTES
                or len({entry.filename for entry in entries}) != len(entries)
            ):
                raise ValueError("DBC archive has duplicate or oversized entries")
            if archive.testzip() is not None:
                raise ValueError("DBC archive failed CRC validation")
            for entry in entries:
                if entry.is_dir() or entry.filename == "manifest.mf":
                    continue
                if not entry.filename.startswith(
                    expected_prefix
                ) or not entry.filename.endswith(".python"):
                    raise ValueError("DBC contains an unexpected notebook path")
                relative = entry.filename[len(expected_prefix) : -len(".python")]
                if "\\" in relative or ".." in PurePosixPath(relative).parts:
                    raise ValueError("DBC contains an unsafe notebook path")
                obj = strict_json(archive.read(entry))
                if (
                    not isinstance(obj, dict)
                    or obj.get("version") != "NotebookV1"
                    or obj.get("language") != "python"
                    or not isinstance(obj.get("commands"), list)
                ):
                    raise ValueError("DBC contains an unsupported notebook object")
                for command in obj["commands"]:
                    if not isinstance(command, dict):
                        raise ValueError("DBC contains an invalid notebook command")
                    result = command.get("results")
                    if result is not None and not isinstance(result, dict):
                        raise ValueError("DBC contains an invalid notebook result")
                    if result and not (
                        result.get("type") == "listResults"
                        and not any(v for k, v in result.items() if k != "type")
                    ):
                        raise ValueError("DBC contains execution outputs")
                observed.add(relative)
    except (
        zipfile.BadZipFile,
        KeyError,
        TypeError,
        zlib.error,
        EOFError,
        RuntimeError,
    ) as error:
        raise ValueError("Invalid Databricks archive") from error
    if not observed or (notebooks is not None and observed != set(notebooks)):
        raise ValueError("DBC notebook inventory differs from the release source")
    return len(observed)


class Workspace:
    def __init__(self, host):
        if not re.fullmatch(r"adb-[0-9]+\.[0-9]+\.azuredatabricks\.net", host):
            raise ValueError("Expected an Azure Databricks workspace hostname")
        self.host = f"https://{host}"
        self.token = (
            azure(
                "account",
                "get-access-token",
                "--resource",
                ADB_RESOURCE,
                "--query",
                "accessToken",
                "-o",
                "tsv",
            )
            .decode()
            .strip()
        )
        if not self.token:
            raise ValueError("Azure CLI returned no Databricks access token")

    def call(self, endpoint, body=None, query=None):
        details = body if body is not None else query or {}
        label = f"{endpoint} {details.get('format', '')} {json.dumps(details.get('path', ''))}"
        url = f"{self.host}/api/2.0/workspace/{endpoint}"
        if query:
            url += "?" + urllib.parse.urlencode(query)
        request = urllib.request.Request(
            url,
            data=json.dumps(body).encode() if body is not None else None,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            method="POST" if body is not None else "GET",
        )
        try:
            with urllib.request.build_opener(NoRedirect()).open(
                request, timeout=120
            ) as response:
                content = response.read(2 * MAX_BYTES + 1)
                if len(content) > 2 * MAX_BYTES:
                    raise ValueError("Databricks response exceeds the supported size")
                return strict_json(content)
        except urllib.error.HTTPError as error:
            if endpoint == "delete" and error.code == 404:
                return {}
            raise ValueError(f"Databricks {label} failed: HTTP {error.code}") from None
        except (OSError, http.client.HTTPException) as error:
            raise ValueError(
                f"Databricks {label} failed: {type(error).__name__}"
            ) from None

    def export(self, path, fmt):
        result = self.call("export", query={"path": path, "format": fmt})
        return base64.b64decode(result["content"], validate=True)

    def import_content(self, path, fmt, data):
        return self.call(
            "import",
            {
                "path": path,
                "format": fmt,
                "overwrite": False,
                "content": base64.b64encode(data).decode("ascii"),
            },
        )


def roundtrip_archive(workspace, version, notebooks, existing=None):
    staging = f"/Shared/synapseml-dbc-release-{uuid.uuid4().hex}"
    root = f"{staging}/{public_dbc_name(version)[:-4]}"
    restored = f"{staging}/roundtrip"
    try:
        workspace.call("mkdirs", {"path": staging})
        if existing is None:
            for directory in sorted({str(PurePosixPath(p).parent) for p in notebooks}):
                workspace.call("mkdirs", {"path": f"{root}/{directory}"})
            for path, notebook in notebooks.items():
                workspace.import_content(
                    f"{root}/{path}", "JUPYTER", json.dumps(notebook).encode()
                )
            data = workspace.export(root, "DBC")
        else:
            data = existing
        validate_archive(data, version, notebooks)
        # DBC import creates its destination; it must not already exist.
        workspace.import_content(restored, "DBC", data)
        for path, notebook in notebooks.items():
            actual = strict_json(workspace.export(f"{restored}/{path}", "JUPYTER"))
            if cells(actual) != cells(notebook) or any(
                cell.get("outputs") for cell in actual["cells"]
            ):
                raise ValueError(f"DBC round-trip changed notebook content: {path}")
        return data
    finally:
        failure = sys.exc_info()[0]
        try:
            workspace.call("delete", {"path": staging, "recursive": True})
        except (ValueError, OSError, http.client.HTTPException) as error:
            print(
                f"error: Databricks cleanup failed for {staging} "
                f"({type(error).__name__}); remove this owned folder manually",
                file=sys.stderr,
            )
            if failure is None:
                raise ValueError(
                    "DBC validation succeeded but workspace cleanup failed"
                ) from None


def build_archive(repo, plan, target, output, workspace):
    output = Path(output)
    if output.exists() or output.is_symlink():
        raise ValueError("DBC staging directory already exists; retain prior evidence")
    notebooks = prepare_notebooks(repo, target.oss_commit)
    expected_source = source_digest(notebooks)
    existing = fetch_public_archive(target.oss_maven_version, target.oss_commit)
    if existing is not None and (
        existing[1]["plan_id"] != plan.plan_id
        or existing[1]["source_digest"] != expected_source
    ):
        raise ValueError("Existing DBC belongs to a different plan or notebook source")
    data = roundtrip_archive(
        workspace,
        target.oss_maven_version,
        notebooks,
        existing[0] if existing is not None else None,
    )
    output.mkdir(parents=True)
    name = public_dbc_name(target.oss_maven_version)
    (output / name).write_bytes(data)
    record = {
        "plan_id": plan.plan_id,
        "source_commit": target.oss_commit,
        "version": target.oss_maven_version,
        "source_digest": expected_source,
        "path": f"dbcs/{name}",
        "sha256": digest(data),
        "size": len(data),
        "notebook_count": len(notebooks),
        "databricks_roundtrip": True,
        "notebook_execution": False,
    }
    (output / "dbc-provenance.json").write_text(
        json.dumps(record, indent=2) + "\n", encoding="utf-8"
    )
    return record


def staged_archive(directory, plan, target):
    directory = Path(directory)
    name = public_dbc_name(target.oss_maven_version)
    path, record_path = directory / name, directory / "dbc-provenance.json"
    if directory.is_symlink() or path.is_symlink() or record_path.is_symlink():
        raise ValueError("DBC staging must not use symbolic links")
    record = strict_json(record_path.read_bytes())
    data = path.read_bytes()
    count = validate_archive(data, target.oss_maven_version)
    if (
        record.get("plan_id") != plan.plan_id
        or record.get("source_commit") != target.oss_commit
        or record.get("version") != target.oss_maven_version
        or record.get("path") != f"dbcs/{name}"
        or record.get("sha256") != digest(data)
        or record.get("size") != len(data)
        or record.get("notebook_count") != count
        or record.get("databricks_roundtrip") is not True
        or not re.fullmatch(r"[0-9a-f]{64}", record.get("source_digest", ""))
    ):
        raise ValueError("DBC staging differs from its validated release identity")
    return path, data, record


def publish_archive(directory, plan, target):
    path, data, record = staged_archive(directory, plan, target)
    existing = fetch_public_archive(target.oss_maven_version, target.oss_commit)
    upload_error = None
    if existing is None:
        try:
            azure(
                "storage",
                "blob",
                "upload",
                "--account-name",
                "mmlspark",
                "--container-name",
                "dbcs",
                "--name",
                path.name,
                "--file",
                str(path),
                "--auth-mode",
                "login",
                "--overwrite",
                "false",
                "--content-type",
                "application/zip",
                "--validate-content",
                "--no-progress",
                "--metadata",
                f"source_commit={target.oss_commit}",
                f"plan_id={plan.plan_id}",
                f"source_digest={record['source_digest']}",
                f"sha256={record['sha256']}",
                f"notebook_count={record['notebook_count']}",
                "-o",
                "none",
            )
        except (ValueError, OSError, subprocess.TimeoutExpired) as error:
            upload_error = error
            print(
                f"warning: DBC upload reported {type(error).__name__}; "
                "checking public bytes before accepting publication",
                file=sys.stderr,
            )
    published = fetch_public_archive(target.oss_maven_version, target.oss_commit)
    if published is None:
        raise ValueError(
            "No public DBC archive exists after upload; check the service "
            "connection's dbcs write access and retry the failed Release job"
        ) from upload_error
    if (
        published[0] != data
        or published[1]["plan_id"] != plan.plan_id
        or published[1]["source_digest"] != record["source_digest"]
    ):
        raise ValueError(
            "Public DBC download does not match the approved archive; "
            "the conflicting version will not be overwritten"
        ) from upload_error
    return {key: record[key] for key in ("path", "sha256", "size")}


def main(argv=None):
    from release_guard import maven_plan, validate_checkout
    from release_matrix import PUBLIC_SCHEMA_VERSION

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "publish"))
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--repo", type=Path, default=Path("."))
    args = parser.parse_args(argv)
    try:
        plan, target = maven_plan(
            os.environ.get("RELEASE_PLAN_BASE64", ""),
            os.environ.get("RELEASE_PLAN_ID", ""),
            os.environ.get("BUILD_SOURCEBRANCH", ""),
            os.environ.get("BUILD_SOURCEVERSION", ""),
        )
        if plan.schema_version != PUBLIC_SCHEMA_VERSION:
            raise ValueError("This approved plan does not authorize DBC publication")
        validate_checkout(args.repo, target)
        if args.repo.resolve() in args.directory.resolve().parents:
            raise ValueError("DBC staging must remain outside the source checkout")
        if args.command == "build":
            result = build_archive(
                args.repo,
                plan,
                target,
                args.directory,
                Workspace(os.environ.get("MML_ADB_WORKSPACE_HOST", "")),
            )
        else:
            result = publish_archive(args.directory, plan, target)
        print(json.dumps(result, indent=2))
        return 0
    except (ValueError, OSError, KeyError, subprocess.TimeoutExpired) as error:
        # Do not print HTTP exception bodies, CLI output or authentication headers.
        print(f"error: DBC release failed ({type(error).__name__})", file=sys.stderr)
        if isinstance(error, ValueError):
            print(str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
