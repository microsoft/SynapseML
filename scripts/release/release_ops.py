#!/usr/bin/env python3
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
"""Approve, queue and reconcile the missing operations in a bound release plan.

All commands default to read-only remote access. Local state is an audit record,
not publication evidence: existing packages still need a matching Azure run.
An interrupted submission is never retried automatically. Reconcile its known
run with ``resume --adopt ACTION_ID=BUILD_ID --apply --approve-plan PLAN_ID``.
Human pipeline approvals, tag creation, GitHub releases and rollout are separate.
"""

from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import hmac
import io
import json
import os
import re
import socket
import stat
import shutil
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_matrix as matrix  # noqa: E402
import verify_release as verify  # noqa: E402

STATE_VERSION = 2
MAX_JSON_BYTES = 16 * 1024 * 1024
MAX_RECEIPT_BYTES = 2 * 1024 * 1024
MAX_LOCK_BYTES = 16384
MAX_RETRY_ATTEMPTS = 16
MAX_CLAIM_CANDIDATES = 128
MAX_ABSENCE_PAGES = 100
ABSENCE_PAGE_SIZE = 100
SHA_RE = re.compile(r"[0-9a-f]{40}\Z")
HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
GUID_RE = re.compile(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\Z")
BLOB_ID_RE = re.compile(r"[0-9a-fA-F]{64}(?:[0-9a-fA-F]{2})?\Z")
PUBLISHER_RECEIPT_RE = re.compile(
    r"release-provenance-((?:SynapseML|Internal)_(?:Pip|Upack)_"
    r"(py311|py312|py313|default|spark4|spark41))-(Build_\2)\Z"
)
AUXILIARY_ARTIFACT_RE = re.compile(
    r"_failed_[0-9]+\Z|_sdl_analysis\Z|_signinglogs|"
    r"\A1espt-autobaseline-|\Adrop_ReleasePolicy_Validate\Z",
    re.IGNORECASE,
)
STATES = {"planned", "existing", "unknown", "pending", "failed", "complete"}
ADO_BASE = "https://dev.azure.com/msdata/A365/_apis"
ADO_PROJECT_ID = "b9b2accc-2d1c-45b3-9d24-0eb5d78cc47f"
PRODUCTION_FEED_IDS = {
    "pip": "40ba8cc6-45a4-4580-bf84-257ce1012263",
    "upack": "cdb0dc93-5fbe-4f25-b8ba-ca322c3fcc03",
}
COUNTERS = ("SYNAPSEML_PATCH_VERSION", "SYNAPSEML_INTERNAL_PATCH_VERSION")
FAMILY_ORDER = ("maven", "pip", "upack")


class ReleaseError(RuntimeError):
    """A safe-to-display validation failure, without raw service diagnostics."""


def now():
    return datetime.now(timezone.utc).isoformat()


def canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("utf-8")


def _digest(value, field):
    return hashlib.sha256(
        canonical({key: item for key, item in value.items() if key != field})
    ).hexdigest()


def _object_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON member")
        result[key] = value
    return result


def _invalid_constant(_value):
    raise ValueError("nonfinite JSON number")


def _json(value, label):
    try:
        return json.loads(
            value,
            object_pairs_hook=_object_pairs,
            parse_constant=_invalid_constant,
        )
    except (TypeError, ValueError, UnicodeError) as error:
        raise ReleaseError(f"{label} contains invalid JSON") from error


def _positive_id(value):
    return type(value) is int and value > 0


def _commit(value):
    return isinstance(value, str) and SHA_RE.fullmatch(value) and value != "0" * 40


def _time(value, label):
    if isinstance(value, str):
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        # Older Python parsers require three or six fractional digits.
        value = re.sub(
            r"(\d{2}:\d{2}:\d{2})\.(\d+)",
            lambda match: match[1] + "." + match[2][:6].ljust(6, "0"),
            value,
        )
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise ReleaseError(f"{label} has an invalid timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ReleaseError(f"{label} must have a UTC timestamp")
    if parsed > datetime.now(timezone.utc) + timedelta(minutes=5):
        raise ReleaseError(f"{label} is dated in the future")
    return parsed


def _safe_error(error):
    if isinstance(error, ReleaseError):
        return str(error)
    return "Authoritative service query failed; no operation will be retried."


class CommandRunner:
    """Use argv throughout, including Windows Azure CLI's Python installation."""

    def run(self, command):
        if not command or command[0] not in {"az", "gh"}:
            raise ReleaseError("Only the Azure and GitHub CLI transports are supported")
        executable = shutil.which(command[0])
        if not executable:
            raise ReleaseError(
                f"Install {command[0]} and authenticate before preflight"
            )
        prefix = [executable]
        if Path(executable).suffix.lower() in {".cmd", ".bat"}:
            if command[0] != "az":
                raise ReleaseError(
                    "Use the native GitHub CLI executable, not a wrapper"
                )
            python = Path(executable).parent.parent / "python.exe"
            if not python.is_file():
                raise ReleaseError(
                    "Cannot find Azure CLI's bundled python.exe; install the native "
                    "Azure CLI. Shell interpolation of release parameters is disabled."
                )
            prefix = [str(python), "-I", "-m", "azure.cli"]
        try:
            result = subprocess.run(
                prefix + list(command[1:]),
                shell=False,
                capture_output=True,
                text=True,
                timeout=180,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as error:
            raise ReleaseError(
                f"{command[0]} did not return a confirmed response"
            ) from error
        if result.returncode:
            # CLI diagnostics can contain access tokens, signed URLs or request bodies.
            raise ReleaseError(
                f"{command[0]} failed with exit code {result.returncode}; "
                "no confirmed service response"
            )
        return result.stdout

    def json(self, command):
        return _json(self.run(command), f"{command[0]} response")


def _azure_url(url):
    try:
        parts = urllib.parse.urlsplit(url)
        host = (parts.hostname or "").lower()
        allowed = host in {
            "dev.azure.com",
            "feeds.dev.azure.com",
            "msdata.visualstudio.com",
            "vsblob.dev.azure.com",
        } or host.endswith(
            (
                ".vsblob.vsassets.io",
                ".blob.core.windows.net",
                ".artifacts.visualstudio.com",
                ".vsblob.visualstudio.com",
            )
        )
        valid = (
            parts.scheme == "https"
            and allowed
            and not parts.username
            and not parts.password
            and parts.port in (None, 443)
        )
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise ReleaseError("Azure returned an untrusted artifact destination")
    return url


class _AzureRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, newurl):
        _azure_url(newurl)
        redirected = super().redirect_request(
            request, response, code, message, headers, newurl
        )
        if redirected is not None:
            # A build artifact can redirect to a signed Azure blob. Never forward
            # the ADO bearer token to another host, even when the host is Azure.
            if urllib.parse.urlsplit(request.full_url).netloc != (
                urllib.parse.urlsplit(newurl).netloc
            ):
                redirected.remove_header("Authorization")
        return redirected


def read_provenance_zip(data):
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            files = [
                item
                for item in archive.infolist()
                if item.filename.replace("\\", "/").rsplit("/", 1)[-1]
                == "release-provenance.json"
                and not item.is_dir()
            ]
            if (
                not files
                or len(files) > 32
                or any(item.file_size > MAX_RECEIPT_BYTES for item in files)
                or sum(item.file_size for item in files) > MAX_JSON_BYTES
            ):
                raise ReleaseError("Azure artifact has missing or oversized provenance")
            return [_json(archive.read(item), "provenance artifact") for item in files]
    except (OSError, ValueError, zipfile.BadZipFile, RuntimeError) as error:
        if isinstance(error, ReleaseError):
            raise
        raise ReleaseError("Azure provenance artifact is not a readable ZIP") from error


class AzureRemote:
    """Read authoritative APIs and submit the exact recorded argv through az."""

    def __init__(self, runner=None):
        self.runner = runner or CommandRunner()
        self._token = None
        self._definitions = {}
        self._opener = urllib.request.build_opener(_AzureRedirects())

    def token(self):
        if self._token is None:
            self._token = os.environ.get("ADO_TOKEN")
            if not self._token:
                result = self.runner.json(
                    [
                        "az",
                        "account",
                        "get-access-token",
                        "--resource",
                        verify.ADO_RESOURCE,
                        "--output",
                        "json",
                        "--only-show-errors",
                    ]
                )
                if not isinstance(result, dict) or not isinstance(
                    result.get("accessToken"), str
                ):
                    raise ReleaseError("Azure CLI did not return an access token")
                self._token = result["accessToken"].strip()
            if not self._token:
                raise ReleaseError("Azure CLI returned an empty access token")
        return self._token

    def _get(self, url, binary=False, max_bytes=MAX_JSON_BYTES, include_headers=False):
        _azure_url(url)
        headers = {"User-Agent": "synapseml-release-ops"}
        host = urllib.parse.urlsplit(url).hostname
        if host in {"dev.azure.com", "feeds.dev.azure.com", "msdata.visualstudio.com"}:
            headers["Authorization"] = f"Bearer {self.token()}"
        request = urllib.request.Request(url, headers=headers)
        try:
            with self._opener.open(request, timeout=60) as response:
                body = response.read(max_bytes + 1)
                response_headers = (
                    dict(getattr(response, "headers", {}) or {})
                    if include_headers
                    else None
                )
        except urllib.error.HTTPError as error:
            raise ReleaseError(
                f"Azure read request failed with HTTP {error.code}"
            ) from error
        except (OSError, urllib.error.URLError) as error:
            raise ReleaseError("Azure read request failed") from error
        if len(body) > max_bytes:
            raise ReleaseError(
                "Azure response exceeded the release evidence size limit"
            )
        result = body if binary else _json(body, "Azure API response")
        return (result, response_headers) if include_headers else result

    def github_variables(self):
        variables = []
        total = None
        for page in range(1, 101):
            data = self.runner.json(
                [
                    "gh",
                    "api",
                    "--hostname",
                    "github.com",
                    (
                        "repos/microsoft/SynapseML/actions/variables"
                        f"?per_page=100&page={page}"
                    ),
                ]
            )
            if (
                not isinstance(data, dict)
                or type(data.get("total_count")) is not int
                or not isinstance(data.get("variables"), list)
                or data["total_count"] < 0
                or (total is not None and data["total_count"] != total)
            ):
                raise ReleaseError("SKIP_SPARK40 policy API returned invalid coverage")
            total = data["total_count"]
            variables.extend(data["variables"])
            if len(variables) == total:
                return {"total_count": total, "variables": variables}
            if not data["variables"] or len(variables) > total:
                break
        raise ReleaseError("Could not read complete SKIP_SPARK40 repository policy")

    def inventory(self, plan):
        return verify.run_plan(
            plan, token=self.token(), gh_token=os.environ.get("GH_TOKEN"), skip=None
        )

    def resolve_feed(self, name):
        encoded = urllib.parse.quote(name.rsplit("/", 1)[-1], safe="")
        return self._get(
            "https://feeds.dev.azure.com/msdata/A365/_apis/packaging/"
            f"feeds/{encoded}?api-version=7.1-preview.1"
        )

    def definition(self, pipeline_id):
        if pipeline_id not in self._definitions:
            self._definitions[pipeline_id] = self._get(
                f"{ADO_BASE}/build/definitions/{pipeline_id}?api-version=7.1"
            )
        return self._definitions[pipeline_id]

    def build(self, build_id):
        return self._get(f"{ADO_BASE}/build/builds/{build_id}?api-version=7.1")

    def timeline(self, build_id):
        return self._get(f"{ADO_BASE}/build/builds/{build_id}/timeline?api-version=7.1")

    def _artifact_file(self, build_id, name, file_id, file_name, **kwargs):
        if not isinstance(file_id, str) or not BLOB_ID_RE.fullmatch(file_id):
            raise ReleaseError(
                "Azure pipeline artifact has an invalid manifest/blob ID"
            )
        query = urllib.parse.urlencode(
            {
                "artifactName": name,
                "fileId": file_id,
                "fileName": file_name,
                "api-version": "7.1",
            }
        )
        return self._get(
            f"{ADO_BASE}/build/builds/{build_id}/artifacts?{query}", **kwargs
        )

    def _pipeline_provenance(self, build_id, artifact):
        # Build Artifacts Get File accepts resource.data as the dedup manifest ID.
        # Read its file index, then only receipt blobs, not a potentially huge UPack.
        manifest = self._artifact_file(
            build_id,
            artifact["name"],
            artifact["resource"].get("data"),
            "manifest.json",
        )
        if (
            not isinstance(manifest, dict)
            or manifest.get("manifestFormat") != "1.1.0"
            or not isinstance(manifest.get("items"), list)
            or manifest.get("manifestReferences") != []
            or len(manifest["items"]) > 10000
        ):
            raise ReleaseError("Azure pipeline artifact has an unsupported manifest")
        documents = []
        paths = set()
        for item in manifest["items"]:
            if not isinstance(item, dict) or not isinstance(item.get("path"), str):
                raise ReleaseError("Azure artifact manifest contains an invalid file")
            path = item["path"]
            if path not in {"/release-provenance.json", "release-provenance.json"}:
                continue
            blob = item.get("blob")
            if (
                path in paths
                or len(paths) >= 32
                or not isinstance(blob, dict)
                or type(blob.get("size")) is not int
                or not 0 < blob["size"] <= MAX_RECEIPT_BYTES
            ):
                raise ReleaseError(
                    "Azure artifact has duplicate or oversized provenance"
                )
            paths.add(path)
            body = self._artifact_file(
                build_id,
                artifact["name"],
                blob.get("id"),
                "release-provenance.json",
                binary=True,
                max_bytes=MAX_RECEIPT_BYTES,
            )
            if len(body) != blob["size"]:
                raise ReleaseError(
                    "Azure provenance blob size differs from its manifest"
                )
            documents.append(_json(body, "Azure provenance artifact"))
        return documents

    def provenance(self, build_id):
        data = self._get(
            f"{ADO_BASE}/build/builds/{build_id}/artifacts?api-version=7.1"
        )
        if not isinstance(data, dict) or not isinstance(data.get("value"), list):
            raise ReleaseError("Azure artifact response has no value list")
        covered_drops = set()
        for artifact in data["value"]:
            if not isinstance(artifact, dict) or not isinstance(
                artifact.get("name"), str
            ):
                continue
            match = PUBLISHER_RECEIPT_RE.fullmatch(artifact["name"])
            if match:
                stage, _, job = match.groups()
                covered_drops.update(
                    {f"drop_{stage}_{job}", f"drop_{stage}_Receipt_{job}"}
                )
        documents = []
        names = set()
        for artifact in data["value"]:
            if (
                not isinstance(artifact, dict)
                or not isinstance(artifact.get("name"), str)
                or not artifact["name"]
            ):
                raise ReleaseError("Azure artifact response has an invalid identity")
            name = artifact["name"]
            if name in covered_drops or AUXILIARY_ARTIFACT_RE.search(name):
                continue
            standalone = re.fullmatch(r"release-provenance(?:-[A-Za-z0-9_.-]+)?", name)
            if name in names:
                raise ReleaseError("Azure returned duplicate provenance artifacts")
            names.add(name)
            resource = artifact.get("resource")
            if not isinstance(resource, dict):
                raise ReleaseError("Azure provenance artifact has no resource metadata")
            if resource.get("type") == "PipelineArtifact":
                documents.extend(self._pipeline_provenance(build_id, artifact))
                continue
            if not standalone:
                continue
            if resource.get("type") != "Container" or not isinstance(
                resource.get("downloadUrl"), str
            ):
                raise ReleaseError("Azure provenance artifact has no download URL")
            documents.extend(
                read_provenance_zip(self._get(resource["downloadUrl"], binary=True))
            )
        # A producer can retain identical receipts in more than one primary artifact.
        # Only identical JSON copies collapse; conflicting claims remain for validation.
        return list({canonical(document): document for document in documents}.values())

    def queue(self, command):
        return self.runner.json(command)

    def absence(self, plan, actions, destinations):
        artifacts = _absence_descriptors(plan, actions, destinations)
        checked_at = now()
        for artifact in artifacts:
            protocol = "pypi" if artifact["family"] == "pip" else "upack"
            parameters = {
                "protocolType": protocol,
                "packageNameQuery": artifact["name"],
                "includeAllVersions": "true",
                "includeDeleted": "true",
                "api-version": "7.1-preview.1",
                "$top": str(ABSENCE_PAGE_SIZE),
                "$skip": "0",
            }
            endpoint = (
                f"https://feeds.dev.azure.com/msdata/{artifact['project_id']}"
                f"/_apis/packaging/Feeds/{artifact['feed_id']}/packages"
            )
            seen_tokens = set()
            seen_pages = set()
            offset = 0
            present = False
            for _ in range(MAX_ABSENCE_PAGES):
                response = self._get(
                    endpoint + "?" + urllib.parse.urlencode(parameters),
                    include_headers=True,
                )
                if (
                    not isinstance(response, tuple)
                    or len(response) != 2
                    or not isinstance(response[0], dict)
                    or not isinstance(response[1], dict)
                ):
                    raise ReleaseError("Retry absence API returned an invalid page")
                data, headers = response
                packages = data.get("value")
                if (
                    not isinstance(packages, list)
                    or type(data.get("count")) is not int
                    or data["count"] != len(packages)
                    or len(packages) > ABSENCE_PAGE_SIZE
                ):
                    raise ReleaseError(
                        "Retry absence API omitted complete package coverage"
                    )
                if packages:
                    page_id = hashlib.sha256(canonical(packages)).hexdigest()
                    if page_id in seen_pages:
                        raise ReleaseError("Retry absence pagination repeated a page")
                    seen_pages.add(page_id)
                for package in packages:
                    if (
                        not isinstance(package, dict)
                        or not isinstance(package.get("name"), str)
                        or not package["name"]
                        or not isinstance(package.get("protocolType"), str)
                        or package["protocolType"].casefold() != protocol
                    ):
                        raise ReleaseError(
                            "Retry absence API returned an invalid package"
                        )
                    if package["name"].casefold() != artifact["name"].casefold():
                        continue
                    versions = package.get("versions")
                    if not isinstance(versions, list):
                        raise ReleaseError("Retry absence API omitted package versions")
                    for version in versions:
                        if (
                            not isinstance(version, dict)
                            or not isinstance(version.get("version"), str)
                            or not version["version"]
                            or (
                                "normalizedVersion" in version
                                and not isinstance(version["normalizedVersion"], str)
                            )
                        ):
                            raise ReleaseError(
                                "Retry absence API returned an invalid version"
                            )
                        if artifact["version"] in (
                            version["version"],
                            version.get("normalizedVersion"),
                        ):
                            present = True
                if present:
                    break
                offset += len(packages)
                continuation = next(
                    (
                        value
                        for key, value in headers.items()
                        if isinstance(key, str)
                        and key.casefold() == "x-ms-continuationtoken"
                    ),
                    None,
                )
                if continuation is not None and continuation != "":
                    if (
                        not isinstance(continuation, str)
                        or len(continuation) > 4096
                        or continuation in seen_tokens
                    ):
                        raise ReleaseError("Retry absence pagination is invalid")
                    seen_tokens.add(continuation)
                    parameters.pop("$skip", None)
                    parameters["continuationToken"] = continuation
                elif not packages:
                    break
                else:
                    # A short page is not proof of completion on an offset API.
                    parameters.pop("continuationToken", None)
                    parameters["$skip"] = str(offset)
            else:
                raise ReleaseError("Retry absence pagination exceeded its bound")
            artifact["status"] = "present" if present else "absent"
        return {
            "schema_version": 1,
            "plan_id": plan.plan_id,
            "checked_at": checked_at,
            "artifacts": artifacts,
        }


def _policy(plan, remote):
    if plan.scope != "full":
        return {"checked_at": now(), "required": False}
    data = remote.github_variables()
    if (
        not isinstance(data, dict)
        or type(data.get("total_count")) is not int
        or not isinstance(data.get("variables"), list)
        or data["total_count"] != len(data["variables"])
    ):
        raise ReleaseError("SKIP_SPARK40 policy API returned incomplete coverage")
    values = {}
    for entry in data["variables"]:
        if (
            not isinstance(entry, dict)
            or not isinstance(entry.get("name"), str)
            or not isinstance(entry.get("value"), str)
            or entry["name"] in values
        ):
            raise ReleaseError("SKIP_SPARK40 policy API returned invalid variables")
        values[entry["name"]] = entry["value"]
    value = values.get("SKIP_SPARK40")
    if value is not None and value.strip().lower() == "true":
        raise ReleaseError(
            "SKIP_SPARK40 is true. Full release mutations are forbidden; "
            "resolve the repository policy with its owner first."
        )
    return {
        "checked_at": now(),
        "required": True,
        "skip_spark40": value,
        "source": "github/microsoft/SynapseML/actions/variables",
    }


def _feed_identity(remote, requested):
    try:
        data = remote.resolve_feed(requested)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        raise ReleaseError(
            "Cannot resolve the approved Azure feed destination"
        ) from error
    if (
        not isinstance(data, dict)
        or not isinstance(data.get("id"), str)
        or not GUID_RE.fullmatch(data["id"])
        or not isinstance(data.get("name"), str)
        or not isinstance(data.get("project"), dict)
        or not isinstance(data["project"].get("name"), str)
        or data["project"]["name"].casefold() != matrix.ADO_PROJECT.casefold()
        or not isinstance(data["project"].get("id"), str)
        or data["project"]["id"].lower() != ADO_PROJECT_ID
        or data.get("isDeleted") is True
    ):
        raise ReleaseError("Azure feed API returned an invalid feed identity")
    if data["name"].casefold() != requested.rsplit("/", 1)[-1].casefold():
        raise ReleaseError("Azure feed alias does not match the approved feed name")
    return {
        "requested": requested,
        "id": data["id"].lower(),
        "name": data["name"],
        "project": matrix.ADO_PROJECT,
        "project_id": ADO_PROJECT_ID,
    }


def _destinations(plan, remote):
    selected = {family for family in plan.families if family in {"pip", "upack"}}
    if not selected:
        return {}
    checked = {"pip", "upack"} if plan.mode == "rehearsal" else selected
    resolved = {
        family: _feed_identity(remote, getattr(plan, family + "_feed"))
        for family in sorted(checked)
    }
    if plan.mode == "production" and any(
        feed["id"] != PRODUCTION_FEED_IDS[family] for family, feed in resolved.items()
    ):
        raise ReleaseError(
            "Production feed identity changed; review the destination policy"
        )
    if plan.mode == "rehearsal":
        production = [
            _feed_identity(remote, name)
            for name in (matrix.PIP_FEED, matrix.UPACK_FEED)
        ]
        ids = {feed["id"] for feed in production} | set(PRODUCTION_FEED_IDS.values())
        names = {feed["name"].casefold() for feed in production}
        if any(
            feed["id"] in ids or feed["name"].casefold() in names
            for feed in resolved.values()
        ):
            raise ReleaseError("Rehearsal feed resolved to a production destination")
        if resolved["pip"]["id"] == resolved["upack"]["id"]:
            raise ReleaseError(
                "Rehearsal requires distinct pip and UPack feed destinations"
            )
    return resolved


def _row_key(row):
    values = tuple(row.get(key) for key in ("kind", "target", "name", "identifier"))
    if any(not isinstance(value, str) for value in values):
        raise ReleaseError("Verifier returned an invalid row identity")
    return values


def _required_rows(plan):
    required = {}

    def add(kind, target, name, identifier, commit=None):
        required[(kind, target.key, name, identifier)] = commit

    for target in plan.targets:
        for repository in plan.repositories:
            name = (
                "github/microsoft/SynapseML"
                if repository == "oss"
                else "ado/SynapseML-Internal"
            )
            tags = getattr(target, repository + "_tags")
            commit = getattr(target, repository + "_commit")
            for tag in tags:
                add("git-tag", target, name, tag, commit)
            add("tag-set", target, name + "/same-commit", ", ".join(tags), commit)
            for family in plan.families:
                version = getattr(target, f"{repository}_{family}_version")
                if family == "maven":
                    modules = (
                        verify.PUBLIC_MAVEN_MODULES
                        if repository == "oss"
                        else [verify.INTERNAL_MAVEN_MODULE]
                    )
                    for module in modules:
                        add("maven", target, f"{module}_{target.scala}", version)
                        if repository == "oss":
                            add(
                                "maven-central",
                                target,
                                f"{module}_{target.scala}",
                                version,
                            )
                    if repository == "oss" and target.key == "master":
                        add("pypi", target, "pypi/synapseml", plan.oss_version)
                else:
                    package = "synapseml"
                    if repository == "internal":
                        package += "-internal" if family == "pip" else "_internal"
                    add(family, target, package, version)
    return required


def _inventory(plan, remote):
    response = remote.inventory(plan)
    if (
        not isinstance(response, (tuple, list))
        or len(response) != 2
        or not isinstance(response[0], list)
        or type(response[1]) is not bool
    ):
        raise ReleaseError("Verifier returned an invalid plan response")
    rows, complete = response
    required = _required_rows(plan)
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ReleaseError("Verifier returned an invalid row")
        key = _row_key(row)
        if key not in required or key in seen:
            raise ReleaseError("Verifier returned duplicate or out-of-scope rows")
        seen.add(key)
        if row.get("status") not in {verify.OK, verify.MISSING}:
            raise ReleaseError(
                "Verifier skipped required coverage or returned an invalid status"
            )
        expected = required[key]
        if expected is not None:
            if (
                row.get("expected_commit") != expected
                or row.get("actual_commit") != expected
                or row["status"] != verify.OK
            ):
                raise ReleaseError(
                    f"Source tag/commit does not match the approved plan for "
                    f"{row['target']}: {row['identifier']}"
                )
    if seen != set(required):
        raise ReleaseError("Verifier omitted required plan coverage")
    actual_complete = bool(rows) and all(row["status"] == verify.OK for row in rows)
    if complete != actual_complete:
        raise ReleaseError("Verifier complete flag disagrees with its required rows")
    clean_rows = [
        {
            key: row[key]
            for key in (
                "kind",
                "target",
                "name",
                "identifier",
                "status",
                "expected_commit",
                "actual_commit",
            )
            if key in row
        }
        for row in rows
    ]
    return verify.build_report(plan, clean_rows, complete)


def _dependency_plan(plan, repositories):
    # These are read-only inventories of existing Maven dependencies. In
    # particular an Internal patch does not start an OSS release.
    internal = "internal" in repositories
    return matrix.build_plan(
        plan.oss_version,
        plan.internal_patch if internal else "0",
        [target.key for target in plan.targets],
        scope=plan.scope if internal else "full",
        families=["maven"],
        repositories=repositories,
        oss_commits={target.key: target.oss_commit for target in plan.targets},
        internal_commits=(
            {target.key: target.internal_commit for target in plan.targets}
            if internal
            else None
        ),
    )


def _probes(plan, remote):
    policy = _policy(plan, remote)
    destinations = _destinations(plan, remote)
    inventory = _inventory(plan, remote)
    dependencies = []
    if "maven" not in plan.families:
        dependencies.append(
            _inventory(_dependency_plan(plan, plan.repositories), remote)
        )
    if "internal" in plan.repositories and "oss" not in plan.repositories:
        dependencies.append(_inventory(_dependency_plan(plan, ["oss"]), remote))
    return policy, destinations, inventory, dependencies


def _target(plan, key):
    return next(target for target in plan.targets if target.key == key)


def _source_tag(target, repository, family):
    if family == "pip":
        tags = [
            tag
            for tag in getattr(target, repository + "_tags")
            if tag.endswith("-python" + target.python)
        ]
        if len(tags) != 1:
            raise ReleaseError("The plan does not identify a unique Python source tag")
        return tags[0]
    return getattr(target, repository + "_maven_tag")


def _maven_id(repository, target):
    return f"maven.{repository}.{target}"


def _publish_flag(repository, target, family):
    repo = "synapseml" if repository == "oss" else "internal"
    if family == "pip":
        suffix = "py" + target.python.replace(".", "")
    else:
        suffix = {"master": "default", "spark4.0": "spark4", "spark4.1": "spark41"}[
            target.key
        ]
    return f"build_{repo}_{family}_{suffix}"


def _operation(plan, action, families):
    families = [family for family in FAMILY_ORDER if family in families]
    allowed = {"maven"} if action["kind"] == "maven" else {"pip", "upack"}
    if not families or not set(families) <= allowed.intersection(plan.families):
        raise ReleaseError("Operation selected an unapproved artifact family")
    target = _target(plan, action["target"])
    repository = action["repository"]
    payload = base64.b64encode(canonical(matrix.plan_to_dict(plan))).decode("ascii")
    parameters = {}
    variables = {}
    if action["kind"] == "publisher":
        parameters = {
            key: False if key.startswith("build_") else value
            for key, value in plan.publish_parameters.items()
        }
        for family in families:
            flag = _publish_flag(repository, target, family)
            if plan.publish_parameters.get(flag) is not True:
                raise ReleaseError("Operation would enable an excluded publication job")
            parameters[flag] = True
        parameters.update(
            release_plan_base64=payload,
            release_plan_id=plan.plan_id,
            publish_release=True,
        )
        variables = {name: "" for name in COUNTERS}
        variables.update(plan.publish_variables)
    elif repository == "oss":
        parameters = {
            "publishRelease": True,
            "release_plan_base64": payload,
            "release_plan_id": plan.plan_id,
        }
        variables = {
            "SYNAPSEML_RELEASE_PLAN_ID": plan.plan_id,
            "SYNAPSEML_RELEASE_COMMIT": action["source_commit"],
        }
    else:
        parameters = {
            "release_publish": True,
            "release_tag": action["source_tag"],
            "release_commit": action["source_commit"],
            "release_plan_id": plan.plan_id,
            "release_plan_base64": payload,
        }
    command = [
        "az",
        "pipelines",
        "run",
        "--id",
        str(action["pipeline_id"]),
        "--org",
        plan.ado_org,
        "--project",
        plan.ado_project,
    ]
    if action["kind"] == "maven":
        command += [
            "--branch",
            "refs/tags/" + action["source_tag"],
            "--commit-id",
            action["source_commit"],
        ]
    command += ["--parameters"] + [
        f"{key}={str(value).lower() if isinstance(value, bool) else value}"
        for key, value in sorted(parameters.items())
    ]
    if variables:
        command += ["--variables"] + [
            f"{key}={value}" for key, value in sorted(variables.items())
        ]
    command += ["--output", "json", "--only-show-errors"]
    identity = {
        "plan_id": plan.plan_id,
        "kind": action["kind"],
        "repository": repository,
        "target": target.key,
        "families": families,
    }
    return {
        "id": hashlib.sha256(canonical(identity)).hexdigest(),
        "families": families,
        "parameters": parameters,
        "variables": variables,
        "command": command,
    }


def build_actions(plan):
    actions = []
    for family in FAMILY_ORDER:
        if family not in plan.families:
            continue
        for target in plan.targets:
            for repository in plan.repositories:
                kind = "maven" if family == "maven" else "publisher"
                action_id = (
                    _maven_id(repository, target.key)
                    if kind == "maven"
                    else f"publisher.{repository}.{target.key}.{family}"
                )
                dependencies = []
                if family != "maven":
                    dependencies.append(_maven_id(repository, target.key))
                if repository == "internal":
                    dependencies.append(_maven_id("oss", target.key))
                item = {
                    "id": action_id,
                    "kind": kind,
                    "repository": repository,
                    "target": target.key,
                    "family": family,
                    "pipeline_id": (
                        plan.publish_pipeline_id
                        if kind == "publisher"
                        else getattr(plan, repository + "_maven_pipeline_id")
                    ),
                    "source_tag": _source_tag(target, repository, family),
                    "source_commit": getattr(target, repository + "_commit"),
                    "version": getattr(target, f"{repository}_{family}_version"),
                    "dependencies": dependencies,
                    "status": "planned",
                    "command": [],
                    "operation": None,
                    "intent_at": None,
                    "build_id": None,
                    "receipt": None,
                    "outcome": None,
                    "error": None,
                    "blocked": [],
                    "attempts": [],
                }
                item["command"] = _operation(plan, item, [family])["command"]
                actions.append(item)
    return actions


def _new_state(plan):
    return {
        "schema_version": STATE_VERSION,
        "plan_id": plan.plan_id,
        "plan": matrix.plan_to_dict(plan),
        "state_id": "",
        "revision": 0,
        "created_at": now(),
        "updated_at": now(),
        "actions": build_actions(plan),
        "inventory": None,
        "dependency_inventory": [],
        "destinations": None,
        "policy": None,
    }


def _validate_state(data, plan):
    expected = _new_state(plan)
    if not isinstance(data, dict) or set(data) != set(expected):
        raise ReleaseError("Release state has an invalid schema")
    if type(data["schema_version"]) is not int or data["schema_version"] not in {
        1,
        STATE_VERSION,
    }:
        raise ReleaseError("Release state has an unsupported schema version")
    if data["plan_id"] != plan.plan_id or data["plan"] != matrix.plan_to_dict(plan):
        raise ReleaseError(
            "Release state belongs to a different plan or source binding"
        )
    if (
        not isinstance(data["state_id"], str)
        or not HASH_RE.fullmatch(data["state_id"])
        or not hmac.compare_digest(data["state_id"], _digest(data, "state_id"))
    ):
        raise ReleaseError("Release state checksum is corrupt")
    if type(data["revision"]) is not int or data["revision"] < 1:
        raise ReleaseError("Release state has an invalid revision")
    families = {family for family in plan.families if family in {"pip", "upack"}}
    if plan.mode == "rehearsal" and families:
        families = {"pip", "upack"}
    destinations = data["destinations"]
    if not isinstance(destinations, dict) or set(destinations) != families:
        raise ReleaseError("Release state has invalid destination bindings")
    for family, feed in destinations.items():
        if (
            not isinstance(feed, dict)
            or set(feed) != {"requested", "id", "name", "project", "project_id"}
            or feed["requested"] != getattr(plan, family + "_feed")
            or not isinstance(feed["id"], str)
            or not GUID_RE.fullmatch(feed["id"])
            or not isinstance(feed["name"], str)
            or feed["name"].casefold()
            != feed["requested"].rsplit("/", 1)[-1].casefold()
            or feed["project"] != matrix.ADO_PROJECT
            or feed["project_id"] != ADO_PROJECT_ID
        ):
            raise ReleaseError("Release state has a conflicting feed destination")
    if (
        not isinstance(data["policy"], dict)
        or type(data["policy"].get("required")) is not bool
        or data["policy"]["required"] != (plan.scope == "full")
        or not isinstance(data["inventory"], dict)
        or data["inventory"].get("plan_id") != plan.plan_id
        or not isinstance(data["inventory"].get("rows"), list)
        or not isinstance(data["dependency_inventory"], list)
        or any(not isinstance(report, dict) for report in data["dependency_inventory"])
    ):
        raise ReleaseError("Release state has malformed preflight evidence")
    _time(data["policy"].get("checked_at"), "Policy evidence")
    _time(data["inventory"].get("checked_at"), "Inventory evidence")
    if _time(data["created_at"], "State creation") > _time(
        data["updated_at"], "State update"
    ):
        raise ReleaseError("Release state timestamps conflict")
    legacy = data["schema_version"] == 1
    if legacy:
        for blueprint in expected["actions"]:
            blueprint.pop("attempts")
    actions = data["actions"]
    if not isinstance(actions, list) or len(actions) != len(expected["actions"]):
        raise ReleaseError("Release state has missing or out-of-scope actions")
    variable_fields = {
        "status",
        "command",
        "operation",
        "intent_at",
        "build_id",
        "receipt",
        "outcome",
        "error",
        "blocked",
        "attempts",
    }
    operation_builds = {}
    for action, blueprint in zip(actions, expected["actions"]):
        if (
            not isinstance(action, dict)
            or set(action) != set(blueprint)
            or any(
                action[key] != value
                for key, value in blueprint.items()
                if key not in variable_fields
            )
        ):
            raise ReleaseError("Release state contains a conflicting action or source")
        if not isinstance(action["status"], str) or action["status"] not in STATES:
            raise ReleaseError("Release state has an invalid action status")
        operation = action["operation"]
        if operation is not None:
            if (
                not isinstance(operation, dict)
                or not isinstance(operation.get("families"), list)
                or action["family"] not in operation["families"]
                or operation != _operation(plan, action, operation["families"])
                or action["command"] != operation["command"]
            ):
                raise ReleaseError(
                    "Release state command differs from the approved operation"
                )
            _time(action["intent_at"], "Submission intent")
            prior_id = operation_builds.setdefault(operation["id"], action["build_id"])
            if prior_id != action["build_id"]:
                raise ReleaseError(
                    "Release state has conflicting run IDs for one submission"
                )
        elif (
            action["command"] != blueprint["command"]
            or action["intent_at"] is not None
            or action["build_id"] is not None
            or action["receipt"] is not None
            or action["outcome"] is not None
            or action["status"] not in {"planned", "existing"}
        ):
            raise ReleaseError("Release state has an outcome without submission intent")
        if action["build_id"] is not None and not _positive_id(action["build_id"]):
            raise ReleaseError("Release state has an invalid Azure build ID")
        if (
            action["status"] in {"pending", "failed", "complete"}
            and action["build_id"] is None
        ):
            raise ReleaseError("Release state lost a required Azure build ID")
        if (
            not isinstance(action["blocked"], list)
            or any(not isinstance(value, str) for value in action["blocked"])
            or (action["error"] is not None and not isinstance(action["error"], str))
        ):
            raise ReleaseError("Release state has malformed outcome evidence")
        if action["outcome"] is not None:
            outcome = action["outcome"]
            if (
                not isinstance(outcome, dict)
                or outcome.get("build_id") != action["build_id"]
                or type(outcome.get("build_id")) is not int
                or outcome.get("pipeline_id") != action["pipeline_id"]
                or not _commit(outcome.get("source_commit"))
                or not isinstance(outcome.get("source_branch"), str)
                or not isinstance(outcome.get("status"), str)
                or outcome.get("status")
                not in {
                    "notStarted",
                    "inProgress",
                    "postponed",
                    "cancelling",
                    "completed",
                }
            ):
                raise ReleaseError(
                    "Release state contains invalid Azure outcome evidence"
                )
            _time(outcome.get("checked_at"), "Azure outcome")
        if action["receipt"] is not None:
            receipt = action["receipt"]
            if (
                not isinstance(receipt, dict)
                or type(receipt.get("schema_version")) is not int
                or receipt["schema_version"] != 1
                or receipt.get("plan_id") != plan.plan_id
                or receipt.get("build_id") != action["build_id"]
                or receipt.get("pipeline_id") != action["pipeline_id"]
                or receipt.get("source_commit") != action["source_commit"]
                or receipt.get("source_tag") != action["source_tag"]
                or receipt.get("result") != "succeeded"
            ):
                raise ReleaseError(
                    "Release state contains a mismatched provenance receipt"
                )
            _time(receipt.get("checked_at"), "Receipt")
            if not isinstance(receipt.get("jobs"), list) or any(
                not isinstance(job, dict) for job in receipt["jobs"]
            ):
                raise ReleaseError("Release state contains invalid Azure job evidence")
            _jobs({"records": [{**job, "type": "Job"} for job in receipt["jobs"]]})
            _validate_manifests(
                plan,
                action,
                receipt.get("provenance"),
                action["build_id"],
                publisher_commit=receipt.get("publisher_commit"),
                destinations=data["destinations"],
            )
        if action["status"] == "complete" and (
            action["receipt"] is None
            or action["outcome"] is None
            or action["outcome"]["status"] != "completed"
            or action["outcome"].get("result") != "succeeded"
        ):
            raise ReleaseError(
                "Release state claims completion without a successful Azure receipt"
            )
    runs = {}
    for action in actions:
        if action["operation"] is None:
            continue
        operation = action["operation"]
        if action["build_id"] is not None:
            prior = runs.setdefault(action["build_id"], operation["id"])
            if prior != operation["id"]:
                raise ReleaseError(
                    "Release state assigns one run to conflicting operations"
                )
        for member in actions:
            if (
                member["kind"] == action["kind"]
                and member["repository"] == action["repository"]
                and member["target"] == action["target"]
                and member["family"] in operation["families"]
                and (
                    member["operation"] != operation
                    or member["build_id"] != action["build_id"]
                )
            ):
                raise ReleaseError(
                    "Release state lost part of a shared publisher submission"
                )
    if legacy:
        data = copy.deepcopy(data)
        data["schema_version"] = STATE_VERSION
        for action in data["actions"]:
            action["attempts"] = []
        data["state_id"] = _digest(data, "state_id")
    else:
        _validate_attempts(plan, data)
    return data


def _file_bytes(path, limit, label):
    try:
        if path.is_symlink() or not stat.S_ISREG(path.stat().st_mode):
            raise ReleaseError(f"{label} must be a regular, non-symbolic file")
        with path.open("rb") as stream:
            body = stream.read(limit + 1)
        if len(body) > limit:
            raise ReleaseError(f"{label} exceeds its size limit")
        return body
    except OSError as error:
        raise ReleaseError(f"Cannot read {label}") from error


def _ledger_paths(path, plan):
    supplied = Path(path)
    resolved = supplied.parent.resolve() / supplied.name
    if not supplied.name or supplied.name.startswith(".release-plan-"):
        raise ReleaseError("Use a non-reserved ledger filename")
    return (
        resolved,
        resolved.with_name(resolved.name + ".lock"),
        resolved.parent / f".release-plan-{plan.plan_id}.json",
        resolved.parent / f".release-plan-{plan.plan_id}.lock",
    )


def _lock_details(path, plan_id, kind):
    result = {"kind": kind, "path": str(path), "exists": path.exists()}
    if path.is_symlink():
        return {**result, "exists": True, "metadata_valid": False}
    if not result["exists"]:
        return result
    try:
        value = _json(
            _file_bytes(path, MAX_LOCK_BYTES, "Lock metadata"), "Lock metadata"
        )
        if not isinstance(value, dict):
            raise ReleaseError("Invalid lock metadata")
        valid_id = isinstance(value.get("plan_id"), str) and HASH_RE.fullmatch(
            value["plan_id"]
        )
        if not valid_id or not _positive_id(value.get("pid")):
            raise ReleaseError("Invalid lock metadata")
        _time(value.get("created_at"), "Lock creation")
        host = value.get("host")
        if host is not None and (
            not isinstance(host, str)
            or not 0 < len(host) <= 255
            or any(ord(char) < 32 or ord(char) > 126 for char in host)
        ):
            raise ReleaseError("Invalid lock host")
        result.update(
            metadata_valid=True,
            plan_id=value["plan_id"],
            matches_plan=value["plan_id"] == plan_id,
            pid=value["pid"],
            host=host,
            created_at=value["created_at"],
        )
    except (ValueError, ReleaseError):
        result["metadata_valid"] = False
    return result


def inspect_locks(plan, path):
    state, lock, claim, guard = _ledger_paths(path, plan)
    return {
        "schema_version": 1,
        "plan_id": plan.plan_id,
        "state_path": str(state),
        "claim_path": str(claim),
        "locks": [
            _lock_details(lock, plan.plan_id, "state"),
            _lock_details(guard, plan.plan_id, "plan"),
        ],
        "safety": (
            "Read-only metadata, not proof of liveness or death. No lock is acquired "
            "or removed and Azure is not contacted. Confirm ownership and Azure "
            "outcomes before any manual recovery."
        ),
    }


class StateStore:
    """Directory-local plan claim and exclusive, compare-before-replace writes."""

    def __init__(self, path, plan, must_exist=False):
        self.path, self.lock, self.claim, self.guard = _ledger_paths(path, plan)
        self.plan = plan
        self.must_exist = must_exist
        self.owner = uuid.uuid4().hex
        self.fingerprint = None
        self.lock_bytes = None
        self.owned_locks = {}
        self.claim_bytes = None
        self.claim_data = None
        self.loaded_version = STATE_VERSION
        self.state = None

    def _acquire(self, path, kind):
        body = canonical(
            {
                "schema_version": 1,
                "owner": self.owner,
                "pid": os.getpid(),
                "host": socket.gethostname(),
                "plan_id": self.plan.plan_id,
                "created_at": now(),
            }
        )
        try:
            descriptor = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as error:
            details = _lock_details(path, self.plan.plan_id, kind)
            raise ReleaseError(
                f"Release {kind} lock is already owned. Lock details: "
                f"{canonical(details).decode('ascii')}. Use status --inspect-lock; "
                "age or PID alone is not permission to remove a lock."
            ) from error
        except OSError as error:
            raise ReleaseError(
                f"Cannot acquire release {kind} lock at {path}"
            ) from error
        self.owned_locks[path] = body
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        return body

    def _legacy_conflicts(self):
        total = 0
        with os.scandir(self.path.parent) as entries:
            for index, entry in enumerate(entries):
                if index >= MAX_CLAIM_CANDIDATES:
                    raise ReleaseError(
                        "Ledger directory is too large for bounded claim initialization"
                    )
                candidate = Path(entry.path)
                if (
                    candidate == self.path
                    or candidate.name.startswith(".release-plan-")
                    or candidate.name.endswith(".lock")
                    or entry.is_dir(follow_symlinks=False)
                ):
                    continue
                if candidate.suffix.lower() != ".json":
                    if not self.path.exists():
                        raise ReleaseError(
                            f"Unclassified file in ledger directory: {candidate}; nominate the existing "
                            "ledger or use the authoritative plan directory"
                        )
                    continue
                label = f"Sibling ledger candidate {candidate}"
                body = _file_bytes(candidate, MAX_JSON_BYTES - total, label)
                total += len(body)
                data = _json(body, label)
                if (
                    isinstance(data, dict)
                    and data.get("plan_id") == self.plan.plan_id
                    and ("state_id" in data or {"plan", "actions"} <= set(data))
                ):
                    raise ReleaseError(
                        f"Another ledger for this plan exists in the same directory: {candidate}"
                    )

    def _read_claim(self):
        body = _file_bytes(self.claim, MAX_LOCK_BYTES, "Plan ledger claim")
        value = _json(body, "Plan ledger claim")
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "schema_version",
                "plan_id",
                "state_path",
                "created_at",
                "initialized",
                "state_version",
                "claim_id",
            }
            or type(value["schema_version"]) is not int
            or value["schema_version"] != 1
            or value["plan_id"] != self.plan.plan_id
            or value["state_path"] != str(self.path)
            or type(value["initialized"]) is not bool
            or type(value["state_version"]) is not int
            or value["state_version"] not in {1, STATE_VERSION}
            or not isinstance(value["claim_id"], str)
            or not HASH_RE.fullmatch(value["claim_id"])
            or not hmac.compare_digest(value["claim_id"], _digest(value, "claim_id"))
        ):
            raise ReleaseError(
                f"Conflicting or corrupt plan ledger claim at {self.claim}; do not create another ledger"
            )
        _time(value["created_at"], "Ledger claim creation")
        self.claim_data, self.claim_bytes = value, body
        if self.loaded_version < value["state_version"]:
            raise ReleaseError(
                "Claimed ledger cannot be downgraded to erase retry history"
            )

    def _ensure_claim(self):
        if self.claim.exists():
            self._read_claim()
            if self.claim_data["initialized"] and not self.path.exists():
                raise ReleaseError(
                    "The claimed ledger is missing; refusing to recreate lost history"
                )
            return
        self._legacy_conflicts()
        self.claim_data = {
            "schema_version": 1,
            "plan_id": self.plan.plan_id,
            "state_path": str(self.path),
            "created_at": now(),
            "initialized": self.path.exists(),
            "state_version": self.loaded_version,
            "claim_id": "",
        }
        self.claim_data["claim_id"] = _digest(self.claim_data, "claim_id")
        self.claim_bytes = canonical(self.claim_data) + b"\n"
        try:
            descriptor = os.open(
                str(self.claim), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600
            )
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(self.claim_bytes)
                stream.flush()
                os.fsync(stream.fileno())
        except OSError as error:
            raise ReleaseError("Could not persist the plan ledger claim") from error

    def __enter__(self):
        entered = False
        try:
            if any(
                path.is_symlink()
                for path in (self.path, self.lock, self.claim, self.guard)
            ):
                raise ReleaseError(
                    "Release state, claim and locks must not be symbolic links"
                )
            self._acquire(self.guard, "plan")
            self.lock_bytes = self._acquire(self.lock, "state")
            if self.path.exists():
                body = self._read()
                self.fingerprint = hashlib.sha256(body).hexdigest()
                raw = _json(body, "Release state")
                self.state = _validate_state(raw, self.plan)
                self.loaded_version = raw["schema_version"]
            elif self.must_exist:
                raise ReleaseError(
                    "Release state does not exist; run preflight or resume first"
                )
            else:
                self.state = _new_state(self.plan)
            self._ensure_claim()
            entered = True
            return self
        finally:
            if not entered:
                self.__exit__(None, None, None)

    def _read(self):
        return _file_bytes(self.path, MAX_JSON_BYTES, "Release state")

    def _replace(self, path, body):
        replacement = path.with_name(path.name + ".write-" + self.owner)
        try:
            descriptor = os.open(
                str(replacement), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600
            )
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(str(replacement), str(path))
            if os.name != "nt":
                descriptor = os.open(str(path.parent), os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
        except OSError as error:
            raise ReleaseError(
                "Could not atomically save release state/claim; do not retry a submission"
            ) from error
        finally:
            if replacement.exists():
                replacement.unlink()

    def save(self):
        for path, body in self.owned_locks.items():
            if _file_bytes(path, MAX_LOCK_BYTES, "Release lock") != body:
                raise ReleaseError("Release state lock ownership changed")
        if (
            _file_bytes(self.claim, MAX_LOCK_BYTES, "Plan ledger claim")
            != self.claim_bytes
        ):
            raise ReleaseError("Release plan ledger claim changed concurrently")
        actual = (
            hashlib.sha256(self._read()).hexdigest() if self.path.exists() else None
        )
        if actual != self.fingerprint:
            raise ReleaseError(
                "Release state changed concurrently; refusing a stale overwrite"
            )
        self.state["revision"] += 1
        self.state["updated_at"] = now()
        self.state["state_id"] = _digest(self.state, "state_id")
        _validate_state(self.state, self.plan)
        body = canonical(self.state) + b"\n"
        if len(body) > MAX_JSON_BYTES:
            raise ReleaseError(
                "Release state would exceed its size limit; retain the recorded run IDs"
            )
        if not self.claim_data["initialized"]:
            self.claim_data["initialized"] = True
            self.claim_data["claim_id"] = _digest(self.claim_data, "claim_id")
            claim_body = canonical(self.claim_data) + b"\n"
            self._replace(self.claim, claim_body)
            self.claim_bytes = claim_body
        self._replace(self.path, body)
        self.fingerprint = hashlib.sha256(body).hexdigest()
        self.loaded_version = STATE_VERSION
        if self.claim_data["state_version"] != STATE_VERSION:
            self.claim_data["state_version"] = STATE_VERSION
            self.claim_data["claim_id"] = _digest(self.claim_data, "claim_id")
            claim_body = canonical(self.claim_data) + b"\n"
            self._replace(self.claim, claim_body)
            self.claim_bytes = claim_body

    def __exit__(self, *_args):
        for path, body in reversed(list(self.owned_locks.items())):
            try:
                if _file_bytes(path, MAX_LOCK_BYTES, "Release lock") == body:
                    path.unlink()
                else:
                    print(
                        f"warning: Release lock changed at {path}; left untouched. "
                        "Use status --inspect-lock.",
                        file=sys.stderr,
                    )
            except (OSError, ReleaseError):
                print(
                    f"warning: Could not release lock at {path}; "
                    "use status --inspect-lock before manual recovery.",
                    file=sys.stderr,
                )
        self.owned_locks.clear()


def _artifact_rows(report, repository, target, family):
    rows = []
    for row in report["rows"]:
        if row["target"] != target or row["kind"] not in (
            {"maven", "maven-central", "pypi"} if family == "maven" else {family}
        ):
            continue
        internal = row["name"].startswith(("synapseml-internal", "synapseml_internal"))
        if internal == (repository == "internal"):
            rows.append(row)
    return rows


def _artifact_present(state, action):
    rows = _artifact_rows(
        state["inventory"], action["repository"], action["target"], action["family"]
    )
    return bool(rows) and all(row["status"] == verify.OK for row in rows)


def _dependency_present(state, identifier):
    _, repository, target = identifier.split(".", 2)
    known = next(
        (action for action in state["actions"] if action["id"] == identifier), None
    )
    if known is not None and known["operation"] is not None:
        return known["status"] == "complete"
    for report in [state["inventory"]] + state["dependency_inventory"]:
        rows = [
            row
            for row in _artifact_rows(report, repository, target, "maven")
            if row["kind"] == "maven"
        ]
        if rows:
            return all(row["status"] == verify.OK for row in rows)
    return False


def _observe(state):
    for action in state["actions"]:
        present = _artifact_present(state, action)
        action["blocked"] = [
            f"Required Maven dependency is not proven present: {identifier}"
            for identifier in action["dependencies"]
            if not _dependency_present(state, identifier)
        ]
        if action["operation"] is None:
            if present:
                action["status"] = "existing"
                action[
                    "error"
                ] = "Artifacts already exist; adopt a matching Azure run for source provenance."
            elif action["status"] == "existing":
                action["blocked"].append(
                    "Previously observed immutable artifacts disappeared; investigate before publication."
                )


def _parameters(build):
    parameters = build.get("templateParameters")
    if isinstance(parameters, str):
        parameters = _json(parameters, "Azure template parameters")
    if not isinstance(parameters, dict):
        raise ReleaseError("Azure build omitted its authoritative template parameters")
    return parameters


def _variables(build):
    values = {}
    for field in ("parameters", "variables"):
        raw = build.get(field)
        if raw is None:
            continue
        if isinstance(raw, str):
            raw = _json(raw, "Azure build variables")
        if not isinstance(raw, dict):
            raise ReleaseError("Azure build returned invalid variables")
        for name, value in raw.items():
            if isinstance(value, dict):
                if value.get("isSecret"):
                    continue
                value = value.get("value")
            if name in values and values[name] != value:
                raise ReleaseError("Azure build returned conflicting variables")
            values[name] = value
    return values


def _parameter_equal(actual, expected):
    if type(expected) is bool:
        return actual is expected or (
            isinstance(actual, str) and actual.lower() == str(expected).lower()
        )
    return type(actual) is type(expected) and actual == expected


def _validate_build(plan, action, build, remote):
    if (
        not isinstance(build, dict)
        or not _positive_id(build.get("id"))
        or build["id"] != action["build_id"]
        or not isinstance(build.get("definition"), dict)
        or type(build["definition"].get("id")) is not int
        or build["definition"]["id"] != action["pipeline_id"]
    ):
        raise ReleaseError(
            "Azure build ID or pipeline definition does not match the operation"
        )
    definition = remote.definition(action["pipeline_id"])
    repository = build.get("repository")
    if (
        not isinstance(definition, dict)
        or type(definition.get("id")) is not int
        or definition["id"] != action["pipeline_id"]
        or not isinstance(definition.get("repository"), dict)
        or not isinstance(repository, dict)
    ):
        raise ReleaseError("Azure pipeline omitted its repository identity")
    expected_repo = definition["repository"]
    for field in ("id", "type", "name"):
        if not isinstance(expected_repo.get(field), str) or not expected_repo[field]:
            raise ReleaseError("Azure pipeline returned an invalid repository identity")
        if repository.get(field) != expected_repo[field]:
            raise ReleaseError("Azure build repository does not match its pipeline")
    if action["kind"] == "maven":
        if action["repository"] == "oss":
            if (
                expected_repo["type"].lower() != "github"
                or expected_repo["id"].casefold() != "microsoft/synapseml"
            ):
                raise ReleaseError(
                    "Public Maven pipeline points to a different source repository"
                )
        elif (
            expected_repo["type"].lower() != "tfsgit"
            or expected_repo["name"].casefold() != "synapseml-internal"
        ):
            raise ReleaseError(
                "Internal Maven pipeline points to a different source repository"
            )
    if (
        not _commit(build.get("sourceVersion"))
        or not isinstance(build.get("sourceBranch"), str)
        or not build["sourceBranch"].startswith(("refs/heads/", "refs/tags/"))
    ):
        raise ReleaseError("Azure build did not return an exact source commit and ref")
    if action["kind"] == "maven" and (
        build["sourceVersion"] != action["source_commit"]
        or build["sourceBranch"] != "refs/tags/" + action["source_tag"]
    ):
        raise ReleaseError(
            "Azure Maven build source does not match the reviewed tag/commit"
        )
    parameters = _parameters(build)
    expected = action["operation"]["parameters"]
    for key, value in expected.items():
        if key not in parameters or not _parameter_equal(parameters[key], value):
            raise ReleaseError(
                f"Azure template parameter {key} differs from the approved operation"
            )
    if any(
        key.startswith("build_")
        and key not in expected
        and not _parameter_equal(value, False)
        for key, value in parameters.items()
    ):
        raise ReleaseError("Azure build enabled an unapproved publication job")
    variables = _variables(build)
    for key, value in action["operation"]["variables"].items():
        if variables.get(key) != value:
            raise ReleaseError(
                f"Azure build variable {key} differs from the recorded operation"
            )
    status, result = build.get("status"), build.get("result")
    if not isinstance(status, str) or status not in {
        "notStarted",
        "inProgress",
        "postponed",
        "cancelling",
        "completed",
    }:
        raise ReleaseError("Azure build returned an invalid status")
    if status == "completed":
        if not isinstance(result, str) or result not in {
            "succeeded",
            "failed",
            "canceled",
            "partiallySucceeded",
        }:
            raise ReleaseError("Azure completed build returned an invalid result")
        _time(build.get("finishTime"), "Azure build finish")
    elif result not in (None, "none"):
        raise ReleaseError("Azure pending build already has a conflicting result")
    _time(build.get("queueTime"), "Azure build queue")
    return {
        "checked_at": now(),
        "build_id": build["id"],
        "pipeline_id": action["pipeline_id"],
        "status": status,
        "result": result,
        "source_branch": build["sourceBranch"],
        "source_commit": build["sourceVersion"],
    }


def _jobs(data):
    if not isinstance(data, dict) or not isinstance(data.get("records"), list):
        raise ReleaseError("Azure build timeline has no job records")
    jobs = []
    for record in data["records"]:
        if not isinstance(record, dict):
            raise ReleaseError("Azure build timeline contains an invalid record")
        if record.get("type") != "Job":
            continue
        if (
            not isinstance(record.get("id"), str)
            or not record["id"]
            or not isinstance(record.get("name"), str)
            or not record["name"]
            or record.get("state") != "completed"
            or not isinstance(record.get("result"), str)
            or record.get("result") not in {"succeeded", "skipped"}
        ):
            raise ReleaseError("Azure build has an incomplete or unsuccessful job")
        jobs.append({key: record.get(key) for key in ("id", "name", "state", "result")})
    if not any(job["result"] == "succeeded" for job in jobs):
        raise ReleaseError("Azure build has no successful job evidence")
    return jobs


def _validate_manifests(
    plan, action, documents, build_id, publisher_commit=None, destinations=None
):
    if not isinstance(documents, list) or not documents:
        raise ReleaseError("Azure build has no release-provenance.json receipt")
    target = _target(plan, action["target"])
    wanted = set(action["operation"]["families"])
    seen = set()
    normalized = []
    for document in documents:
        if (
            not isinstance(document, dict)
            or type(document.get("schema_version")) is not int
            or document["schema_version"] != 1
            or document.get("plan_id") != plan.plan_id
            or type(document.get("build_id")) is not int
            or document["build_id"] != build_id
            or type(document.get("pipeline_id")) is not int
            or document["pipeline_id"] != action["pipeline_id"]
            or document.get("repository") != action["repository"]
            or document.get("target") != action["target"]
            or document.get("source_commit") != action["source_commit"]
        ):
            raise ReleaseError(
                "Release provenance does not match the plan, source or Azure run"
            )
        families = document.get("families")
        if (
            not isinstance(families, list)
            or len(families) != 1
            or not isinstance(families[0], str)
            or families[0] not in wanted
            or families[0] in seen
        ):
            raise ReleaseError(
                "Release provenance has duplicate or unapproved family coverage"
            )
        family = families[0]
        seen.add(family)
        if document.get("source_tag") != _source_tag(
            target, action["repository"], family
        ) or document.get("version") != getattr(
            target, f"{action['repository']}_{family}_version"
        ):
            raise ReleaseError(
                "Release provenance tag or artifact version does not match the plan"
            )
        fields = {
            "schema_version",
            "plan_id",
            "build_id",
            "pipeline_id",
            "repository",
            "target",
            "families",
            "source_tag",
            "source_commit",
            "version",
        }
        if action["kind"] == "publisher":
            package = (
                "synapseml"
                if action["repository"] == "oss"
                else "synapseml-internal"
                if family == "pip"
                else "synapseml_internal"
            )
            if not isinstance(destinations, dict) or family not in destinations:
                raise ReleaseError(
                    "Publisher receipt has no approved destination binding"
                )
            feed = destinations[family]
            destination = {
                "feed_id": feed["id"],
                "feed_name": feed["name"],
                "project_id": feed["project_id"],
                "project_name": feed["project"],
            }
            output = {
                "family": family,
                "package_name": package,
                "version": document["version"],
                **destination,
            }
            if (
                document.get("receipt_type") != "synapseml-publisher"
                or not _commit(publisher_commit)
                or document.get("publisher_source_commit") != publisher_commit
                or document.get("scope") != plan.scope
                or document.get("mode") != plan.mode
                or document.get("family") != family
                or document.get("package_name") != package
                or document.get("destination") != destination
                or document.get("outputs") != [output]
            ):
                raise ReleaseError(
                    "Publisher provenance source, scope or output destination is invalid"
                )
            fields.update(
                {
                    "receipt_type",
                    "publisher_source_commit",
                    "mode",
                    "scope",
                    "family",
                    "package_name",
                    "destination",
                    "outputs",
                }
            )
        artifacts = document.get("artifacts")
        if not isinstance(artifacts, list) or not artifacts:
            raise ReleaseError("Release provenance has no output identities")
        paths = set()
        for artifact in artifacts:
            if (
                not isinstance(artifact, dict)
                or not isinstance(artifact.get("path"), str)
                or not artifact["path"]
                or "\\" in artifact["path"]
                or artifact["path"].startswith("/")
                or ":" in artifact["path"]
                or any(part in {"", ".", ".."} for part in artifact["path"].split("/"))
                or artifact["path"] in paths
                or not isinstance(artifact.get("sha256"), str)
                or not HASH_RE.fullmatch(artifact["sha256"])
                or type(artifact.get("size")) is not int
                or artifact["size"] < 0
            ):
                raise ReleaseError(
                    "Release provenance has invalid output identities or hashes"
                )
            paths.add(artifact["path"])
        if (
            action["kind"] != "publisher"
            and action["repository"] == "oss"
            and target.key == "master"
            and f"pypi/{verify.public_pypi_wheel_name(plan.oss_version)}" not in paths
        ):
            raise ReleaseError(
                "Primary Maven provenance omits its published PyPI wheel"
            )
        normalized.append(
            {
                **{key: copy.deepcopy(document[key]) for key in fields},
                "artifacts": [
                    {key: artifact[key] for key in ("path", "sha256", "size")}
                    for artifact in artifacts
                ],
            }
        )
    if seen != wanted:
        raise ReleaseError("Release provenance omits a required family")
    return normalized


def _refresh_group(plan, state, actions, remote):
    first = actions[0]
    try:
        build = remote.build(first["build_id"])
        outcome = _validate_build(plan, first, build, remote)
        for action in actions:
            action["outcome"] = copy.deepcopy(outcome)
            action["receipt"] = None
            action["error"] = None
        if outcome["status"] != "completed":
            for action in actions:
                action["status"] = "pending"
            return
        if outcome["result"] != "succeeded":
            for action in actions:
                action["status"] = "failed"
                action[
                    "error"
                ] = "Azure build did not succeed; automatic retry is forbidden."
            return
        jobs = _jobs(remote.timeline(first["build_id"]))
        documents = _validate_manifests(
            plan,
            first,
            remote.provenance(first["build_id"]),
            first["build_id"],
            publisher_commit=outcome["source_commit"],
            destinations=state["destinations"],
        )
        for action in actions:
            action["receipt"] = {
                "schema_version": 1,
                "plan_id": plan.plan_id,
                "build_id": action["build_id"],
                "pipeline_id": action["pipeline_id"],
                "source_tag": action["source_tag"],
                "source_commit": action["source_commit"],
                "publisher_commit": outcome["source_commit"]
                if action["kind"] == "publisher"
                else None,
                "result": "succeeded",
                "checked_at": now(),
                "jobs": jobs,
                "provenance": copy.deepcopy(documents),
            }
            if _artifact_present(state, action):
                action["status"] = "complete"
            else:
                action["status"] = "pending"
                action[
                    "error"
                ] = "Azure succeeded; waiting for required artifact visibility."
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        for action in actions:
            action["status"] = "unknown"
            action["receipt"] = None
            action["error"] = _safe_error(error)


def _refresh(plan, state, remote):
    groups = {}
    for action in state["actions"]:
        if action["build_id"] is not None:
            groups.setdefault(
                (action["operation"]["id"], action["build_id"]), []
            ).append(action)
    for actions in groups.values():
        _refresh_group(plan, state, actions, remote)
    _observe(state)


def _adopt(plan, state, specifications, remote):
    requests = []
    seen = set()
    assigned = {}
    for specification in specifications:
        action_id, separator, number = specification.partition("=")
        if (
            not separator
            or not re.fullmatch(r"[1-9][0-9]*", number)
            or action_id in seen
        ):
            raise ReleaseError("--adopt expects unique ACTION_ID=BUILD_ID values")
        seen.add(action_id)
        action = next(
            (value for value in state["actions"] if value["id"] == action_id), None
        )
        if action is None:
            raise ReleaseError("--adopt selected an action outside this plan")
        build_id = int(number)
        if any(
            entry["previous"]["build_id"] == build_id
            for item in state["actions"]
            for entry in item["attempts"]
        ):
            raise ReleaseError(
                "Cannot adopt a retired attempt as the current submission"
            )
        if action["build_id"] not in (None, build_id):
            raise ReleaseError(
                "Cannot replace an action's already-recorded Azure build ID"
            )
        build = remote.build(build_id)
        operation = action["operation"]
        if operation is None:
            families = [action["family"]]
            if action["kind"] == "publisher":
                parameters = _parameters(build)
                target = _target(plan, action["target"])
                families = [
                    family
                    for family in ("pip", "upack")
                    if _parameter_equal(
                        parameters.get(
                            _publish_flag(action["repository"], target, family)
                        ),
                        True,
                    )
                ]
                if action["family"] not in families:
                    raise ReleaseError(
                        "Adopted Azure build did not select this action's family"
                    )
            operation = _operation(plan, action, families)
        candidate = {**action, "operation": operation, "build_id": build_id}
        _validate_build(plan, candidate, build, remote)
        group = [
            value
            for value in state["actions"]
            if value["kind"] == action["kind"]
            and value["repository"] == action["repository"]
            and value["target"] == action["target"]
            and value["family"] in operation["families"]
        ]
        for item in group:
            if item["build_id"] not in (None, build_id):
                raise ReleaseError(
                    "Adoption conflicts with another recorded submission"
                )
            if item["operation"] not in (None, operation):
                raise ReleaseError(
                    "Adoption conflicts with an ambiguous submission intent"
                )
            binding = (operation["id"], build_id)
            if item["id"] in assigned and assigned[item["id"]] != binding:
                raise ReleaseError(
                    "Requested adoptions conflict for a shared publisher operation"
                )
            assigned[item["id"]] = binding
        requests.append((group, operation, build_id))
    # Validate every requested identity before changing any local binding.
    for group, operation, build_id in requests:
        for action in group:
            action["operation"] = copy.deepcopy(operation)
            action["command"] = list(operation["command"])
            action["build_id"] = build_id
            action["intent_at"] = action["intent_at"] or now()
            action["status"] = "pending"
            action["error"] = None


def _absence_descriptors(plan, actions, destinations):
    descriptors = []
    for action in actions:
        if action["kind"] != "publisher" or action["family"] not in {"pip", "upack"}:
            raise ReleaseError(
                "Maven retry requires definitive whole-namespace absence; aggregate MISSING is insufficient"
            )
        family, repository = action["family"], action["repository"]
        feed = destinations[family]
        package = (
            "synapseml"
            if repository == "oss"
            else "synapseml-internal"
            if family == "pip"
            else "synapseml_internal"
        )
        descriptors.append(
            {
                "action_id": action["id"],
                "repository": repository,
                "target": action["target"],
                "family": family,
                "name": package,
                "version": action["version"],
                "feed_id": feed["id"],
                "project_id": feed["project_id"],
                "status": "absent",
            }
        )
    return descriptors


def _validate_absence(plan, actions, destinations, proof, fresh=True):
    if (
        not isinstance(proof, dict)
        or set(proof) != {"schema_version", "plan_id", "checked_at", "artifacts"}
        or type(proof["schema_version"]) is not int
        or proof["schema_version"] != 1
        or proof["plan_id"] != plan.plan_id
        or proof["artifacts"] != _absence_descriptors(plan, actions, destinations)
    ):
        raise ReleaseError(
            "Retry requires definitive absence of every selected artifact namespace"
        )
    checked = _time(proof["checked_at"], "Retry absence")
    if fresh and not timedelta(0) <= datetime.now(timezone.utc) - checked <= timedelta(
        minutes=5
    ):
        raise ReleaseError("Retry absence evidence is stale or in the future")
    return checked


def _terminal_jobs(data):
    if not isinstance(data, dict) or not isinstance(data.get("records"), list):
        raise ReleaseError("Retry requires authoritative terminal job evidence")
    jobs = []
    for item in data["records"]:
        if not isinstance(item, dict):
            raise ReleaseError("Retry job evidence is invalid")
        if item.get("type") != "Job":
            continue
        if (
            not isinstance(item.get("id"), str)
            or not item["id"]
            or not isinstance(item.get("name"), str)
            or not item["name"]
            or item.get("state") != "completed"
            or not isinstance(item.get("result"), str)
            or item.get("result")
            not in {
                "succeeded",
                "failed",
                "canceled",
                "skipped",
                "abandoned",
                "succeededWithIssues",
            }
        ):
            raise ReleaseError(
                "Retry refused: a job is active or has an uncertain outcome"
            )
        jobs.append({key: item[key] for key in ("id", "name", "state", "result")})
    if not jobs or len({job["id"] for job in jobs}) != len(jobs):
        raise ReleaseError("Retry requires complete, unique terminal job evidence")
    return jobs


def _operation_group(actions, first):
    operation = first["operation"]
    return [
        action
        for action in actions
        if action["kind"] == first["kind"]
        and action["repository"] == first["repository"]
        and action["target"] == first["target"]
        and action["family"] in operation["families"]
    ]


def _validate_attempts(plan, state):
    blueprints = {item["id"]: item for item in build_actions(plan)}
    mutable = {
        "status",
        "command",
        "operation",
        "intent_at",
        "build_id",
        "receipt",
        "outcome",
        "error",
        "blocked",
        "attempts",
    }
    bindings = {}
    for action in state["actions"]:
        attempts = action.get("attempts")
        if not isinstance(attempts, list) or len(attempts) > MAX_RETRY_ATTEMPTS:
            raise ReleaseError("Release state has invalid or excessive retry history")
        prior_time = None
        for number, entry in enumerate(attempts, 1):
            if (
                not isinstance(entry, dict)
                or set(entry)
                != {
                    "schema_version",
                    "plan_id",
                    "number",
                    "retried_at",
                    "previous",
                    "proof",
                    "attempt_id",
                }
                or type(entry["schema_version"]) is not int
                or entry["schema_version"] != 1
                or entry["plan_id"] != plan.plan_id
                or type(entry["number"]) is not int
                or entry["number"] != number
                or not isinstance(entry["attempt_id"], str)
                or not HASH_RE.fullmatch(entry["attempt_id"])
                or not hmac.compare_digest(
                    entry["attempt_id"], _digest(entry, "attempt_id")
                )
            ):
                raise ReleaseError("Release state retry history is corrupt")
            previous, proof = entry["previous"], entry["proof"]
            blueprint = blueprints[action["id"]]
            if (
                not isinstance(previous, dict)
                or set(previous) != set(blueprint) - {"attempts"}
                or any(
                    previous[key] != value
                    for key, value in blueprint.items()
                    if key not in mutable
                )
                or previous["status"] != "failed"
                or previous["receipt"] is not None
                or not _positive_id(previous["build_id"])
                or not isinstance(previous["error"], str)
                or not isinstance(previous["blocked"], list)
                or any(not isinstance(value, str) for value in previous["blocked"])
                or previous["operation"] is None
                or previous["operation"] != action["operation"]
                or previous["command"] != previous["operation"]["command"]
                or not isinstance(proof, dict)
                or set(proof) != {"build", "definition", "jobs", "absence"}
            ):
                raise ReleaseError("Release state has an invalid retired attempt")
            outcome = _validate_build(
                plan, previous, proof["build"], _RecordedDefinition(proof["definition"])
            )
            old_outcome = previous["outcome"]
            if (
                not isinstance(old_outcome, dict)
                or outcome["status"] != "completed"
                or outcome["result"] != "failed"
                or {
                    key: value
                    for key, value in old_outcome.items()
                    if key != "checked_at"
                }
                != {key: value for key, value in outcome.items() if key != "checked_at"}
                or proof["build"]
                != _evidence_build(proof["build"], previous["operation"])
                or proof["definition"] != _evidence_definition(proof["definition"])
                or not isinstance(proof["jobs"], list)
            ):
                raise ReleaseError(
                    "Retry history lacks a matching terminal failed build"
                )
            jobs = _terminal_jobs(
                {
                    "records": [
                        {**job, "type": "Job"}
                        for job in proof["jobs"]
                        if isinstance(job, dict)
                    ]
                }
            )
            if jobs != proof["jobs"]:
                raise ReleaseError("Retry history has unvalidated job fields")
            group = _operation_group(state["actions"], action)
            checked = _validate_absence(
                plan, group, state["destinations"], proof["absence"], fresh=False
            )
            retried = _time(entry["retried_at"], "Retry authorization")
            if (
                _time(previous["intent_at"], "Retired intent") > retried
                or _time(old_outcome.get("checked_at"), "Retired outcome") > retried
                or _time(proof["build"]["queueTime"], "Retired queue")
                > _time(proof["build"]["finishTime"], "Retired finish")
                or _time(proof["build"]["finishTime"], "Retired finish") > checked
                or checked > retried
                or (prior_time is not None and retried < prior_time)
                or _time(action["intent_at"], "Current intent") < retried
            ):
                raise ReleaseError("Retry history timestamps conflict")
            prior_time = retried
            binding = (previous["operation"]["id"], number)
            if bindings.setdefault(previous["build_id"], binding) != binding:
                raise ReleaseError(
                    "Retry history reuses a build ID for different attempts"
                )
            for member in group:
                if (
                    not isinstance(member.get("attempts"), list)
                    or len(member["attempts"]) != len(attempts)
                    or not isinstance(member["attempts"][number - 1], dict)
                    or member["attempts"][number - 1].get("proof") != proof
                    or member["attempts"][number - 1].get("retried_at")
                    != entry["retried_at"]
                ):
                    raise ReleaseError("Retry history lost part of a grouped operation")
        if action["build_id"] is not None:
            binding = (action["operation"]["id"], len(attempts) + 1)
            if bindings.setdefault(action["build_id"], binding) != binding:
                raise ReleaseError("Release state reused a retired Azure build ID")


def _retry(plan, store, identifier, remote):
    state = store.state
    first = next((item for item in state["actions"] if item["id"] == identifier), None)
    if first is None or first["status"] != "failed" or first["operation"] is None:
        raise ReleaseError(
            "Retry requires an already-recorded terminal failed operation"
        )
    if first["kind"] != "publisher":
        raise ReleaseError(
            "Maven retry is blocked: aggregate MISSING is not definitive namespace absence"
        )
    group = _operation_group(state["actions"], first)
    if any(
        item["status"] != "failed"
        or item["build_id"] != first["build_id"]
        or item["operation"] != first["operation"]
        or not _positive_id(item["build_id"])
        or len(item["attempts"]) >= MAX_RETRY_ATTEMPTS
        for item in group
    ):
        raise ReleaseError(
            "Retry requires one coherent failed group within the attempt limit"
        )
    build = remote.build(first["build_id"])
    outcome = _validate_build(plan, first, build, remote)
    if (
        outcome["status"] != "completed"
        or outcome["result"] != "failed"
        or not isinstance(first["outcome"], dict)
        or first["outcome"].get("status") != "completed"
        or first["outcome"].get("result") != "failed"
        or any(
            first["outcome"].get(key) != outcome[key]
            for key in ("source_commit", "source_branch")
        )
    ):
        raise ReleaseError(
            "Retry refused: Azure no longer confirms the same terminal failed source"
        )
    jobs = _terminal_jobs(remote.timeline(first["build_id"]))
    for item in group:
        rows = _artifact_rows(
            state["inventory"], item["repository"], item["target"], item["family"]
        )
        if not rows or any(row["status"] != verify.MISSING for row in rows):
            raise ReleaseError(
                "Retry refused: selected artifacts are present or only partially absent"
            )
        if item["blocked"]:
            raise ReleaseError("Retry refused: required dependencies are not available")
    absence = remote.absence(plan, group, state["destinations"])
    _validate_absence(plan, group, state["destinations"], absence)
    proof = {
        "build": _evidence_build(build, first["operation"]),
        "definition": _evidence_definition(remote.definition(first["pipeline_id"])),
        "jobs": jobs,
        "absence": copy.deepcopy(absence),
    }
    retry_snapshot = copy.deepcopy(state)
    timestamp = now()
    for item in group:
        entry = {
            "schema_version": 1,
            "plan_id": plan.plan_id,
            "number": len(item["attempts"]) + 1,
            "retried_at": timestamp,
            "previous": {
                key: copy.deepcopy(value)
                for key, value in item.items()
                if key != "attempts"
            },
            "proof": copy.deepcopy(proof),
            "attempt_id": "",
        }
        entry["attempt_id"] = _digest(entry, "attempt_id")
        item["attempts"].append(entry)
    # History and the next unknown intent commit together; no replayable planned gap.
    _queue(plan, store, group, remote, retry_snapshot=retry_snapshot)


def _queue(plan, store, actions, remote, retry_snapshot=None):
    store.state["policy"] = _policy(plan, remote)
    if retry_snapshot is not None:
        absence = actions[0]["attempts"][-1]["proof"]["absence"]
        _validate_absence(plan, actions, store.state["destinations"], absence)
    operation = _operation(plan, actions[0], [action["family"] for action in actions])
    timestamp = now()
    for action in actions:
        action.update(
            operation=copy.deepcopy(operation),
            command=list(operation["command"]),
            status="unknown",
            intent_at=timestamp,
            build_id=None,
            outcome=None,
            receipt=None,
            error="Submission intent recorded; Azure outcome is not yet confirmed.",
        )
    # This write must precede the request, including on the very first invocation.
    store.save()
    if retry_snapshot is not None:
        try:
            _validate_absence(plan, actions, store.state["destinations"], absence)
        except ReleaseError as error:
            # No request was sent; restore the failed attempt rather than strand an unknown one.
            retry_snapshot["revision"] = store.state["revision"]
            store.state = retry_snapshot
            store.save()
            raise ReleaseError(
                f"Retry not submitted; original failed attempt restored. {error}"
            ) from error
    try:
        returned = remote.queue(operation["command"])
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
        for action in actions:
            action[
                "error"
            ] = "Queue response is ambiguous. Use --adopt with a verified run ID; do not retry."
        store.save()
        return
    if not isinstance(returned, dict) or not _positive_id(returned.get("id")):
        for action in actions:
            action[
                "error"
            ] = "Queue returned no valid Azure build ID. Reconcile; do not retry."
        store.save()
        return
    for action in actions:
        action["build_id"] = returned["id"]
    print(
        f"Azure build {returned['id']} accepted for operation {operation['id']}; "
        f"reconcile with --adopt {actions[0]['id']}={returned['id']} if persistence fails.",
        file=sys.stderr,
        flush=True,
    )
    # Preserve the ID before source checks, artifact downloads or another queue.
    store.save()
    if any(
        field in returned
        for field in (
            "sourceVersion",
            "sourceBranch",
            "definition",
            "templateParameters",
        )
    ):
        try:
            _validate_build(plan, actions[0], returned, remote)
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
            for action in actions:
                action["error"] = _safe_error(error)
            store.save()
            return
    _refresh_group(plan, store.state, actions, remote)
    store.save()


def _execute(plan, store, remote):
    state = store.state
    attempted = set()
    for action in state["actions"]:
        _observe(state)
        if (
            action["status"] != "planned"
            or action["operation"] is not None
            or action["blocked"]
            or action["id"] in attempted
        ):
            continue
        group = [action]
        if action["kind"] == "publisher":
            group = [
                other
                for other in state["actions"]
                if other["kind"] == "publisher"
                and other["repository"] == action["repository"]
                and other["target"] == action["target"]
                and other["status"] == "planned"
                and other["operation"] is None
                and not other["blocked"]
            ]
        attempted.update(value["id"] for value in group)
        _queue(plan, store, group, remote)
    _observe(state)


def _report(state, apply):
    complete = (
        bool(state["actions"])
        and all(action["status"] == "complete" for action in state["actions"])
        and state["inventory"]["inventory_complete"]
    )
    return {
        "schema_version": STATE_VERSION,
        "plan_id": state["plan_id"],
        "scope": state["plan"]["scope"],
        "repositories": state["plan"]["repositories"],
        "families": state["plan"]["families"],
        "mode": state["plan"]["mode"],
        "checked_at": now(),
        "apply": apply,
        "complete": complete,
        "destinations": state["destinations"],
        "policy": state["policy"],
        "inventory": state["inventory"],
        "dependency_inventory": state["dependency_inventory"],
        "actions": [
            {
                **{
                    key: copy.deepcopy(action[key])
                    for key in (
                        "id",
                        "kind",
                        "repository",
                        "target",
                        "family",
                        "pipeline_id",
                        "source_tag",
                        "source_commit",
                        "version",
                        "dependencies",
                        "status",
                        "build_id",
                        "outcome",
                        "error",
                        "blocked",
                    )
                },
                "attempts": [
                    {
                        "number": attempt["number"],
                        "retried_at": attempt["retried_at"],
                        "build_id": attempt["previous"]["build_id"],
                        "operation_id": attempt["previous"]["operation"]["id"],
                        "status": attempt["previous"]["status"],
                        "error": attempt["previous"]["error"],
                        "attempt_id": attempt["attempt_id"],
                        "absence_checked_at": attempt["proof"]["absence"]["checked_at"],
                    }
                    for attempt in action["attempts"]
                ],
            }
            for action in state["actions"]
        ],
        "human_gates": (
            "ESRP/SAW and other pipeline approvals remain manual. "
            "This command never creates tags, GitHub releases or BBC-VHD changes."
        ),
    }


def _evidence_definition(definition):
    return {
        "id": definition["id"],
        "repository": {
            key: definition["repository"][key] for key in ("id", "name", "type")
        },
    }


def _evidence_build(build, operation):
    parameters, variables = _parameters(build), _variables(build)
    return {
        "id": build["id"],
        "definition": {"id": build["definition"]["id"]},
        "repository": {key: build["repository"][key] for key in ("id", "name", "type")},
        "sourceBranch": build["sourceBranch"],
        "sourceVersion": build["sourceVersion"],
        "templateParameters": {key: parameters[key] for key in operation["parameters"]},
        "parameters": canonical(
            {key: variables[key] for key in operation["variables"]}
        ).decode("ascii"),
        "status": build["status"],
        "result": build["result"],
        "queueTime": build["queueTime"],
        "finishTime": build["finishTime"],
    }


class _RecordedDefinition:
    def __init__(self, definition):
        self.value = definition

    def definition(self, _pipeline_id):
        return self.value


def _validate_evidence_destinations(plan, destinations):
    families = {family for family in plan.families if family in {"pip", "upack"}}
    if plan.mode == "rehearsal" and families:
        families = {"pip", "upack"}
    if not isinstance(destinations, dict) or set(destinations) != families:
        raise ReleaseError("Producer evidence omits approved destination bindings")
    for family, feed in destinations.items():
        if (
            not isinstance(feed, dict)
            or set(feed) != {"requested", "id", "name", "project", "project_id"}
            or feed["requested"] != getattr(plan, family + "_feed")
            or not isinstance(feed["id"], str)
            or not GUID_RE.fullmatch(feed["id"])
            or not isinstance(feed["name"], str)
            or feed["name"].casefold()
            != feed["requested"].rsplit("/", 1)[-1].casefold()
            or feed["project"] != matrix.ADO_PROJECT
            or feed["project_id"] != ADO_PROJECT_ID
        ):
            raise ReleaseError("Producer evidence has an invalid destination")
        if plan.mode == "production":
            if feed["id"] != PRODUCTION_FEED_IDS[family]:
                raise ReleaseError(
                    "Producer evidence changed a production feed identity"
                )
        elif feed["id"].lower() in PRODUCTION_FEED_IDS.values():
            raise ReleaseError("Rehearsal evidence uses a production feed identity")
    if len(destinations) == 2 and (
        destinations["pip"]["id"].lower() == destinations["upack"]["id"].lower()
    ):
        raise ReleaseError("Producer evidence requires distinct feed destinations")


def validate_producer_evidence(plan, report):
    """Validate exported producer facts without authenticating or trusting summaries."""
    plan = matrix.load_plan(matrix.plan_to_dict(plan), require_bound=True)
    verify.validate_inventory(plan, report)
    expected_keys = set(verify.build_report(plan, report["rows"], True)) | {
        "producer_evidence"
    }
    if (
        set(report) != expected_keys
        or report.get("complete") is not True
        or report.get("evidence_kind") != "producer-verified"
    ):
        raise ReleaseError(
            "Producer evidence is absent or has an invalid report envelope"
        )
    evidence = report["producer_evidence"]
    if (
        not isinstance(evidence, dict)
        or set(evidence)
        != {"schema_version", "plan_id", "checked_at", "destinations", "runs"}
        or type(evidence["schema_version"]) is not int
        or evidence["schema_version"] != 1
        or evidence["plan_id"] != plan.plan_id
        or evidence["checked_at"] != report["checked_at"]
        or not isinstance(evidence["runs"], list)
        or not evidence["runs"]
    ):
        raise ReleaseError("Producer evidence has an invalid identity or run inventory")
    _time(evidence["checked_at"], "Producer evidence")
    _validate_evidence_destinations(plan, evidence["destinations"])
    blueprints = {action["id"]: action for action in build_actions(plan)}
    seen_actions, seen_builds = set(), set()
    build_keys = {
        "id",
        "definition",
        "repository",
        "sourceBranch",
        "sourceVersion",
        "templateParameters",
        "parameters",
        "status",
        "result",
        "queueTime",
        "finishTime",
    }
    for run in evidence["runs"]:
        if not isinstance(run, dict) or set(run) != {
            "action_ids",
            "operation",
            "build",
            "definition",
            "jobs",
            "provenance",
        }:
            raise ReleaseError("Producer evidence contains an invalid run")
        ids = run["action_ids"]
        if (
            not isinstance(ids, list)
            or not ids
            or any(
                not isinstance(value, str) or value not in blueprints for value in ids
            )
            or len(ids) != len(set(ids))
            or seen_actions.intersection(ids)
        ):
            raise ReleaseError(
                "Producer evidence has duplicate or out-of-scope actions"
            )
        actions = [blueprints[value] for value in ids]
        first = actions[0]
        if any(
            (action["kind"], action["repository"], action["target"])
            != (first["kind"], first["repository"], first["target"])
            for action in actions
        ):
            raise ReleaseError("Producer evidence groups unrelated operations")
        operation = _operation(plan, first, [action["family"] for action in actions])
        if run["operation"] != operation:
            raise ReleaseError(
                "Producer request differs from its approved action subset"
            )
        build, definition = run["build"], run["definition"]
        if (
            not isinstance(build, dict)
            or set(build) != build_keys
            or not _positive_id(build.get("id"))
            or build["id"] in seen_builds
            or not isinstance(build.get("definition"), dict)
            or set(build["definition"]) != {"id"}
            or not isinstance(build.get("repository"), dict)
            or set(build["repository"]) != {"id", "name", "type"}
            or not isinstance(definition, dict)
            or set(definition) != {"id", "repository"}
            or not isinstance(definition["repository"], dict)
            or set(definition["repository"]) != {"id", "name", "type"}
        ):
            raise ReleaseError(
                "Producer evidence has invalid or duplicate Azure build facts"
            )
        candidate = {**first, "operation": operation, "build_id": build["id"]}
        outcome = _validate_build(
            plan, candidate, build, _RecordedDefinition(definition)
        )
        if outcome["status"] != "completed" or outcome["result"] != "succeeded":
            raise ReleaseError(
                "Producer evidence does not describe a succeeded Azure build"
            )
        if not isinstance(run["jobs"], list) or any(
            not isinstance(job, dict) or set(job) != {"id", "name", "state", "result"}
            for job in run["jobs"]
        ):
            raise ReleaseError("Producer evidence has invalid Azure job facts")
        _jobs({"records": [{**job, "type": "Job"} for job in run["jobs"]]})
        documents = _validate_manifests(
            plan,
            candidate,
            run["provenance"],
            build["id"],
            publisher_commit=build["sourceVersion"],
            destinations=evidence["destinations"],
        )
        if documents != run["provenance"]:
            raise ReleaseError("Producer evidence contains unvalidated artifact fields")
        seen_actions.update(ids)
        seen_builds.add(build["id"])
    if seen_actions != set(blueprints):
        raise ReleaseError("Producer evidence omits required action coverage")


def verified_evidence(plan, state_path, remote=None):
    """Refresh a ledger read-only against Azure and export safe producer evidence."""
    plan = matrix.load_plan(matrix.plan_to_dict(plan), require_bound=True)
    service = remote if remote is not None else AzureRemote()
    with StateStore(state_path, plan, must_exist=True) as store:
        state = store.state
        policy, destinations, inventory, dependencies = _probes(plan, service)
        if state["destinations"] != destinations:
            raise ReleaseError("Resolved feed destination changed since preflight")
        state.update(
            policy=policy,
            destinations=destinations,
            inventory=inventory,
            dependency_inventory=dependencies,
        )
        _observe(state)
        _refresh(plan, state, service)
        store.save()
        report = copy.deepcopy(inventory)
        if not _report(state, False)["complete"]:
            return report
        groups = {}
        for action in state["actions"]:
            groups.setdefault(
                (action["operation"]["id"], action["build_id"]), []
            ).append(action)
        runs = []
        for actions in groups.values():
            first = actions[0]
            build = service.build(first["build_id"])
            outcome = _validate_build(plan, first, build, service)
            if outcome["status"] != "completed" or outcome["result"] != "succeeded":
                raise ReleaseError(
                    "Producer build changed while evidence was collected"
                )
            if outcome["source_commit"] != first["outcome"]["source_commit"]:
                raise ReleaseError(
                    "Producer source changed while evidence was collected"
                )
            runs.append(
                {
                    "action_ids": [action["id"] for action in actions],
                    "operation": copy.deepcopy(first["operation"]),
                    "build": _evidence_build(build, first["operation"]),
                    "definition": _evidence_definition(
                        service.definition(first["pipeline_id"])
                    ),
                    "jobs": copy.deepcopy(first["receipt"]["jobs"]),
                    "provenance": copy.deepcopy(first["receipt"]["provenance"]),
                }
            )
        report.update(
            complete=True,
            evidence_kind="producer-verified",
            producer_evidence={
                "schema_version": 1,
                "plan_id": plan.plan_id,
                "checked_at": report["checked_at"],
                "destinations": copy.deepcopy(destinations),
                "runs": runs,
            },
        )
        validate_producer_evidence(plan, report)
        return report


def main(argv=None, remote=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("preflight", "status", "resume"):
        command = commands.add_parser(name)
        command.add_argument("--plan", required=True)
        command.add_argument("--state", required=name != "preflight")
        if name == "status":
            command.add_argument(
                "--inspect-lock",
                action="store_true",
                help="Read bounded lock metadata without acquiring locks or contacting Azure",
            )
        if name == "resume":
            command.add_argument("--apply", action="store_true")
            command.add_argument("--approve-plan", metavar="PLAN_ID")
            command.add_argument(
                "--adopt",
                action="append",
                default=[],
                metavar="ACTION_ID=BUILD_ID",
                help="Reconcile matching known Azure runs; adoption does not queue new jobs",
            )
            command.add_argument(
                "--retry",
                action="append",
                default=[],
                metavar="ACTION_ID",
                help="Retry one recorded failed publisher group after definitive absence checks",
            )
    args = parser.parse_args(argv)
    apply = getattr(args, "apply", False)
    approved = getattr(args, "approve_plan", None)
    adoptions = getattr(args, "adopt", [])
    retries = getattr(args, "retry", [])
    try:
        plan = matrix.read_plan(args.plan, require_bound=True)
        plan = matrix.load_plan(matrix.plan_to_dict(plan), require_bound=True)
        if apply != bool(approved) or (
            approved is not None
            and (
                not HASH_RE.fullmatch(approved)
                or not hmac.compare_digest(approved, plan.plan_id)
            )
        ):
            raise ReleaseError(
                "Approval requires --apply AND --approve-plan matching the validated plan_id"
            )
        if adoptions and not apply:
            raise ReleaseError("Adoption requires --apply and matching --approve-plan")
        if retries and (not apply or len(retries) != 1 or adoptions):
            raise ReleaseError(
                "Retry requires --apply, matching --approve-plan and exactly one --retry; "
                "it cannot be combined with --adopt"
            )
        if args.state and (
            args.state == "-"
            or (
                args.plan != "-"
                and Path(args.state).resolve() == Path(args.plan).resolve()
            )
        ):
            raise ReleaseError("Use a separate local state file, not the plan or stdin")
        if getattr(args, "inspect_lock", False):
            print(json.dumps(inspect_locks(plan, args.state), indent=2, sort_keys=True))
            return 0
        context = (
            StateStore(
                args.state, plan, must_exist=args.command == "status" or bool(retries)
            )
            if args.state
            else nullcontext(None)
        )
        with context as store:
            state = store.state if store is not None else _new_state(plan)
            service = remote if remote is not None else AzureRemote()
            policy, destinations, inventory, dependencies = _probes(plan, service)
            if (
                state["destinations"] is not None
                and state["destinations"] != destinations
            ):
                raise ReleaseError(
                    "Resolved feed destination changed since preflight; refusing stale state"
                )
            state.update(
                policy=policy,
                destinations=destinations,
                inventory=inventory,
                dependency_inventory=dependencies,
            )
            _observe(state)
            if retries:
                _retry(plan, store, retries[0], service)
            else:
                if adoptions:
                    _adopt(plan, state, adoptions, service)
                    store.save()
                _refresh(plan, state, service)
            if store is not None:
                store.save()
            if apply and not adoptions and not retries:
                _execute(plan, store, service)
            if store is not None:
                store.save()
            report = _report(state, apply)
            print(json.dumps(report, indent=2, sort_keys=True))
            return 0 if args.command == "preflight" or report["complete"] else 1
    except (ValueError, RuntimeError, OSError) as error:
        message = (
            str(error)
            if isinstance(error, (ValueError, ReleaseError))
            else _safe_error(error)
        )
        print(f"error: {message}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
