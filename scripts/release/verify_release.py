#!/usr/bin/env python3
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
"""
Verify a SynapseML release end-to-end against live sources of truth.

Automates the artifact checks in Release Guide Steps 1-3, and doubles as a
regression test for `release_matrix.py`: run it against an already-shipped
version and every non-skipped row must be PRESENT.

NOTE: the wiki currently tells you to run `az artifacts universal show`.
That command does not exist in the Azure CLI. This uses the Azure Artifacts
REST API instead, which works.

Auth: internal checks use ADO_TOKEN when set, otherwise the script shells out to
    az account get-access-token --resource 499b84ac-1321-427f-aa17-267ca6975798
GitHub checks use GH_TOKEN when set and otherwise use the unauthenticated API.

Usage:
    python scripts/release/verify_release.py --version 1.1.3
    python scripts/release/verify_release.py --version 1.1.4 --internal-patch 0 --json
    python scripts/release/verify_release.py --version 1.1.3 --internal-patch 1 --scope internal-only
    python scripts/release/verify_release.py --version 1.1.4 --skip ado,internal
    python scripts/release/verify_release.py --version 1.1.4 --skip internal
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, __file__.rsplit("/", 1)[0].rsplit("\\", 1)[0])
from release_matrix import (  # noqa: E402
    ADO_ORG,
    ADO_PROJECT,
    RELEASE_SCOPES,
    build_plan,
    parse_iterations,
)

ADO_RESOURCE = "499b84ac-1321-427f-aa17-267ca6975798"
ORG_SHORT = "msdata"
GITHUB_REPO = "microsoft/SynapseML"
INTERNAL_REPO = "SynapseML-Internal"
MAVEN_BASE = "https://mmlspark.azureedge.net/maven"
PUBLIC_MAVEN_MODULES = (
    "synapseml",
    "synapseml-core",
    "synapseml-cognitive",
    "synapseml-deep-learning",
    "synapseml-lightgbm",
    "synapseml-opencv",
    "synapseml-vw",
)
INTERNAL_MAVEN_MODULE = "synapseml-internal"
PYPI_BASE = "https://pypi.org/pypi"
SKIP_CHOICES = {"github", "ado", "upack", "pip", "internal", "public"}

OK, MISSING, SKIPPED = "PRESENT", "MISSING", "SKIPPED"


def _get_ado_token(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    command = [
        "az",
        "account",
        "get-access-token",
        "--resource",
        ADO_RESOURCE,
        "--query",
        "accessToken",
        "-o",
        "tsv",
    ]
    use_shell = sys.platform == "win32"
    try:
        out = subprocess.run(
            subprocess.list2cmdline(command) if use_shell else command,
            capture_output=True,
            text=True,
            shell=use_shell,
        )
    except OSError as e:
        raise RuntimeError(
            "could not run Azure CLI 'az'; install Azure CLI and sign in, "
            f"or set ADO_TOKEN: {e}"
        ) from e
    if out.returncode != 0:
        raise RuntimeError(f"could not get ADO token: {out.stderr.strip()}")
    token = out.stdout.strip()
    if not token:
        raise RuntimeError("Azure CLI returned an empty ADO token")
    return token


def _json_get_page(url: str, headers: Dict[str, str]) -> Tuple[Optional[dict], object]:
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            body = r.read()
            response_headers = r.headers
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None, {}
        raise RuntimeError(f"HTTP {e.code} for {url}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"request failed for {url}: {e.reason}") from e
    try:
        return json.loads(body.decode("utf-8")), response_headers
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise RuntimeError(f"invalid JSON response from {url}: {e}") from e


def _json_get(url: str, headers: Dict[str, str]) -> Optional[dict]:
    data, _ = _json_get_page(url, headers)
    return data


def _url_exists(url: str, headers: Dict[str, str]) -> bool:
    for method in ("HEAD", "GET"):
        req = urllib.request.Request(url, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=60):
                return True
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return False
            if method == "HEAD" and e.code in {405, 501}:
                continue
            raise RuntimeError(f"HTTP {e.code} for {url}") from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"request failed for {url}: {e.reason}") from e
    raise AssertionError("GET fallback did not return a result")


def _with_query(url: str, **updates: str) -> str:
    parts = urllib.parse.urlsplit(url)
    query = dict(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
    query.update(updates)
    return urllib.parse.urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urllib.parse.urlencode(query),
            parts.fragment,
        )
    )


class Checker:
    def __init__(self, token: Optional[str], gh_token: Optional[str], skip: List[str]):
        self.skip = set(skip)
        self._ado_headers = None
        needs_ado = (
            "ado" not in self.skip
            and not {
                "upack",
                "pip",
                "internal",
            }
            <= self.skip
        )
        if needs_ado:
            self._ado_headers = {"Authorization": f"Bearer {_get_ado_token(token)}"}
        self._gh_headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "synapseml-release-verify",
        }
        self._public_headers = {"User-Agent": "synapseml-release-verify"}
        if gh_token:
            self._gh_headers["Authorization"] = f"Bearer {gh_token}"
        self._pkg_cache: Dict[Tuple[str, str, str], List[str]] = {}

    # --- git tags ---------------------------------------------------------
    def github_tag(self, tag: str) -> Tuple[str, Optional[str]]:
        if "github" in self.skip:
            return SKIPPED, None
        encoded_tag = urllib.parse.quote(tag, safe="")
        url = f"https://api.github.com/repos/{GITHUB_REPO}/git/ref/tags/{encoded_tag}"
        data = _json_get(url, self._gh_headers)
        if data is None:
            return MISSING, None

        obj = data.get("object")
        for _ in range(5):
            if not isinstance(obj, dict):
                raise RuntimeError(f"GitHub tag {tag} has no object")
            object_type = obj.get("type")
            sha = obj.get("sha")
            if object_type == "commit" and isinstance(sha, str) and sha:
                return OK, sha
            if object_type != "tag" or not isinstance(obj.get("url"), str):
                raise RuntimeError(
                    f"GitHub tag {tag} has unsupported object type {object_type!r}"
                )
            data = _json_get(obj["url"], self._gh_headers)
            if data is None:
                raise RuntimeError(f"annotated GitHub tag object not found for {tag}")
            obj = data.get("object")
        raise RuntimeError(f"GitHub tag {tag} has more than five annotation layers")

    def _maven(
        self, module: str, scala: str, version: str, require_tests: bool = False
    ) -> str:
        artifact = f"{module}_{scala}"
        escaped_version = urllib.parse.quote(version, safe="")
        base = (
            f"{MAVEN_BASE}/com/microsoft/azure/{artifact}/{escaped_version}/"
            f"{artifact}-{escaped_version}"
        )
        files = [f"{base}.pom", f"{base}.jar"]
        if require_tests:
            files.append(f"{base}-tests.jar")
        return (
            OK
            if all(_url_exists(url, self._public_headers) for url in files)
            else MISSING
        )

    def public_maven(self, module: str, scala: str, version: str) -> str:
        if "public" in self.skip:
            return SKIPPED
        return self._maven(
            module,
            scala,
            version,
            require_tests=module == "synapseml-core",
        )

    def internal_maven(self, scala: str, version: str) -> str:
        if "internal" in self.skip:
            return SKIPPED
        return self._maven(INTERNAL_MAVEN_MODULE, scala, version)

    def public_pypi(self, version: str) -> str:
        if "public" in self.skip:
            return SKIPPED
        url = f"{PYPI_BASE}/synapseml/{urllib.parse.quote(version, safe='')}/json"
        data = _json_get(url, self._public_headers)
        published = data and data.get("info", {}).get("version") == version
        return OK if published else MISSING

    def ado_tag(self, tag: str) -> Tuple[str, Optional[str]]:
        if "ado" in self.skip or "internal" in self.skip:
            return SKIPPED, None
        url = (
            f"https://dev.azure.com/{ORG_SHORT}/{ADO_PROJECT}/_apis/git/repositories/"
            f"{INTERNAL_REPO}/refs?filter=tags/{tag}&peelTags=true&api-version=7.1"
        )
        data = _json_get(url, self._ado_headers)
        if data is None:
            raise RuntimeError(f"SynapseML-Internal refs endpoint not found: {url}")
        refs = data.get("value")
        if not isinstance(refs, list):
            raise RuntimeError("SynapseML-Internal refs response has no value list")
        wanted = f"refs/tags/{tag}"
        found = next((value for value in refs if value.get("name") == wanted), None)
        if found is None:
            return MISSING, None
        peeled = found.get("peeledObjectId")
        commit = (
            peeled
            if isinstance(peeled, str) and peeled.strip("0")
            else found.get("objectId")
        )
        if not isinstance(commit, str) or not commit:
            raise RuntimeError(f"SynapseML-Internal tag {tag} has no object ID")
        return OK, commit

    # --- artifact feeds ---------------------------------------------------
    def _feed_versions(self, feed: str, protocol: str, package: str) -> List[str]:
        normalized = package.lower()
        key = (feed, protocol.lower(), normalized)
        if key in self._pkg_cache:
            return self._pkg_cache[key]

        url = (
            f"https://feeds.dev.azure.com/{ORG_SHORT}/{ADO_PROJECT}/_apis/packaging/"
            f"Feeds/{urllib.parse.quote(feed, safe='')}/packages"
        )
        url = _with_query(
            url,
            **{
                "protocolType": protocol,
                "packageNameQuery": package,
                "includeAllVersions": "true",
                "api-version": "7.1-preview.1",
            },
        )

        versions: List[str] = []
        while url:
            data, headers = _json_get_page(url, self._ado_headers)
            if data is None:
                raise RuntimeError(f"Azure Artifacts feed not found: {feed}")
            packages = data.get("value")
            if not isinstance(packages, list):
                raise RuntimeError(
                    f"Azure Artifacts response for {feed}/{package} has no value list"
                )
            for item in packages:
                if item.get("name", "").lower() == normalized:
                    versions.extend(
                        version["version"]
                        for version in item.get("versions", [])
                        if "version" in version
                    )
            continuation = headers.get("x-ms-continuationtoken")
            url = (
                _with_query(url, continuationToken=continuation) if continuation else ""
            )

        self._pkg_cache[key] = versions
        return versions

    def upack(self, package: str, version: str, internal: bool = False) -> str:
        if (
            "upack" in self.skip
            or "ado" in self.skip
            or (internal and "internal" in self.skip)
        ):
            return SKIPPED
        return (
            OK
            if version
            in self._feed_versions("BBC-VHD_PublicPackages", "upack", package)
            else MISSING
        )

    def pip(self, package: str, version: str, internal: bool = False) -> str:
        if (
            "pip" in self.skip
            or "ado" in self.skip
            or (internal and "internal" in self.skip)
        ):
            return SKIPPED
        # Azure Artifacts normalises pypi names: synapseml_internal -> synapseml-internal
        return (
            OK
            if version
            in self._feed_versions("Synapse-Conda", "pypi", package.replace("_", "-"))
            else MISSING
        )


def run(
    version: str,
    internal_patch: str,
    target_keys,
    token,
    gh_token,
    skip,
    upack_iteration=None,
    internal_upack_iteration=None,
    scope=None,
) -> Tuple[List[dict], bool]:
    scope = _resolve_scope(scope, internal_patch)
    plan = build_plan(
        version,
        internal_patch,
        target_keys,
        upack_iteration,
        internal_upack_iteration,
        scope,
    )
    c = Checker(token, gh_token, skip)
    rows: List[dict] = []
    include_oss = plan.scope == "full"

    def add(kind, target, name, ident, status):
        rows.append(
            {
                "kind": kind,
                "target": target,
                "name": name,
                "identifier": ident,
                "status": status,
            }
        )

    def add_tag_family(target, name, tags, check):
        results = []
        for tag in tags:
            status, commit = check(tag)
            add("git-tag", target, name, tag, status)
            results.append((status, commit))

        if all(status == SKIPPED for status, _ in results):
            consistency = SKIPPED
        elif all(status == OK and commit for status, commit in results):
            consistency = OK if len({commit for _, commit in results}) == 1 else MISSING
        else:
            consistency = MISSING
        add(
            "tag-set",
            target,
            name + "/same-commit",
            ", ".join(tags),
            consistency,
        )

    for tp in plan.targets:
        if include_oss:
            add_tag_family(
                tp.key,
                "github/" + GITHUB_REPO,
                tp.oss_tags,
                c.github_tag,
            )
            for module in PUBLIC_MAVEN_MODULES:
                artifact = f"{module}_{tp.scala}"
                add(
                    "maven",
                    tp.key,
                    artifact,
                    tp.oss_maven_version,
                    c.public_maven(module, tp.scala, tp.oss_maven_version),
                )
        add(
            "maven",
            tp.key,
            f"{INTERNAL_MAVEN_MODULE}_{tp.scala}",
            tp.internal_maven_version,
            c.internal_maven(tp.scala, tp.internal_maven_version),
        )
        if include_oss and tp.key == "master":
            add(
                "pypi",
                tp.key,
                "pypi/synapseml",
                plan.oss_version,
                c.public_pypi(plan.oss_version),
            )
        add_tag_family(
            tp.key,
            "ado/" + INTERNAL_REPO,
            tp.internal_tags,
            c.ado_tag,
        )
        if include_oss:
            add(
                "upack",
                tp.key,
                "synapseml",
                tp.oss_upack_version,
                c.upack("synapseml", tp.oss_upack_version),
            )
        add(
            "upack",
            tp.key,
            "synapseml_internal",
            tp.internal_upack_version,
            c.upack(
                "synapseml_internal",
                tp.internal_upack_version,
                internal=True,
            ),
        )
        if include_oss:
            add(
                "pip",
                tp.key,
                "synapseml",
                tp.oss_pip_version,
                c.pip("synapseml", tp.oss_pip_version),
            )
        add(
            "pip",
            tp.key,
            "synapseml-internal",
            tp.internal_pip_version,
            c.pip(
                "synapseml_internal",
                tp.internal_pip_version,
                internal=True,
            ),
        )

    ok = not any(r["status"] == MISSING for r in rows)
    return rows, ok


def _resolve_scope(scope: Optional[str], internal_patch: str) -> str:
    return scope or ("internal-only" if internal_patch != "0" else "full")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Verify a SynapseML release's artifacts and tags."
    )
    p.add_argument("--version", required=True)
    p.add_argument("--internal-patch", default="0")
    p.add_argument(
        "--scope",
        choices=RELEASE_SCOPES,
        default=None,
        help=(
            "Verify the full release or only a nonzero Internal patch "
            "(default: infer from --internal-patch)"
        ),
    )
    p.add_argument("--targets", default="")
    p.add_argument(
        "--upack-iteration",
        default="",
        metavar="KEY=N",
        help="OSS UPack rebuild counters, e.g. spark4.0=1",
    )
    p.add_argument(
        "--internal-upack-iteration",
        default="",
        metavar="KEY=N",
        help="Internal UPack rebuild counters, e.g. spark4.0=1",
    )
    p.add_argument(
        "--skip",
        default="",
        help=(
            "Comma-separated checks to skip: github (OSS tags), "
            "ado (all ADO-backed checks), upack (all UPacks), "
            "pip (all Synapse-Conda wheels), internal "
            "(Internal tags, Maven, UPacks, and wheels), "
            "public (OSS Maven CDN and PyPI)"
        ),
    )
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)

    keys = [k.strip() for k in args.targets.split(",") if k.strip()] or None
    skip = [s.strip() for s in args.skip.split(",") if s.strip()]
    unknown_skip = sorted(set(skip) - SKIP_CHOICES)
    if unknown_skip:
        print(
            f"error: unknown --skip value(s): {unknown_skip}; "
            f"known: {sorted(SKIP_CHOICES)}",
            file=sys.stderr,
        )
        return 2

    try:
        scope = _resolve_scope(args.scope, args.internal_patch)
        upack_iteration = parse_iterations(args.upack_iteration, "--upack-iteration")
        internal_upack_iteration = parse_iterations(
            args.internal_upack_iteration,
            "--internal-upack-iteration",
        )
        rows, ok = run(
            args.version,
            args.internal_patch,
            keys,
            os.environ.get("ADO_TOKEN"),
            os.environ.get("GH_TOKEN"),
            skip,
            upack_iteration,
            internal_upack_iteration,
            scope=scope,
        )
    except (ValueError, RuntimeError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    if args.json:
        print(
            json.dumps(
                {
                    "version": args.version,
                    "internal_patch": args.internal_patch,
                    "scope": scope,
                    "complete": ok,
                    "rows": rows,
                },
                indent=2,
            )
        )
    else:
        print(
            f"SynapseML verification  OSS v{args.version}  "
            f"Internal patch {args.internal_patch}  scope={scope}"
        )
        print(
            f"{'STATUS':<8} {'KIND':<8} {'TARGET':<9} {'PACKAGE/REPO':<30} IDENTIFIER"
        )
        for r in rows:
            print(
                f"{r['status']:<8} {r['kind']:<8} {r['target']:<9} {r['name']:<30} {r['identifier']}"
            )
        n_missing = sum(1 for r in rows if r["status"] == MISSING)
        print("")
        print(
            f"{len(rows)} checks, {n_missing} missing -> {'COMPLETE' if ok else 'INCOMPLETE'}"
        )

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
