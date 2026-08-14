#!/usr/bin/env python3
"""
Verify a SynapseML release end-to-end against live sources of truth.

Replaces the manual "Step 4 - Verify Artifacts" checklist, and doubles as a
regression test for `release_matrix.py`: run it against an already-shipped
version and every row must be PRESENT.

NOTE: the wiki currently tells you to run `az artifacts universal show`.
That command does not exist in the Azure CLI. This uses the Azure Artifacts
REST API instead, which works.

Auth: needs an Azure DevOps bearer token. Either
    --token <pat-or-bearer>
or leave it out and the script shells out to
    az account get-access-token --resource 499b84ac-1321-427f-aa17-267ca6975798

Usage:
    python scripts/release/verify_release.py --version 1.1.3
    python scripts/release/verify_release.py --version 1.1.4 --internal-patch 0 --json
    python scripts/release/verify_release.py --version 1.1.4 --skip ado   # GitHub only
"""

from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys
import urllib.error
import urllib.request
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, __file__.rsplit("/", 1)[0].rsplit("\\", 1)[0])
from release_matrix import ADO_ORG, ADO_PROJECT, build_plan  # noqa: E402

ADO_RESOURCE = "499b84ac-1321-427f-aa17-267ca6975798"
ORG_SHORT = "msdata"
GITHUB_REPO = "microsoft/SynapseML"
INTERNAL_REPO = "SynapseML-Internal"

OK, MISSING, SKIPPED = "PRESENT", "MISSING", "SKIPPED"


def _get_ado_token(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    out = subprocess.run(
        [
            "az",
            "account",
            "get-access-token",
            "--resource",
            ADO_RESOURCE,
            "--query",
            "accessToken",
            "-o",
            "tsv",
        ],
        capture_output=True,
        text=True,
        shell=(sys.platform == "win32"),
    )
    if out.returncode != 0:
        raise RuntimeError(f"could not get ADO token: {out.stderr.strip()}")
    return out.stdout.strip()


def _json_get(url: str, headers: Dict[str, str]) -> Optional[dict]:
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            raise RuntimeError(f"auth failed ({e.code}) for {url}") from e
        return None
    except urllib.error.URLError:
        return None


class Checker:
    def __init__(self, token: Optional[str], gh_token: Optional[str], skip: List[str]):
        self.skip = set(skip)
        self._ado_headers = None
        if not {"ado", "upack", "pip", "internal"} <= self.skip:
            self._ado_headers = {"Authorization": f"Bearer {_get_ado_token(token)}"}
        self._gh_headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "synapseml-release-verify",
        }
        if gh_token:
            self._gh_headers["Authorization"] = f"Bearer {gh_token}"
        self._pkg_cache: Dict[Tuple[str, str], Dict[str, List[str]]] = {}

    # --- git tags ---------------------------------------------------------
    def github_tag(self, tag: str) -> str:
        if "github" in self.skip:
            return SKIPPED
        url = f"https://api.github.com/repos/{GITHUB_REPO}/git/ref/tags/{tag}"
        return OK if _json_get(url, self._gh_headers) else MISSING

    def ado_tag(self, tag: str) -> str:
        if "ado" in self.skip or "internal" in self.skip:
            return SKIPPED
        url = (
            f"https://dev.azure.com/{ORG_SHORT}/{ADO_PROJECT}/_apis/git/repositories/"
            f"{INTERNAL_REPO}/refs?filter=tags/{tag}&api-version=7.1"
        )
        data = _json_get(url, self._ado_headers)
        if not data:
            return MISSING
        wanted = f"refs/tags/{tag}"
        return (
            OK
            if any(v.get("name") == wanted for v in data.get("value", []))
            else MISSING
        )

    # --- artifact feeds ---------------------------------------------------
    def _feed_versions(self, feed: str, protocol: str, package: str) -> List[str]:
        key = (feed, protocol)
        if key not in self._pkg_cache:
            url = (
                f"https://feeds.dev.azure.com/{ORG_SHORT}/{ADO_PROJECT}/_apis/packaging/Feeds/"
                f"{feed}/packages?protocolType={protocol}&includeAllVersions=true&api-version=7.1-preview.1"
            )
            data = _json_get(url, self._ado_headers) or {}
            self._pkg_cache[key] = {
                p["name"].lower(): [v["version"] for v in p.get("versions", [])]
                for p in data.get("value", [])
            }
        return self._pkg_cache[key].get(package.lower(), [])

    def upack(self, package: str, version: str) -> str:
        if "upack" in self.skip or "ado" in self.skip:
            return SKIPPED
        return (
            OK
            if version
            in self._feed_versions("BBC-VHD_PublicPackages", "upack", package)
            else MISSING
        )

    def pip(self, package: str, version: str) -> str:
        if "pip" in self.skip or "ado" in self.skip:
            return SKIPPED
        # Azure Artifacts normalises pypi names: synapseml_internal -> synapseml-internal
        return (
            OK
            if version
            in self._feed_versions("Synapse-Conda", "pypi", package.replace("_", "-"))
            else MISSING
        )


def run(
    version: str, internal_patch: str, target_keys, token, gh_token, skip
) -> Tuple[List[dict], bool]:
    plan = build_plan(version, internal_patch, target_keys)
    c = Checker(token, gh_token, skip)
    rows: List[dict] = []

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

    for tp in plan.targets:
        for tag in tp.oss_tags:
            add("git-tag", tp.key, "github/" + GITHUB_REPO, tag, c.github_tag(tag))
        for tag in tp.internal_tags:
            add("git-tag", tp.key, "ado/" + INTERNAL_REPO, tag, c.ado_tag(tag))
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
            c.upack("synapseml_internal", tp.internal_upack_version),
        )
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
            c.pip("synapseml_internal", tp.internal_pip_version),
        )

    ok = not any(r["status"] == MISSING for r in rows)
    return rows, ok


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Verify a SynapseML release's artifacts and tags."
    )
    p.add_argument("--version", required=True)
    p.add_argument("--internal-patch", default="0")
    p.add_argument("--targets", default="")
    p.add_argument(
        "--skip", default="", help="Comma-separated: github,ado,upack,pip,internal"
    )
    p.add_argument(
        "--token",
        default=None,
        help="ADO bearer token (default: az account get-access-token)",
    )
    p.add_argument("--github-token", default=None)
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)

    keys = [k.strip() for k in args.targets.split(",") if k.strip()] or None
    skip = [s.strip() for s in args.skip.split(",") if s.strip()]

    try:
        rows, ok = run(
            args.version, args.internal_patch, keys, args.token, args.github_token, skip
        )
    except (ValueError, RuntimeError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    if args.json:
        print(
            json.dumps(
                {"version": args.version, "complete": ok, "rows": rows}, indent=2
            )
        )
    else:
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
