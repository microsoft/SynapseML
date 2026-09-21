# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

"""Watch one Azure build's GitHub check without repeated agent invocations."""

import argparse
import json
import re
import subprocess
import time
from urllib.parse import parse_qs, urlparse

POLL_SECONDS = 600
MAX_TIMEOUT_MINUTES = 120
CHECK_NAME = "microsoft.SynapseML"


class MonitorError(Exception):
    """The requested build could not be monitored reliably."""


def query_pr(args, timeout):
    command = [
        "gh",
        "pr",
        "view",
        str(args.pull_request),
        "--repo",
        args.repo,
        "--json",
        "state,headRefOid,statusCheckRollup",
    ]
    try:
        process = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout, check=False
        )
    except subprocess.TimeoutExpired as error:
        raise MonitorError("GitHub status query timed out.") from error
    except OSError as error:
        raise MonitorError(f"Could not run GitHub CLI: {error}") from error
    if process.returncode:
        raise MonitorError(f"GitHub CLI failed: {process.stderr.strip()[:1000]}")
    try:
        snapshot = json.loads(process.stdout)
    except json.JSONDecodeError as error:
        raise MonitorError("GitHub CLI returned invalid JSON.") from error
    if not isinstance(snapshot, dict):
        raise MonitorError("GitHub CLI did not return a PR object.")
    return snapshot


def monitor(args):
    deadline = time.monotonic() + args.timeout_minutes * 60
    timeout_result = {"outcome": "timeout"}
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return timeout_result
        try:
            snapshot = query_pr(args, timeout=min(60, remaining))
        except MonitorError:
            if time.monotonic() >= deadline:
                return timeout_result
            raise
        if time.monotonic() >= deadline:
            return timeout_result
        if not snapshot.get("headRefOid") or snapshot.get("state") not in (
            "OPEN",
            "CLOSED",
            "MERGED",
        ):
            raise MonitorError("GitHub response is missing valid PR state or head.")
        if snapshot["headRefOid"] != args.head_sha or snapshot["state"] != "OPEN":
            return {
                "outcome": "superseded",
                "message": "PR head or open state changed; recheck before monitoring.",
            }
        checks = snapshot.get("statusCheckRollup")
        if not isinstance(checks, list) or not all(
            isinstance(check, dict) for check in checks
        ):
            raise MonitorError("GitHub response is missing valid check results.")
        matching = []
        for check in checks:
            if (check.get("name") or check.get("context")) != CHECK_NAME:
                continue
            url = check.get("detailsUrl") or check.get("targetUrl") or ""
            if not isinstance(url, str):
                raise MonitorError("Azure check has an invalid build URL.")
            if parse_qs(urlparse(url).query).get("buildId") == [str(args.build_id)]:
                matching.append((check, url))
        if len(matching) != 1:
            raise MonitorError(
                "Expected one matching Azure build check. Verify its registration "
                "and build ID; do not silently follow a replacement run."
            )
        check, url = matching[0]
        timeout_result["url"] = url
        state = check.get("status") or check.get("state")
        if state == "COMPLETED":
            conclusion = check.get("conclusion")
            if not isinstance(conclusion, str) or not conclusion:
                raise MonitorError("Completed Azure check has no conclusion.")
        elif state in ("SUCCESS", "FAILURE", "ERROR"):
            conclusion = state
        elif state in ("QUEUED", "IN_PROGRESS", "WAITING", "PENDING", "REQUESTED"):
            time.sleep(min(POLL_SECONDS, max(0, deadline - time.monotonic())))
            continue
        else:
            raise MonitorError(f"Unknown Azure check state: {state!r}")
        return {
            "outcome": "success" if conclusion == "SUCCESS" else "failed",
            "conclusion": conclusion,
            "url": url,
        }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default="microsoft/SynapseML")
    parser.add_argument("--pull-request", required=True, type=int)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--build-id", required=True, type=int)
    parser.add_argument("--timeout-minutes", type=int, default=MAX_TIMEOUT_MINUTES)
    args = parser.parse_args(argv)
    if args.pull_request <= 0 or args.build_id <= 0:
        parser.error("PR and build IDs must be positive.")
    if not re.fullmatch(r"[^/\s]+/[^/\s]+", args.repo):
        parser.error("--repo must be owner/repository.")
    if not re.fullmatch(r"[0-9a-fA-F]{40}", args.head_sha):
        parser.error("--head-sha must be a full 40-character commit SHA.")
    args.head_sha = args.head_sha.lower()
    if not 1 <= args.timeout_minutes <= MAX_TIMEOUT_MINUTES:
        parser.error("--timeout-minutes must be between 1 and 120.")
    return args


def main(argv=None):
    args = parse_args(argv)
    context = {
        "repo": args.repo,
        "pullRequest": args.pull_request,
        "headSha": args.head_sha,
        "buildId": args.build_id,
    }
    print(
        json.dumps(
            {
                **context,
                "event": "started",
                "pollSeconds": POLL_SECONDS,
                "timeoutMinutes": args.timeout_minutes,
            }
        ),
        flush=True,
    )
    try:
        result = monitor(args)
    except MonitorError as error:
        result = {"outcome": "error", "message": str(error)}
    except KeyboardInterrupt:
        result = {"outcome": "interrupted"}
    print(json.dumps({**context, "event": "finished", **result}), flush=True)
    return {
        "success": 0,
        "failed": 1,
        "error": 1,
        "superseded": 2,
        "timeout": 124,
        "interrupted": 130,
    }[result["outcome"]]


if __name__ == "__main__":
    raise SystemExit(main())
