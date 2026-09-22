# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

"""Watch one Azure build's GitHub check without repeated agent invocations."""

import argparse
from datetime import datetime, timedelta, timezone
import json
import re
import subprocess
import time
from urllib.parse import parse_qs, urlparse

POLL_SECONDS = 600
MAX_TIMEOUT_MINUTES = 120
MAX_BUILD_ID = 2_147_483_647
CHECK_NAME = "microsoft.SynapseML"
REPOSITORY = "microsoft/SynapseML"
AZURE_PROJECTS = ("b9b2accc-2d1c-45b3-9d24-0eb5d78cc47f", "a365")
AZURE_BUILD_PATHS = {
    "dev.azure.com": {
        f"/msdata/{project}/_build/results" for project in AZURE_PROJECTS
    },
    "msdata.visualstudio.com": {
        f"/{project}/_build/results" for project in AZURE_PROJECTS
    },
}


class MonitorError(Exception):
    """The requested build could not be monitored reliably."""


def parse_build_id(url):
    """Reject checks outside the trusted SynapseML Azure project."""
    if not isinstance(url, str):
        raise MonitorError("Azure check has an invalid build URL.")
    try:
        parsed = urlparse(url)
        trusted_paths = AZURE_BUILD_PATHS.get(parsed.hostname, ())
        trusted = (
            parsed.scheme == "https"
            and parsed.port in (None, 443)
            and parsed.username is None
            and parsed.password is None
            and parsed.path.lower() in trusted_paths
            and not parsed.fragment
        )
    except ValueError as error:
        raise MonitorError("Azure check has an invalid build URL.") from error
    if not trusted:
        raise MonitorError("Azure check URL is outside the trusted SynapseML project.")
    build_ids = parse_qs(parsed.query).get("buildId", [])
    if len(build_ids) != 1 or not re.fullmatch(r"[0-9]+", build_ids[0]):
        raise MonitorError("Azure check has no valid build ID.")
    number = build_ids[0].lstrip("0") or "0"
    if len(number) > len(str(MAX_BUILD_ID)):
        raise MonitorError("Azure check build ID exceeds the supported int32 range.")
    build_id = int(number)
    if not 1 <= build_id <= MAX_BUILD_ID:
        raise MonitorError("Azure check build ID is outside the supported int32 range.")
    return build_id


def query_pr(args, timeout):
    command = [
        "gh",
        "pr",
        "view",
        str(args.pull_request),
        "--repo",
        REPOSITORY,
        "--json",
        "state,headRefOid,statusCheckRollup",
    ]
    try:
        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise MonitorError("GitHub status query timed out.") from error
    except OSError as error:
        raise MonitorError(f"Could not run GitHub CLI: {error}") from error
    except UnicodeError as error:
        raise MonitorError("GitHub CLI output is not valid UTF-8.") from error
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
    remaining_budget = (
        args.kickoff_at.timestamp() + args.timeout_minutes * 60 - time.time()
    )
    deadline = time.monotonic() + max(0, remaining_budget)
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
        matching = {}
        for check in checks:
            if (check.get("name") or check.get("context")) != CHECK_NAME:
                continue
            url = check.get("detailsUrl") or check.get("targetUrl") or ""
            build_id = parse_build_id(url)
            if build_id in matching:
                raise MonitorError("Azure check has duplicate results for one build.")
            matching[build_id] = (check, url)
        if not matching or max(matching) < args.build_id:
            raise MonitorError(
                "Expected Azure build is not registered. Verify its build ID."
            )
        latest_build_id = max(matching)
        if latest_build_id != args.build_id:
            return {
                "outcome": "replaced",
                "replacementBuildId": latest_build_id,
                "replacementUrl": matching[latest_build_id][1],
                "message": "Start a monitor using the new run's verified kickoff time.",
            }
        check, url = matching[args.build_id]
        timeout_result["url"] = url
        state = check.get("status") or check.get("state")
        if state == "COMPLETED":
            conclusion = check.get("conclusion")
            if not isinstance(conclusion, str) or not conclusion:
                raise MonitorError("Completed Azure check has no conclusion.")
        elif state in ("SUCCESS", "FAILURE", "ERROR"):
            conclusion = state
        elif state in (
            "QUEUED",
            "IN_PROGRESS",
            "WAITING",
            "PENDING",
            "REQUESTED",
            "EXPECTED",
        ):
            time.sleep(min(POLL_SECONDS, max(0, deadline - time.monotonic())))
            continue
        else:
            raise MonitorError(f"Unknown Azure check state: {state!r}")
        return {
            "outcome": "success" if conclusion == "SUCCESS" else "failed",
            "conclusion": conclusion,
            "url": url,
        }


def parse_kickoff(value):
    try:
        kickoff = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if kickoff.tzinfo is None:
            raise argparse.ArgumentTypeError("Kickoff must include its time zone.")
        return kickoff.astimezone(timezone.utc)
    except (ValueError, OverflowError) as error:
        raise argparse.ArgumentTypeError(
            "Kickoff must be an ISO 8601 timestamp within the supported UTC date range."
        ) from error


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=REPOSITORY)
    parser.add_argument("--pull-request", required=True, type=int)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--build-id", required=True, type=int)
    parser.add_argument("--kickoff-at", required=True, type=parse_kickoff)
    parser.add_argument("--timeout-minutes", type=int, default=MAX_TIMEOUT_MINUTES)
    args = parser.parse_args(argv)
    if args.pull_request <= 0 or args.build_id <= 0:
        parser.error("PR and build IDs must be positive.")
    if args.build_id > MAX_BUILD_ID:
        parser.error(f"--build-id must be at most {MAX_BUILD_ID}.")
    if args.repo.lower() != REPOSITORY.lower():
        parser.error(
            f"--repo must be {REPOSITORY}; other repositories are unsupported."
        )
    args.repo = REPOSITORY
    if not re.fullmatch(r"[0-9a-fA-F]{40}", args.head_sha):
        parser.error("--head-sha must be a full 40-character commit SHA.")
    args.head_sha = args.head_sha.lower()
    if not 1 <= args.timeout_minutes <= MAX_TIMEOUT_MINUTES:
        parser.error("--timeout-minutes must be between 1 and 120.")
    if args.kickoff_at.timestamp() > time.time():
        parser.error("--kickoff-at cannot be in the future.")
    return args


def main(argv=None):
    args = parse_args(argv)
    context = {
        "repo": args.repo,
        "pullRequest": args.pull_request,
        "headSha": args.head_sha,
        "buildId": args.build_id,
        "kickoffAt": args.kickoff_at.isoformat(),
        "deadlineAt": (
            args.kickoff_at + timedelta(minutes=args.timeout_minutes)
        ).isoformat(),
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
        "replaced": 3,
        "timeout": 124,
        "interrupted": 130,
    }[result["outcome"]]


if __name__ == "__main__":
    raise SystemExit(main())
