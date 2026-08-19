# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Run checked-in SynapseML scenarios on a managed Microsoft Fabric Spark runtime."""

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence
from xml.etree import ElementTree

RESULT_MARKER = "SYNAPSEML_FABRIC_E2E_RESULT="
DIAGNOSTIC_MARKER = "SYNAPSEML_FABRIC_E2E_DIAGNOSTIC="
MANIFEST_PATH = Path(__file__).with_name("scenarios.json")
REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Scenario:
    """A checked-in Fabric E2E scenario."""

    name: str
    description: str
    script: Path
    minimum_jars: int
    default_args: Sequence[str]
    execution: str = "batch"
    default_spark_conf: Sequence[str] = ()


def load_scenarios(manifest_path: Path = MANIFEST_PATH) -> Dict[str, Scenario]:
    """Load and validate the checked-in scenario manifest."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    scenario_root = manifest_path.parent.resolve()
    scenarios: Dict[str, Scenario] = {}
    for name, raw in manifest.items():
        script = (manifest_path.parent / raw["script"]).resolve()
        if scenario_root not in script.parents or not script.is_file():
            raise ValueError(f"Scenario {name!r} has an invalid script path: {script}")
        minimum_jars = int(raw.get("minimumJars", 0))
        if minimum_jars < 0:
            raise ValueError(f"Scenario {name!r} has a negative minimumJars")
        execution = str(raw.get("execution", "batch"))
        if execution not in {"batch", "notebook"}:
            raise ValueError(
                f"Scenario {name!r} has an invalid execution mode: {execution}"
            )
        scenarios[name] = Scenario(
            name=name,
            description=str(raw["description"]),
            script=script,
            minimum_jars=minimum_jars,
            default_args=tuple(str(value) for value in raw.get("defaultArgs", [])),
            execution=execution,
            default_spark_conf=tuple(
                str(value) for value in raw.get("defaultSparkConf", [])
            ),
        )
    if not scenarios:
        raise ValueError("Fabric E2E scenario manifest is empty")
    return scenarios


def sha256_file(path: Path) -> str:
    """Return a file's SHA-256 digest."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sanitize_name(value: str) -> str:
    """Return a Fabric-safe lowercase identifier."""
    return re.sub(r"[^a-z0-9_]", "_", value.lower()).strip("_")


def new_run_id(scenario_name: str, now: Optional[datetime] = None) -> str:
    """Create a collision-resistant run identifier."""
    timestamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%d%H%M%S")
    scenario = sanitize_name(scenario_name)[:20]
    return f"{scenario}_{timestamp}_{uuid.uuid4().hex[:8]}"


def resolve_scenario_args(
    scenario: Scenario, jars: Sequence[Path], additional_args: Sequence[str]
) -> List[str]:
    """Expand manifest placeholders and append caller-provided scenario arguments."""
    values = {
        "firstJarName": jars[0].name if jars else "",
        "lastJarName": jars[-1].name if jars else "",
    }
    values.update({f"jar{index}Name": jar.name for index, jar in enumerate(jars)})
    defaults = [value.format(**values) for value in scenario.default_args]
    return defaults + list(additional_args)


def resolve_spark_conf(scenario: Scenario, additional_conf: Sequence[str]) -> List[str]:
    """Combine scenario safety defaults with caller-provided Spark settings."""
    return list(scenario.default_spark_conf) + list(additional_conf)


def write_scenario_notebook(
    path: Path, scenario: Scenario, scenario_args: Sequence[str]
) -> None:
    """Wrap a Python scenario in a Fabric platform notebook."""
    source = scenario.script.read_text(encoding="utf-8")
    argv = [scenario.script.name, *scenario_args]
    cell_source = f"import sys\nsys.argv = {json.dumps(argv)}\n{source}"
    notebook = {
        "cells": [
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": cell_source.splitlines(keepends=True),
            }
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Synapse PySpark",
                "language": "python",
                "name": "synapse_pyspark",
            },
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    path.write_text(json.dumps(notebook, indent=2) + "\n", encoding="utf-8")


def build_submission_command(
    executable: str,
    scenario: Scenario,
    workspace: str,
    lakehouse: str,
    job_name: str,
    output_dir: Path,
    environment: str,
    jars: Sequence[Path],
    scenario_args: Sequence[str],
    subscription: Optional[str] = None,
    runtime: Optional[str] = None,
    node_size: Optional[str] = None,
    node_count: Optional[int] = None,
    spark_conf: Sequence[str] = (),
) -> List[str]:
    """Build a non-interactive fabric-spark-cli batch command."""
    command = [
        executable,
        "batch",
        "submit",
        "--backend",
        "fabric",
        "--py",
        str(scenario.script),
        "--name",
        job_name,
        "--env",
        environment,
        "--workspace",
        workspace,
        "--lakehouse",
        lakehouse,
        "--no-overwrite",
        "--download-log",
        "-o",
        str(output_dir),
    ]
    if subscription:
        command.extend(("--subscription", subscription))
    if runtime:
        command.extend(("--runtime", runtime))
    if node_size:
        command.extend(("--node-size", node_size))
    if node_count is not None:
        command.extend(("--node-count", str(node_count)))
    for entry in spark_conf:
        command.extend(("--conf", entry))
    if scenario_args:
        command.extend(("--args", shlex.join(scenario_args)))
    if jars:
        command.extend(("--extra-jars", *(str(path) for path in jars)))
    return command


def build_notebook_submission_command(
    executable: str,
    notebook_path: Path,
    workspace: str,
    lakehouse: str,
    notebook_name: str,
    output_path: Path,
    environment: str,
    jars: Sequence[Path],
    subscription: Optional[str] = None,
    spark_conf: Sequence[str] = (),
) -> List[str]:
    """Build a non-interactive Fabric platform-notebook command."""
    command = [
        executable,
        "notebook",
        "run",
        str(notebook_path),
        "--name",
        notebook_name,
        "--stdout",
        "--output-file",
        str(output_path),
        "--env",
        environment,
        "--workspace",
        workspace,
        "--lakehouse",
        lakehouse,
    ]
    if subscription:
        command.extend(("--subscription", subscription))
    for entry in spark_conf:
        command.extend(("--conf", entry))
    if jars:
        command.extend(("--extra-jars", *(str(path) for path in jars)))
    return command


def build_cleanup_command(
    executable: str,
    workspace: str,
    lakehouse: str,
    environment: str,
    subscription: Optional[str] = None,
) -> List[str]:
    """Build an exact-name scratch lakehouse cleanup command."""
    command = [
        executable,
        "lakehouse",
        "delete",
        "--env",
        environment,
        "--workspace",
        workspace,
    ]
    if subscription:
        command.extend(("--subscription", subscription))
    command.append(lakehouse)
    return command


def build_notebook_cleanup_command(
    executable: str,
    workspace: str,
    notebook_name: str,
    environment: str,
    subscription: Optional[str] = None,
) -> List[str]:
    """Build an exact-name Fabric notebook cleanup command."""
    command = [
        executable,
        "notebook",
        "delete",
        "--env",
        environment,
        "--workspace",
        workspace,
    ]
    if subscription:
        command.extend(("--subscription", subscription))
    command.extend(("--name", notebook_name))
    return command


def console_safe_text(value: str, encoding: Optional[str]) -> str:
    """Replace characters unsupported by the active console encoding."""
    resolved_encoding = encoding or "utf-8"
    return value.encode(resolved_encoding, errors="replace").decode(resolved_encoding)


def run_and_tee(command: Sequence[str], log_path: Path) -> tuple[int, str]:
    """Run a command while streaming and retaining its combined output."""
    environment = os.environ.copy()
    environment["PYTHONUTF8"] = "1"
    environment["PYTHONIOENCODING"] = "utf-8"
    output: List[str] = []
    with log_path.open("a", encoding="utf-8") as log:
        process = subprocess.Popen(
            list(command),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=environment,
        )
        try:
            assert process.stdout is not None
            for line in process.stdout:
                sys.stdout.write(console_safe_text(line, sys.stdout.encoding))
                sys.stdout.flush()
                log.write(line)
                output.append(line)
            return process.wait(), "".join(output)
        except BaseException:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            raise


def downloaded_marker_output(log_root: Path) -> str:
    """Read structured scenario markers from downloaded driver stdout logs."""
    marker_lines: List[str] = []
    if not log_root.is_dir():
        return ""
    for path in sorted(log_root.rglob("stdout.log")):
        with path.open(encoding="utf-8", errors="replace") as stream:
            marker_lines.extend(
                line
                for line in stream
                if RESULT_MARKER in line or DIAGNOSTIC_MARKER in line
            )
    return "".join(marker_lines)


def marker_payloads(output: str, marker: str) -> List[str]:
    """Return unique marker payloads in emission order."""
    matches: List[str] = []
    for line in output.splitlines():
        if marker in line:
            payload = line.split(marker, 1)[1].strip()
            if payload[:1] in {"{", "["} and payload not in matches:
                matches.append(payload)
    return matches


def parse_runtime_evidence(output: str) -> Mapping[str, object]:
    """Parse the scenario's single structured result marker."""
    matches = marker_payloads(output, RESULT_MARKER)
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one {RESULT_MARKER} marker, found {len(matches)}"
        )
    evidence = json.loads(matches[0])
    if not isinstance(evidence, dict):
        raise ValueError("Fabric scenario evidence must be a JSON object")
    return evidence


def parse_runtime_diagnostics(output: str) -> List[Mapping[str, object]]:
    """Parse zero or more structured diagnostics emitted before a scenario completes."""
    matches = marker_payloads(output, DIAGNOSTIC_MARKER)
    diagnostics: List[Mapping[str, object]] = []
    for match in matches:
        diagnostic = json.loads(match)
        if not isinstance(diagnostic, dict):
            raise ValueError("Fabric scenario diagnostics must be JSON objects")
        diagnostics.append(diagnostic)
    return diagnostics


def diagnostic_failure_message(
    diagnostics: Sequence[Mapping[str, object]],
) -> Optional[str]:
    """Return the last structured scenario error, when one was emitted."""
    for diagnostic in reversed(diagnostics):
        message = diagnostic.get("errorMessage")
        if isinstance(message, str) and message:
            return message
    return None


def command_version(executable: str) -> str:
    """Return the installed fabric-spark-cli version without failing the run."""
    process = subprocess.run(
        [executable, "--version"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return (process.stdout or process.stderr).strip()


def git_commit(repo_root: Path) -> str:
    """Return the source commit used by this run."""
    process = subprocess.run(
        ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return process.stdout.strip() if process.returncode == 0 else "unknown"


def write_junit(
    path: Path,
    scenario: str,
    elapsed_seconds: float,
    failure: Optional[str],
    evidence_path: Path,
) -> None:
    """Write one JUnit test case for Azure Pipelines and local tooling."""
    suite = ElementTree.Element(
        "testsuite",
        {
            "name": "SynapseML Fabric E2E",
            "tests": "1",
            "failures": "1" if failure else "0",
            "time": f"{elapsed_seconds:.3f}",
        },
    )
    case = ElementTree.SubElement(
        suite,
        "testcase",
        {
            "classname": "fabric_e2e",
            "name": scenario,
            "time": f"{elapsed_seconds:.3f}",
        },
    )
    if failure:
        ElementTree.SubElement(
            case, "failure", {"message": failure[:500]}
        ).text = failure
    ElementTree.SubElement(case, "system-out").text = f"Evidence: {evidence_path}"
    ElementTree.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)


def checked_jars(raw_paths: Iterable[str]) -> List[Path]:
    """Resolve jar arguments and reject missing or non-jar paths."""
    jars = [Path(raw).expanduser().resolve() for raw in raw_paths]
    for jar in jars:
        if not jar.is_file() or jar.suffix.lower() != ".jar":
            raise ValueError(f"Extra jar does not exist or is not a .jar file: {jar}")
    return jars


def create_parser(scenarios: Mapping[str, Scenario]) -> argparse.ArgumentParser:
    """Create the command-line parser."""
    parser = argparse.ArgumentParser(
        description="Run a checked-in SynapseML scenario on managed Fabric Spark."
    )
    parser.add_argument("--list-scenarios", action="store_true")
    parser.add_argument("--scenario", choices=sorted(scenarios))
    parser.add_argument(
        "--workspace",
        help="Fabric workspace name. Required for unattended execution.",
    )
    parser.add_argument("--env", default="msit", dest="environment")
    parser.add_argument("--subscription")
    parser.add_argument("--runtime")
    parser.add_argument("--node-size")
    parser.add_argument("--node-count", type=int)
    parser.add_argument("--conf", action="append", default=[])
    parser.add_argument("--extra-jar", action="append", default=[])
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--source-repo",
        type=Path,
        default=REPO_ROOT,
        help="Checkout whose commit produced the supplied jars.",
    )
    parser.add_argument("--keep-lakehouse", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--scenario-args",
        nargs=argparse.REMAINDER,
        default=[],
        help="Arguments passed to the scenario; this option must be last.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run a scenario and persist its evidence."""
    scenarios = load_scenarios()
    parser = create_parser(scenarios)
    args = parser.parse_args(argv)

    if args.list_scenarios:
        for name in sorted(scenarios):
            print(f"{name}: {scenarios[name].description}")
        return 0
    if not args.scenario:
        parser.error("--scenario is required unless --list-scenarios is used")
    if not args.workspace:
        parser.error("--workspace is required for unattended execution")
    if (args.node_size is None) != (args.node_count is None):
        parser.error("--node-size and --node-count must be supplied together")
    if args.node_count is not None and args.node_count < 1:
        parser.error("--node-count must be positive")

    executable = shutil.which("fabric-spark-cli")
    if not executable:
        parser.error("fabric-spark-cli is not installed or is not on PATH")

    scenario = scenarios[args.scenario]
    if scenario.execution == "notebook" and (
        args.runtime or args.node_size or args.node_count is not None
    ):
        parser.error(
            f"Scenario {scenario.name!r} uses Fabric platform-notebook execution, "
            "which uses the workspace's configured runtime and pool"
        )
    try:
        jars = checked_jars(args.extra_jar)
    except ValueError as error:
        parser.error(str(error))
    if len(jars) < scenario.minimum_jars:
        parser.error(
            f"Scenario {scenario.name!r} requires at least "
            f"{scenario.minimum_jars} --extra-jar value(s)"
        )

    run_id = new_run_id(scenario.name)
    lakehouse = f"synapseml_e2e_{run_id}"
    job_name = (
        f"synapseml-e2e-{sanitize_name(scenario.name).replace('_', '-')}"
        f"-{run_id.rsplit('_', 1)[-1]}"
    )
    output_dir = args.output_dir or REPO_ROOT / "target" / "fabric-e2e" / run_id
    output_dir = output_dir.resolve()
    source_repo = args.source_repo.expanduser().resolve()
    if not (source_repo / ".git").exists():
        parser.error(f"--source-repo is not a Git checkout: {source_repo}")
    log_path = output_dir / "runner.log"
    evidence_path = output_dir / "evidence.json"
    junit_path = output_dir / "junit.xml"
    notebook_path = output_dir / "scenario.ipynb"
    scenario_args = resolve_scenario_args(scenario, jars, args.scenario_args)
    spark_conf = resolve_spark_conf(scenario, args.conf)
    if scenario.execution == "notebook":
        command = build_notebook_submission_command(
            executable=executable,
            notebook_path=notebook_path,
            workspace=args.workspace,
            lakehouse=lakehouse,
            notebook_name=job_name,
            output_path=output_dir / "executed-notebook.ipynb",
            environment=args.environment,
            subscription=args.subscription,
            spark_conf=spark_conf,
            jars=jars,
        )
    else:
        command = build_submission_command(
            executable=executable,
            scenario=scenario,
            workspace=args.workspace,
            lakehouse=lakehouse,
            job_name=job_name,
            output_dir=output_dir / "fabric-logs",
            environment=args.environment,
            subscription=args.subscription,
            runtime=args.runtime,
            node_size=args.node_size,
            node_count=args.node_count,
            spark_conf=spark_conf,
            jars=jars,
            scenario_args=scenario_args,
        )
    cleanup_command = build_cleanup_command(
        executable=executable,
        workspace=args.workspace,
        lakehouse=lakehouse,
        environment=args.environment,
        subscription=args.subscription,
    )
    notebook_cleanup_command = (
        build_notebook_cleanup_command(
            executable=executable,
            workspace=args.workspace,
            notebook_name=job_name,
            environment=args.environment,
            subscription=args.subscription,
        )
        if scenario.execution == "notebook"
        else None
    )

    if args.dry_run:
        print(
            json.dumps(
                {
                    "cleanupCommand": cleanup_command,
                    "lakehouse": lakehouse,
                    "notebookCleanupCommand": notebook_cleanup_command,
                    "submissionCommand": command,
                },
                indent=2,
            )
        )
        return 0

    output_dir.mkdir(parents=True, exist_ok=False)
    if scenario.execution == "notebook":
        write_scenario_notebook(notebook_path, scenario, scenario_args)
    started = datetime.now(timezone.utc)
    submission_code = 1
    cleanup_code: Optional[int] = None
    notebook_cleanup_code: Optional[int] = None
    output = ""
    runtime_evidence: Mapping[str, object] = {}
    runtime_diagnostics: List[Mapping[str, object]] = []
    failure: Optional[str] = None
    try:
        submission_code, output = run_and_tee(command, log_path)
        marker_output = output + downloaded_marker_output(output_dir / "fabric-logs")
        try:
            runtime_diagnostics = parse_runtime_diagnostics(marker_output)
        except (ValueError, json.JSONDecodeError) as error:
            failure = f"Invalid Fabric E2E diagnostics: {error}"
        if submission_code != 0:
            if failure is None:
                detail = diagnostic_failure_message(runtime_diagnostics)
                failure = f"fabric-spark-cli exited with code {submission_code}"
                if detail:
                    failure += f": {detail}"
        elif failure is None:
            try:
                runtime_evidence = parse_runtime_evidence(marker_output)
            except (ValueError, json.JSONDecodeError) as error:
                failure = str(error)
    finally:
        if notebook_cleanup_command:
            notebook_cleanup_code, notebook_cleanup_output = run_and_tee(
                notebook_cleanup_command, log_path
            )
            output += notebook_cleanup_output
            if notebook_cleanup_code != 0 and failure is None:
                failure = (
                    "Scratch notebook cleanup exited with code "
                    f"{notebook_cleanup_code}"
                )
        if args.keep_lakehouse:
            print(f"Keeping scratch lakehouse for debugging: {lakehouse}")
        else:
            cleanup_code, cleanup_output = run_and_tee(cleanup_command, log_path)
            output += cleanup_output
            if cleanup_code != 0 and failure is None:
                failure = f"Scratch lakehouse cleanup exited with code {cleanup_code}"

    finished = datetime.now(timezone.utc)
    evidence = {
        "cleanupExitCode": cleanup_code,
        "commit": git_commit(source_repo),
        "execution": scenario.execution,
        "fabricSparkCliVersion": command_version(executable),
        "finishedAt": finished.isoformat(),
        "failure": failure,
        "jars": [
            {"name": jar.name, "path": str(jar), "sha256": sha256_file(jar)}
            for jar in jars
        ],
        "lakehouse": lakehouse,
        "notebookCleanupExitCode": notebook_cleanup_code,
        "runtimeDiagnostics": runtime_diagnostics,
        "runtimeEvidence": runtime_evidence,
        "scenario": scenario.name,
        "sparkConf": spark_conf,
        "startedAt": started.isoformat(),
        "status": "failed" if failure else "passed",
        "submissionExitCode": submission_code,
        "workspace": args.workspace,
    }
    evidence_path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    write_junit(
        junit_path,
        scenario.name,
        (finished - started).total_seconds(),
        failure,
        evidence_path,
    )
    print(f"SYNAPSEML_FABRIC_E2E_EVIDENCE={evidence_path}")
    if failure:
        print(f"Fabric E2E failed: {failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
