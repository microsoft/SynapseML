# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""Run the OpenAIPrompt end-to-end test on Microsoft Fabric Spark."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Mapping, Optional, Sequence
from xml.etree import ElementTree

SCENARIO_NAME = "openai-prompt"
SCENARIO_PATH = (Path(__file__).with_name("scenarios") / "openai_prompt.py").resolve()
RESULT_MARKER = "SYNAPSEML_FABRIC_E2E_RESULT="
DIAGNOSTIC_MARKER = "SYNAPSEML_FABRIC_E2E_DIAGNOSTIC="
NOTEBOOK_MARKER_PATH = "Files/synapseml-fabric-e2e/markers.jsonl"
REPO_ROOT = Path(__file__).resolve().parents[2]


def sha256_file(path: Path) -> str:
    """Return a file's SHA-256 digest."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def new_run_id(now: Optional[datetime] = None) -> str:
    """Create a collision-resistant run identifier."""
    timestamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%d%H%M%S")
    return f"{timestamp}_{uuid.uuid4().hex[:8]}"


def write_scenario_notebook(
    path: Path,
    scenario_path: Path,
    scenario_args: Sequence[str],
    marker_path: str = NOTEBOOK_MARKER_PATH,
) -> None:
    """Wrap the Python scenario in a Fabric platform notebook."""
    source = scenario_path.read_text(encoding="utf-8")
    argv = [scenario_path.name, *scenario_args]
    cell_source = "\n".join(
        [
            "import builtins",
            "import json",
            "import sys",
            "",
            "from notebookutils import mssparkutils",
            "",
            f"sys.argv = {json.dumps(argv)}",
            f"_synapseml_scenario_name = {json.dumps(scenario_path.name)}",
            f"_synapseml_scenario_source = {json.dumps(source)}",
            f"_synapseml_result_marker = {json.dumps(RESULT_MARKER)}",
            f"_synapseml_diagnostic_marker = {json.dumps(DIAGNOSTIC_MARKER)}",
            f"_synapseml_marker_path = {json.dumps(marker_path)}",
            "_synapseml_markers = []",
            "_synapseml_original_print = builtins.print",
            "",
            "def _synapseml_capture_print(*values, **kwargs):",
            '    separator = kwargs.get("sep", " ")',
            '    separator = " " if separator is None else separator',
            "    rendered = separator.join(str(value) for value in values)",
            "    if (",
            "        _synapseml_result_marker in rendered",
            "        or _synapseml_diagnostic_marker in rendered",
            "    ):",
            "        _synapseml_markers.append(rendered)",
            "    _synapseml_original_print(*values, **kwargs)",
            "",
            "builtins.print = _synapseml_capture_print",
            "try:",
            "    scenario_globals = dict(",
            "        globals(),",
            '        __name__="__main__",',
            "        __file__=_synapseml_scenario_name,",
            "    )",
            "    exec(",
            "        compile(",
            "            _synapseml_scenario_source,",
            "            _synapseml_scenario_name,",
            '            "exec",',
            "        ),",
            "        scenario_globals,",
            "    )",
            "except BaseException as error:",
            "    _synapseml_markers.append(",
            "        _synapseml_diagnostic_marker",
            "        + json.dumps(",
            "            {",
            '                "errorType": type(error).__name__,',
            '                "phase": "scenario-exception",',
            "            },",
            "            sort_keys=True,",
            "        )",
            "    )",
            "    raise",
            "finally:",
            "    builtins.print = _synapseml_original_print",
            '    marker_payload = "\\n".join(_synapseml_markers)',
            "    if marker_payload:",
            '        marker_payload += "\\n"',
            "    mssparkutils.fs.put(",
            "        _synapseml_marker_path,",
            "        marker_payload,",
            "        True,",
            "    )",
            "",
        ]
    )
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


def build_notebook_submission_command(
    executable: str,
    notebook_path: Path,
    workspace: str,
    lakehouse: str,
    notebook_name: str,
    environment: str,
    jars: Sequence[Path],
    subscription: Optional[str] = None,
    spark_conf: Sequence[str] = (),
) -> List[str]:
    """Build the non-interactive Fabric notebook command."""
    command = [
        executable,
        "notebook",
        "run",
        str(notebook_path),
        "--name",
        notebook_name,
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
    command.extend(("--extra-jars", *(str(path) for path in jars)))
    return command


def build_notebook_marker_command(
    executable: str,
    workspace: str,
    lakehouse: str,
    output_path: Path,
    environment: str,
    subscription: Optional[str] = None,
    marker_path: str = NOTEBOOK_MARKER_PATH,
) -> List[str]:
    """Build a command that downloads structured markers from OneLake."""
    command = [
        executable,
        "lakehouse",
        "cat",
        "--output",
        str(output_path),
        "--env",
        environment,
        "--workspace",
        workspace,
        "--lakehouse",
        lakehouse,
        "--no-create-lakehouse",
    ]
    if subscription:
        command.extend(("--subscription", subscription))
    command.append(marker_path)
    return command


def build_lakehouse_cleanup_command(
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
    """Parse structured diagnostics emitted before a scenario completes."""
    matches = marker_payloads(output, DIAGNOSTIC_MARKER)
    diagnostics: List[Mapping[str, object]] = []
    for match in matches:
        diagnostic = json.loads(match)
        if not isinstance(diagnostic, dict):
            raise ValueError("Fabric scenario diagnostics must be JSON objects")
        diagnostics.append(diagnostic)
    return diagnostics


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
    elapsed_seconds: float,
    failure: Optional[str],
    evidence_path: Path,
) -> None:
    """Write one JUnit test case for Azure Pipelines."""
    suite = ElementTree.Element(
        "testsuite",
        {
            "name": "SynapseML Fabric OpenAIPrompt E2E",
            "tests": "1",
            "failures": "1" if failure else "0",
            "time": f"{elapsed_seconds:.3f}",
        },
    )
    case = ElementTree.SubElement(
        suite,
        "testcase",
        {
            "classname": "fabric_openai_e2e",
            "name": SCENARIO_NAME,
            "time": f"{elapsed_seconds:.3f}",
        },
    )
    if failure:
        ElementTree.SubElement(
            case, "failure", {"message": failure[:500]}
        ).text = failure
    ElementTree.SubElement(case, "system-out").text = f"Evidence: {evidence_path}"
    ElementTree.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)


def checked_jar(raw_path: str, argument: str) -> Path:
    """Resolve one required jar and reject missing or non-jar paths."""
    jar = Path(raw_path).expanduser().resolve()
    if not jar.is_file() or jar.suffix.lower() != ".jar":
        raise ValueError(f"{argument} does not exist or is not a .jar file: {jar}")
    return jar


def create_parser() -> argparse.ArgumentParser:
    """Create the command-line parser."""
    parser = argparse.ArgumentParser(
        description="Run OpenAIPrompt on a managed Fabric Spark notebook."
    )
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--env", default="msit", dest="environment")
    parser.add_argument("--subscription")
    parser.add_argument("--core-jar", required=True)
    parser.add_argument("--cognitive-jar", required=True)
    parser.add_argument("--model", default="gpt-5-mini")
    parser.add_argument("--conf", action="append", default=[])
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--source-repo",
        type=Path,
        default=REPO_ROOT,
        help="Checkout whose commit produced the supplied jars.",
    )
    parser.add_argument("--keep-lakehouse", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run OpenAIPrompt and persist managed-runtime evidence."""
    parser = create_parser()
    args = parser.parse_args(argv)

    executable = shutil.which("fabric-spark-cli")
    if not executable:
        parser.error("fabric-spark-cli is not installed or is not on PATH")

    try:
        core_jar = checked_jar(args.core_jar, "--core-jar")
        cognitive_jar = checked_jar(args.cognitive_jar, "--cognitive-jar")
    except ValueError as error:
        parser.error(str(error))
    jars = (core_jar, cognitive_jar)

    source_repo = args.source_repo.expanduser().resolve()
    if not (source_repo / ".git").exists():
        parser.error(f"--source-repo is not a Git checkout: {source_repo}")

    run_id = new_run_id()
    lakehouse = f"synapseml_openai_e2e_{run_id}"
    notebook_name = f"synapseml-openai-e2e-{run_id}"
    output_dir = (
        args.output_dir or REPO_ROOT / "target" / "fabric-openai-e2e" / run_id
    ).resolve()
    log_path = output_dir / "runner.log"
    evidence_path = output_dir / "evidence.json"
    junit_path = output_dir / "junit.xml"
    notebook_path = output_dir / "scenario.ipynb"
    notebook_marker_path = output_dir / "notebook-markers.jsonl"
    scenario_args = (
        "--expected-core-jar",
        core_jar.name,
        "--expected-cognitive-jar",
        cognitive_jar.name,
        "--model",
        args.model,
    )

    command = build_notebook_submission_command(
        executable=executable,
        notebook_path=notebook_path,
        workspace=args.workspace,
        lakehouse=lakehouse,
        notebook_name=notebook_name,
        environment=args.environment,
        subscription=args.subscription,
        spark_conf=args.conf,
        jars=jars,
    )
    marker_command = build_notebook_marker_command(
        executable=executable,
        workspace=args.workspace,
        lakehouse=lakehouse,
        output_path=notebook_marker_path,
        environment=args.environment,
        subscription=args.subscription,
    )
    notebook_cleanup_command = build_notebook_cleanup_command(
        executable=executable,
        workspace=args.workspace,
        notebook_name=notebook_name,
        environment=args.environment,
        subscription=args.subscription,
    )
    lakehouse_cleanup_command = build_lakehouse_cleanup_command(
        executable=executable,
        workspace=args.workspace,
        lakehouse=lakehouse,
        environment=args.environment,
        subscription=args.subscription,
    )

    if args.dry_run:
        print(
            json.dumps(
                {
                    "lakehouse": lakehouse,
                    "lakehouseCleanupCommand": lakehouse_cleanup_command,
                    "markerCommand": marker_command,
                    "notebookCleanupCommand": notebook_cleanup_command,
                    "submissionCommand": command,
                },
                indent=2,
            )
        )
        return 0

    output_dir.mkdir(parents=True, exist_ok=False)
    write_scenario_notebook(notebook_path, SCENARIO_PATH, scenario_args)

    started = datetime.now(timezone.utc)
    submission_code = 1
    marker_code: Optional[int] = None
    notebook_cleanup_code: Optional[int] = None
    lakehouse_cleanup_code: Optional[int] = None
    runtime_evidence: Mapping[str, object] = {}
    runtime_diagnostics: List[Mapping[str, object]] = []
    failure: Optional[str] = None
    try:
        submission_code, _ = run_and_tee(command, log_path)
        marker_code, _ = run_and_tee(marker_command, log_path)
        marker_output = ""
        if marker_code == 0:
            marker_output = notebook_marker_path.read_text(
                encoding="utf-8", errors="replace"
            )
        try:
            runtime_diagnostics = parse_runtime_diagnostics(marker_output)
        except (ValueError, json.JSONDecodeError) as error:
            failure = f"Invalid Fabric OpenAIPrompt diagnostics: {error}"

        if submission_code != 0 and failure is None:
            failure = f"fabric-spark-cli exited with code {submission_code}"
            if runtime_diagnostics:
                error_type = runtime_diagnostics[-1].get("errorType")
                if isinstance(error_type, str) and error_type:
                    failure += f": {error_type}"
            if marker_code != 0:
                failure += f"; marker retrieval exited with code {marker_code}"
        elif marker_code != 0 and failure is None:
            failure = f"Notebook marker retrieval exited with code {marker_code}"
        elif failure is None:
            try:
                runtime_evidence = parse_runtime_evidence(marker_output)
            except (ValueError, json.JSONDecodeError) as error:
                failure = str(error)
    finally:
        notebook_cleanup_code, _ = run_and_tee(notebook_cleanup_command, log_path)
        if notebook_cleanup_code != 0 and failure is None:
            failure = (
                "Scratch notebook cleanup exited with code " f"{notebook_cleanup_code}"
            )
        if args.keep_lakehouse:
            print(f"Keeping scratch lakehouse for debugging: {lakehouse}")
        else:
            lakehouse_cleanup_code, _ = run_and_tee(lakehouse_cleanup_command, log_path)
            if lakehouse_cleanup_code != 0 and failure is None:
                failure = (
                    "Scratch lakehouse cleanup exited with code "
                    f"{lakehouse_cleanup_code}"
                )

    finished = datetime.now(timezone.utc)
    evidence = {
        "commit": git_commit(source_repo),
        "fabricSparkCliVersion": command_version(executable),
        "finishedAt": finished.isoformat(),
        "failure": failure,
        "jars": [
            {"name": jar.name, "path": str(jar), "sha256": sha256_file(jar)}
            for jar in jars
        ],
        "lakehouse": lakehouse,
        "lakehouseCleanupExitCode": lakehouse_cleanup_code,
        "notebookCleanupExitCode": notebook_cleanup_code,
        "notebookMarkerExitCode": marker_code,
        "notebookMarkerFile": (
            str(notebook_marker_path) if notebook_marker_path.is_file() else None
        ),
        "runtimeDiagnostics": runtime_diagnostics,
        "runtimeEvidence": runtime_evidence,
        "scenario": SCENARIO_NAME,
        "sparkConf": args.conf,
        "startedAt": started.isoformat(),
        "status": "failed" if failure else "passed",
        "submissionExitCode": submission_code,
        "workspace": args.workspace,
    }
    evidence_path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    write_junit(
        junit_path,
        (finished - started).total_seconds(),
        failure,
        evidence_path,
    )
    print(f"SYNAPSEML_FABRIC_E2E_EVIDENCE={evidence_path}")
    if failure:
        print(f"Fabric OpenAIPrompt E2E failed: {failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
