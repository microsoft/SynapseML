#!/usr/bin/env python
"""Run the actual CU semantic notebook with this build's packaged artifacts."""

import hashlib
import importlib
import importlib.metadata
import json
import os
import platform
import subprocess
import sys
import traceback
import uuid
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import url2pathname

ROOT = Path(__file__).resolve().parents[2]
JAR_SNAPSHOT = Path("target") / "cu-semantic-notebook" / "packaged-jars.json"
NOTEBOOK = (
    Path("docs")
    / "Explore Algorithms"
    / "AI Services"
    / "Quickstart - Semantic Extraction with Content Understanding.ipynb"
)
CELL_IDS = (
    "configuration",
    "sample-input",
    "create-analyzer",
    "submit-and-save",
    "recovery-paths",
    "poll-and-save",
    "display-fields",
)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def only_file(paths, description):
    paths = list(paths)
    if len(paths) != 1:
        raise RuntimeError(f"Expected exactly one {description}, found {len(paths)}")
    return paths[0]


def verify_wheel(root, module, anchor):
    distribution = importlib.metadata.distribution(f"synapseml-{module}")
    wheel = only_file(
        (root / module / "target").glob(
            "scala-*/generated/package/python/"
            f"synapseml_{module}-{distribution.version}-*.whl"
        ),
        f"built {module} wheel matching the installed version",
    )
    anchor_path = anchor.replace(".", "/") + ".py"
    loaded = Path(importlib.import_module(anchor).__file__).resolve()
    expected = Path(distribution.locate_file(anchor_path)).resolve()
    if loaded != expected:
        raise RuntimeError(
            f"The imported {module} module is not installed from the wheel: "
            f"expected {expected}, loaded {loaded}"
        )
    with zipfile.ZipFile(wheel) as archive:
        members = [
            name
            for name in archive.namelist()
            if name.startswith("synapse/") and name.endswith(".py")
        ]
        if anchor_path not in members:
            raise RuntimeError(f"The {module} wheel is missing its runtime module")
        for name in members:
            installed = Path(distribution.locate_file(name))
            if not installed.is_file() or installed.read_bytes() != archive.read(name):
                raise RuntimeError(
                    f"Installed Python code differs from {wheel.name}: {name}"
                )
    return {
        "file": wheel.name,
        "version": distribution.version,
        "sha256": sha256(wheel),
    }


def snapshot_jars(root):
    from synapse.ml.core import __spark_package_version__

    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()
    if commit != os.environ["BUILD_SOURCEVERSION"]:
        raise RuntimeError("The checkout does not match the CI build source version")
    jars = {}
    for module in ("core", "cognitive"):
        built = only_file(
            (root / module / "target").glob(
                f"scala-*/synapseml-{module}_*-{__spark_package_version__}.jar"
            ),
            f"built {module} JAR",
        )
        jars[module] = {
            "file": built.name,
            "version": __spark_package_version__,
            "sha256": sha256(built),
        }
    destination = root / JAR_SNAPSHOT
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps({"sourceCommit": commit, "jars": jars}, indent=2) + "\n",
        encoding="utf-8",
    )
    return 0


def verify_jar(root, module, version, location):
    snapshot = json.loads((root / JAR_SNAPSHOT).read_text(encoding="utf-8"))
    if snapshot["sourceCommit"] != os.environ["BUILD_SOURCEVERSION"]:
        raise RuntimeError("The packaged JAR record belongs to another build source")
    expected = snapshot["jars"][module]
    if expected["version"] != version:
        raise RuntimeError(f"The packaged {module} JAR version does not match Python")
    uri = urlsplit(location)
    if uri.scheme != "file" or uri.netloc:
        raise RuntimeError(
            f"The loaded {module} class must come from a local packaged JAR"
        )
    loaded = Path(url2pathname(uri.path))
    if loaded.suffix != ".jar":
        raise RuntimeError(f"The loaded {module} class must come from a packaged JAR")
    loaded_hash = sha256(loaded)
    if loaded_hash != expected["sha256"]:
        raise RuntimeError(
            f"The loaded {module} JAR differs from this build: "
            f"expected {expected['file']} ({expected['sha256']}), "
            f"loaded {loaded} ({loaded_hash})"
        )
    return expected


def runtime_evidence(root, spark):
    from synapse.ml.core import __spark_package_version__
    from synapse.ml.services.contentunderstanding import ContentUnderstanding

    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()
    if commit != os.environ["BUILD_SOURCEVERSION"]:
        raise RuntimeError("The checkout does not match the CI build source version")
    if (
        os.environ.get("BUILD_SOURCEBRANCH", "").startswith("refs/pull/")
        and commit[:7] not in __spark_package_version__
    ):
        raise RuntimeError(
            "The packaged version does not identify this PR merge commit"
        )
    parents = subprocess.check_output(
        ["git", "show", "-s", "--format=%P", "HEAD"], cwd=root, text=True
    ).split()
    loader = spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
    classes = {
        "core": loader.loadClass("com.microsoft.azure.synapse.ml.param.ServiceParam"),
        "cognitive": ContentUnderstanding()._java_obj.getClass(),
    }
    anchors = {
        "core": "synapse.ml.core.init_spark",
        "cognitive": "synapse.ml.services.contentunderstanding.ContentUnderstanding",
    }
    wheels = {name: verify_wheel(root, name, anchors[name]) for name in classes}
    if wheels["core"]["version"] != wheels["cognitive"]["version"]:
        raise RuntimeError("Core and cognitive Python package versions do not match")
    jars = {
        name: verify_jar(
            root,
            name,
            __spark_package_version__,
            cls.getProtectionDomain().getCodeSource().getLocation().toString(),
        )
        for name, cls in classes.items()
    }
    return {
        "buildId": os.environ.get("BUILD_BUILDID"),
        "sourceCommit": commit,
        "sourceParents": parents,
        "python": platform.python_version(),
        "spark": spark.version,
        "scala": spark._jvm.scala.util.Properties.versionNumberString(),
        "java": spark._jvm.java.lang.System.getProperty("java.version"),
        "pythonWheels": wheels,
        "jvmJars": jars,
    }


def semantic_results(namespace):
    results = namespace["results"].collect()
    if (
        len(results) != 1
        or results[0].requestError is not None
        or results[0].analysis.status != "Succeeded"
        or not results[0].analysis.operationLocation
    ):
        raise RuntimeError(
            "The saved notebook result must contain one successful operation"
        )
    fields = [
        row.asDict(recursive=True)
        for row in namespace["extracted"].orderBy("contentIndex", "fieldName").collect()
    ]
    by_name = {field["fieldName"]: field for field in fields}
    if (
        (by_name.get("Supplier", {}).get("value") or "").strip()
        != "Contoso Office Supplies"
        or float(by_name.get("TotalAmount", {}).get("value") or "nan") != 125.0
        or by_name.get("RequestedDeliveryDate", {}).get("value") != "2026-02-01"
        or not (by_name.get("OrderSummary", {}).get("value") or "").strip()
    ):
        raise RuntimeError(
            "Semantic fields do not match the notebook's synthetic input"
        )
    supplier = by_name["Supplier"]
    confidence = supplier.get("confidence")
    if (
        confidence is None
        or not 0 <= confidence <= 1
        or not supplier.get("source")
        or not supplier.get("spans")
        or not supplier.get("supportingText")
    ):
        raise RuntimeError("Supplier confidence and source grounding must be returned")
    return fields, results[0].analysis.operationLocation


def execute_notebook(path, report):
    notebook = json.loads(path.read_text(encoding="utf-8"))
    cells = {
        cell["id"]: compile("".join(cell["source"]), f"{path}#{cell['id']}", "exec")
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
    }
    if tuple(cells) != CELL_IDS:
        raise RuntimeError("The semantic notebook cell contract changed")
    report.update(notebook=path.name, notebookSha256=sha256(path))
    namespace = {}
    for cell_id, code in cells.items():
        report["phase"] = cell_id
        exec(code, namespace)
        if cell_id == "configuration":
            report.update(
                runId=namespace["run_id"],
                analyzerId=namespace["analyzer_id"],
                apiVersion=namespace["api_version"],
            )
    fields, operation = semantic_results(namespace)
    report["fields"] = fields
    # A fresh namespace cannot reuse the original input DataFrame or analyzer.
    recovery = {}
    report["phase"] = "saved-handle-recovery"
    exec(cells["configuration"], recovery)
    recovery["recovery_input_path"] = namespace["handles_path"]
    recovery["recovery_output_path"] = namespace["run_root"] + "/poll-recovered"
    exec(cells["poll-and-save"], recovery)
    exec(cells["display-fields"], recovery)
    recovered_fields, recovered_operation = semantic_results(recovery)
    if recovered_operation != operation or recovered_fields != fields:
        raise RuntimeError("Saved-handle recovery did not reproduce the same operation")
    report["recovery"] = {
        "sameOperation": True,
        "sameFields": True,
        "status": "Succeeded",
    }


def redact(text, key):
    if key:
        text = text.replace(json.dumps(key)[1:-1], "[REDACTED]")
        text = text.replace(key, "[REDACTED]")
    return text


def execute(root, report_path):
    # CI helper tests do not install Spark; load it only in the execution process.
    from synapse.ml.core.init_spark import init_spark

    report = {"status": "failed", "phase": "packaged-artifact-preflight"}
    spark = None
    try:
        # Bind the Python API to the wheel before Spark puts embedded JAR Python
        # ahead of site-packages on sys.path.
        importlib.import_module(
            "synapse.ml.services.contentunderstanding.ContentUnderstanding"
        )
        spark = init_spark()
        report["runtime"] = runtime_evidence(root, spark)
        execute_notebook(root / NOTEBOOK, report)
        report.update(status="passed", phase="complete")
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
        traceback.print_exc()
    finally:
        text = redact(
            json.dumps(report, indent=2), os.environ["CONTENT_UNDERSTANDING_API_KEY"]
        )
        report_path.write_text(text + "\n", encoding="utf-8")
        print(text)
        if spark is not None:
            spark.stop()
    return 0 if report["status"] == "passed" else 1


def main():
    output = ROOT / "target" / "cu-semantic-notebook"
    output.mkdir(parents=True, exist_ok=True)
    print("##vso[task.setvariable variable=cuNotebookEvidenceReady]true", flush=True)
    report_path = output / "evidence.json"
    report_path.unlink(missing_ok=True)
    key = os.environ.get("CONTENT_UNDERSTANDING_API_KEY", "")
    required = (
        "CONTENT_UNDERSTANDING_ENDPOINT",
        "CONTENT_UNDERSTANDING_API_KEY",
        "CONTENT_UNDERSTANDING_OUTPUT_PATH",
        "BUILD_SOURCEVERSION",
    )
    missing = [
        name
        for name in required
        if not os.environ.get(name, "").strip() or "$(" in os.environ[name]
    ]
    if missing:
        result_code = 2
        message = "Missing notebook CI configuration: " + ", ".join(missing)
        report_path.write_text(
            json.dumps(
                {"status": "failed", "phase": "configuration", "error": message}
            ),
            encoding="utf-8",
        )
    else:
        env = os.environ.copy()
        env["CONTENT_UNDERSTANDING_RUN_ID"] = "ci-" + uuid.uuid4().hex
        # Capture in memory so neither notebook output nor JVM exceptions can
        # reach CI logs before the supplied credential has been redacted.
        completed = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--execute"],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        result_code = completed.returncode
        message = redact(completed.stdout + completed.stderr, key)
        if not report_path.is_file():
            result_code = result_code or 1
            report_path.write_text(
                json.dumps(
                    {
                        "status": "failed",
                        "phase": "execution-process",
                        "error": "The notebook process did not produce evidence",
                    }
                ),
                encoding="utf-8",
            )
        report = json.loads(report_path.read_text(encoding="utf-8"))
        if result_code or report.get("status") != "passed":
            result_code = result_code or 1
            report["status"] = "failed"
            report["processExitCode"] = result_code
            report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(message)
    suite = ET.Element("testsuite", name="ContentUnderstandingNotebook", tests="1")
    suite.set("failures", "1" if result_code else "0")
    case = ET.SubElement(suite, "testcase", name="live_semantic_notebook_and_recovery")
    if result_code:
        ET.SubElement(
            case, "failure", message="Live semantic notebook failed"
        ).text = message
    ET.ElementTree(suite).write(
        ROOT / "target" / "python-test-content-understanding-notebook.xml",
        encoding="utf-8",
        xml_declaration=True,
    )
    return result_code


if __name__ == "__main__":
    if sys.argv[1:] == ["--snapshot-jars"]:
        sys.exit(snapshot_jars(ROOT))
    if sys.argv[1:] == ["--execute"]:
        sys.exit(
            execute(ROOT, ROOT / "target" / "cu-semantic-notebook" / "evidence.json")
        )
    sys.exit(main())
