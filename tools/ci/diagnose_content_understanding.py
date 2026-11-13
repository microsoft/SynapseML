"""Reproduce the notebook's analyzer-creation request without building Spark."""

import argparse
import ast
import json
import os
import uuid
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener

from tools.ci.run_content_understanding_notebook import NOTEBOOK, ROOT, redact, sha256


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def notebook_request(notebook: Path, run_id: str) -> tuple:
    cells = {
        cell["id"]: "".join(cell["source"])
        for cell in json.loads(notebook.read_text(encoding="utf-8"))["cells"]
        if cell["cell_type"] == "code"
    }
    assignments = {}
    for cell_id in ("configuration", "create-analyzer"):
        for node in ast.parse(cells[cell_id]).body:
            if (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
            ):
                assignments[node.targets[0].id] = node.value
    # Reuse notebook naming logic without executing its Spark configuration.
    analyzer_id = eval(
        compile(ast.Expression(assignments["analyzer_id"]), str(notebook), "eval"),
        {"__builtins__": {}},
        {"run_id": run_id},
    )
    return (
        ast.literal_eval(assignments["api_version"]),
        analyzer_id,
        ast.literal_eval(assignments["definition"]),
    )


def diagnose(root: Path, output: Path, endpoint: str, key: str) -> int:
    report = {
        "sourceCommit": os.environ.get("BUILD_SOURCEVERSION"),
        "phase": "create-analyzer",
        "requestAccepted": False,
    }
    try:
        if not endpoint or not key or "$(" in key:
            raise ValueError("Missing CU endpoint or resource key")
        notebook = root / NOTEBOOK
        api_version, analyzer_id, definition = notebook_request(
            notebook, "ci-" + uuid.uuid4().hex
        )
        report["notebookSha256"] = sha256(notebook)
        url = (
            endpoint.rstrip("/")
            + f"/contentunderstanding/analyzers/{analyzer_id}"
            + f"?api-version={api_version}&allowReplace=false"
        )
        report["request"] = {"method": "PUT", "url": url, "definition": definition}
        request = Request(
            url,
            data=json.dumps(definition).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Ocp-Apim-Subscription-Key": key,
            },
            method="PUT",
        )
        try:
            response = build_opener(NoRedirect()).open(request)
        except HTTPError as error:
            response = error
        with response:
            report["httpStatus"] = response.code
            report["requestId"] = response.headers.get(
                "apim-request-id"
            ) or response.headers.get("x-ms-request-id")
            report["requestAccepted"] = 200 <= response.code < 300
            body = response.read()
            try:
                body = json.loads(body) if body else {}
                report["serviceError"] = body.get("error")
                report["analyzerStatus"] = body.get("status")
            except (ValueError, AttributeError):
                report["responseWasJson"] = False
    except Exception as error:
        report["diagnosticError"] = type(error).__name__
        report["requestAccepted"] = False
    output.mkdir(parents=True, exist_ok=True)
    text = redact(json.dumps(report, indent=2), key)
    (output / "evidence.json").write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if report["requestAccepted"] else 1


def inspect_models(root: Path, output: Path, endpoint: str, key: str) -> int:
    api_version, _, _ = notebook_request(root / NOTEBOOK, "inspection")
    report = {"phase": "inspect-models"}
    for name, path in (
        ("defaults", "defaults"),
        ("baseAnalyzer", "analyzers/prebuilt-document"),
    ):
        request = Request(
            endpoint.rstrip("/")
            + f"/contentunderstanding/{path}?api-version={api_version}",
            headers={"Ocp-Apim-Subscription-Key": key},
        )
        try:
            response = build_opener(NoRedirect()).open(request)
        except HTTPError as error:
            response = error
        with response:
            body = json.loads(response.read())
            report[name] = {
                "httpStatus": response.code,
                **{
                    field: body[field]
                    for field in (
                        "models",
                        "supportedModels",
                        "modelDeployments",
                        "error",
                    )
                    if field in body
                },
            }
    output.mkdir(parents=True, exist_ok=True)
    text = redact(json.dumps(report, indent=2), key)
    (output / "models.json").write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inspect-models", action="store_true")
    arguments = parser.parse_args()
    raise SystemExit(
        (inspect_models if arguments.inspect_models else diagnose)(
            ROOT,
            ROOT / "target" / "cu-analyzer-diagnostic",
            os.environ.get("CONTENT_UNDERSTANDING_ENDPOINT", ""),
            os.environ.get("CONTENT_UNDERSTANDING_API_KEY", ""),
        )
    )
