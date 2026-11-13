"""Reproduce the notebook's analyzer-creation request without building Spark."""

import argparse
import json
import os
import uuid
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, build_opener

from tools.ci.run_content_understanding_notebook import (
    HTTP_TIMEOUT_SECONDS,
    NOTEBOOK,
    ROOT,
    NoRedirect,
    cleanup_analyzer,
    notebook_request,
    redact,
    sha256,
)


def diagnose(root: Path, output: Path, endpoint: str, key: str) -> int:
    report = {
        "sourceCommit": os.environ.get("BUILD_SOURCEVERSION"),
        "phase": "create-analyzer",
        "requestAccepted": False,
    }
    cleanup_id = None
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
        cleanup_id = analyzer_id
        try:
            response = build_opener(NoRedirect()).open(
                request, timeout=HTTP_TIMEOUT_SECONDS
            )
        except HTTPError as error:
            response = error
        with response:
            report["httpStatus"] = response.code
            if 300 <= response.code < 500 and response.code != 408:
                cleanup_id = None
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
    finally:
        if cleanup_id is not None:
            report["cleanup"] = cleanup_analyzer(endpoint, key, api_version, cleanup_id)
    output.mkdir(parents=True, exist_ok=True)
    text = redact(json.dumps(report, indent=2), key)
    (output / "evidence.json").write_text(text + "\n", encoding="utf-8")
    print(text)
    return (
        0
        if report["requestAccepted"]
        and report.get("cleanup", {}).get("status") != "failed"
        else 1
    )


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
            response = build_opener(NoRedirect()).open(
                request, timeout=HTTP_TIMEOUT_SECONDS
            )
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
