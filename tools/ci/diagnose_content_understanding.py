"""Reproduce the notebook's analyzer-creation request without building Spark."""

import argparse
import json
import os
import uuid
from pathlib import Path
from time import sleep
from urllib.error import HTTPError
from urllib.parse import urljoin, urlsplit
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


def poll_provisioning(
    url: str, key: str, retry_after: str | None, report: dict
) -> bool:
    report["pollCount"] = 0
    while True:
        sleep(int(retry_after) if retry_after and retry_after.isdigit() else 5)
        request = Request(url, headers={"Ocp-Apim-Subscription-Key": key})
        report["pollCount"] += 1
        try:
            response = build_opener(NoRedirect()).open(
                request, timeout=HTTP_TIMEOUT_SECONDS
            )
        except HTTPError as error:
            response = error
        with response:
            report["provisioning"] = {
                "httpStatus": response.code,
                "requestId": response.headers.get("apim-request-id")
                or response.headers.get("x-ms-request-id"),
            }
            body = json.loads(response.read())
            report["provisioning"]["status"] = body.get("status")
            report["serviceError"] = body.get("error")
            status = str(body.get("status", "")).lower()
            if response.code != 200 or report["serviceError"]:
                return False
            if status not in ("running", "notstarted"):
                return status == "succeeded"
            retry_after = response.headers.get("Retry-After")


def diagnose(root: Path, output: Path, endpoint: str, key: str) -> int:
    report = {
        "sourceCommit": os.environ.get("BUILD_SOURCEVERSION"),
        "phase": "create-analyzer",
        "requestAccepted": False,
        "provisioningSucceeded": False,
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
            operation_location = response.headers.get("operation-location")
            retry_after = response.headers.get("Retry-After")
            body = response.read()
            try:
                body = json.loads(body) if body else {}
                report["serviceError"] = body.get("error")
                report["analyzerStatus"] = body.get("status")
            except (ValueError, AttributeError):
                report["responseWasJson"] = False
        if report["requestAccepted"] and report.get("responseWasJson") is not False:
            report["phase"] = "provision-analyzer"
            if str(report.get("analyzerStatus", "")).lower() == "ready":
                report["provisioningSucceeded"] = not report.get("serviceError")
            else:
                if not operation_location:
                    raise ValueError("Accepted creation has no operation location")
                operation_url = urljoin(url, operation_location)
                origin, target = urlsplit(url), urlsplit(operation_url)
                origins = [
                    (
                        uri.scheme,
                        uri.hostname,
                        uri.port or (443 if uri.scheme == "https" else 80),
                    )
                    for uri in (origin, target)
                ]
                if (
                    origins[0] != origins[1]
                    or target.username is not None
                    or target.password is not None
                    or target.fragment
                ):
                    raise ValueError("Unsafe analyzer operation location")
                report["provisioningSucceeded"] = poll_provisioning(
                    operation_url, key, retry_after, report
                )
    except Exception as error:
        report["diagnosticError"] = type(error).__name__
        if report["phase"] == "create-analyzer":
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
        if report["provisioningSucceeded"]
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


def configure_defaults(root: Path, output: Path, endpoint: str, key: str) -> int:
    report = {
        "phase": "configure-defaults",
        "status": "failed",
        "sourceCommit": os.environ.get("BUILD_SOURCEVERSION"),
    }

    def send(request):
        try:
            response = build_opener(NoRedirect()).open(
                request, timeout=HTTP_TIMEOUT_SECONDS
            )
        except HTTPError as error:
            response = error
        with response:
            body = json.loads(response.read())
            report["httpStatus"] = response.code
            report["requestId"] = response.headers.get(
                "apim-request-id"
            ) or response.headers.get("x-ms-request-id")
            if response.code != 200:
                report["serviceError"] = body.get("error")
                raise RuntimeError("Defaults request was rejected")
            return body["modelDeployments"]

    try:
        inspect_models(root, output, endpoint, key)
        metadata = json.loads((output / "models.json").read_text(encoding="utf-8"))
        deployments = json.loads(
            (output / "deployments.json").read_text(encoding="utf-8")
        )
        base = metadata["baseAnalyzer"]
        if base["httpStatus"] != 200:
            raise RuntimeError("Could not read the base analyzer's supported models")
        selected = {
            "completion": "gpt-4.1",
            "embedding": "text-embedding-3-large",
        }
        mappings = {}
        for role, model in selected.items():
            if model not in base["supportedModels"][role] or not any(
                item["deployment"] == model
                and item["model"]["name"] == model
                and item["state"] == "Succeeded"
                for item in deployments
            ):
                raise RuntimeError(f"No compatible existing deployment for {role}")
            mappings[model] = model
            mappings[f"prebuilt-analyzer-{role}"] = model
        defaults = metadata["defaults"]
        if defaults["httpStatus"] == 200:
            before = defaults["modelDeployments"]
        elif (
            defaults["httpStatus"] == 400
            and defaults.get("error", {}).get("innererror", {}).get("code")
            == "DefaultsNotSet"
        ):
            before = {}
        else:
            raise RuntimeError("Could not read existing CU defaults")
        report["before"] = before
        if any(
            name in before and before[name] != value for name, value in mappings.items()
        ):
            raise RuntimeError("Refusing to overwrite an existing CU model mapping")
        missing = {
            name: value for name, value in mappings.items() if name not in before
        }
        report["patch"] = {"modelDeployments": missing}
        api_version, _, _ = notebook_request(root / NOTEBOOK, "configuration")
        url = (
            endpoint.rstrip("/")
            + f"/contentunderstanding/defaults?api-version={api_version}"
        )
        headers = {"Ocp-Apim-Subscription-Key": key}
        if missing:
            send(
                Request(
                    url,
                    data=json.dumps(report["patch"]).encode(),
                    headers={**headers, "Content-Type": "application/merge-patch+json"},
                    method="PATCH",
                )
            )
            report["patchRequestId"] = report["requestId"]
        after = send(Request(url, headers=headers))
        report["after"] = after
        if any(
            after.get(name) != value for name, value in {**before, **mappings}.items()
        ):
            raise RuntimeError(
                "CU defaults readback did not preserve the expected mappings"
            )
        report["status"] = "configured" if missing else "unchanged"
    except Exception as error:
        report["configurationError"] = f"{type(error).__name__}: {error}"
    output.mkdir(parents=True, exist_ok=True)
    text = redact(json.dumps(report, indent=2), key)
    (output / "defaults.json").write_text(text + "\n", encoding="utf-8")
    print(text)
    return 1 if report["status"] == "failed" else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--inspect-models", action="store_true")
    mode.add_argument("--configure-defaults", action="store_true")
    arguments = parser.parse_args()
    run = (
        configure_defaults
        if arguments.configure_defaults
        else inspect_models
        if arguments.inspect_models
        else diagnose
    )
    raise SystemExit(
        run(
            ROOT,
            ROOT / "target" / "cu-analyzer-diagnostic",
            os.environ.get("CONTENT_UNDERSTANDING_ENDPOINT", ""),
            os.environ.get("CONTENT_UNDERSTANDING_API_KEY", ""),
        )
    )
