# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import ast
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest


NOTEBOOK = (
    Path(__file__).resolve().parents[3]
    / "docs"
    / "Explore Algorithms"
    / "OpenAI"
    / "AIPrompt.ipynb"
)


@pytest.mark.parametrize(
    ("diagnostic", "cleanup_failure"),
    [
        (None, None),
        ("incomplete", None),
        ("missing-answer", None),
        (None, "request"),
        ("incomplete", "request"),
        (None, "confirmation"),
    ],
)
def test_stored_response_cleanup_uses_collected_ids_before_validation(
    monkeypatch, capsys, diagnostic, cleanup_failure
):
    notebook = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    cells = [
        "".join(cell["source"])
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
    ]
    setup = ast.parse(next(cell for cell in cells if "def run_example(" in cell))
    helpers = [
        node
        for node in setup.body
        if isinstance(node, ast.FunctionDef)
        and node.name in ("run_example", "validate_example")
    ]
    storage = next(cell for cell in cells if "created_response_ids = []" in cell)

    def stage():
        result = Mock()
        for name in (
            "setPromptTemplate",
            "setStore",
            "setResponseIdCol",
            "setPreviousResponseId",
            "setPreviousResponseIdCol",
            "copy",
        ):
            getattr(result, name).return_value = result
        result.getResponseIdCol.return_value = "response_id"
        return result

    stored = stage()
    continued = stage()
    rows = [
        {
            "response_id": identifier,
            "answer": "ALPHA",
            "error": None,
            "raw_response": json.dumps({"id": identifier}),
        }
        for identifier in ("created-first", "created-second")
    ]
    if diagnostic == "incomplete":
        rows[0]["error"] = {"code": "incomplete"}
    elif diagnostic == "missing-answer":
        rows[0]["answer"] = None
    stored.transform.return_value.collect.return_value = rows
    continued.transform.return_value.collect.return_value = [
        {"answer": "ALPHA", "error": None, "raw_response": '{"output": "ALPHA"}'}
    ]
    response = Mock(status_code=204)
    delete = Mock(return_value=response)

    class DeleteError(Exception):
        pass

    if cleanup_failure == "request":
        delete.side_effect = [DeleteError("Synthetic deletion failure"), response]
    elif cleanup_failure == "confirmation":
        unconfirmed = Mock(status_code=200)
        unconfirmed.json.return_value = {"deleted": False}
        delete.side_effect = [unconfirmed, response]
    monkeypatch.setitem(
        sys.modules,
        "requests",
        SimpleNamespace(delete=delete, RequestException=DeleteError),
    )
    countries = Mock()
    namespace = {
        "json": json,
        "completed_examples": [],
        "prompt_for": Mock(side_effect=[stored, continued]),
        "countries": countries,
        "F": SimpleNamespace(lit=lambda value: value),
        "openai_endpoint": "https://example.invalid",
        "openai_token": "synthetic-token",
    }
    exec(
        compile(ast.Module(body=helpers, type_ignores=[]), str(NOTEBOOK), "exec"),
        namespace,
    )

    if diagnostic:
        with pytest.raises(AssertionError, match="responses/store-and-response-id"):
            exec(compile(storage, str(NOTEBOOK), "exec"), namespace)
        assert namespace["completed_examples"] == []
        continued.transform.assert_not_called()
        if cleanup_failure:
            assert "Could not delete" in capsys.readouterr().err
    else:
        if cleanup_failure:
            with pytest.raises(RuntimeError, match="Could not delete") as error:
                exec(compile(storage, str(NOTEBOOK), "exec"), namespace)
            assert isinstance(error.value.__cause__, (DeleteError, ValueError))
        else:
            exec(compile(storage, str(NOTEBOOK), "exec"), namespace)
        assert namespace["completed_examples"] == [
            "responses/store-and-response-id",
            "responses/scalar-previous-id",
            "responses/column-previous-id",
        ]
    stored.transform.assert_called_once_with(countries)
    stored.transform.return_value.collect.assert_called_once_with()
    assert namespace["created_response_ids"] == ["created-first", "created-second"]
    expected_deleted = (
        ["created-second"] if cleanup_failure else ["created-first", "created-second"]
    )
    assert namespace["deleted_response_ids"] == expected_deleted
    assert delete.call_args_list == [
        call(
            f"https://example.invalid/openai/v1/responses/{identifier}",
            headers={"Authorization": "Bearer synthetic-token"},
            timeout=60,
        )
        for identifier in ("created-first", "created-second")
    ]
    assert response.raise_for_status.call_count == (1 if cleanup_failure else 2)
