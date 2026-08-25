# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import pytest

from tools.ci.patch_internal_typing_support import (
    CURRENT_DECLARATION,
    CURRENT_PACKAGE_DATA,
    LEGACY_PACKAGE_DATA,
    patch_internal_typing_support,
)


def _helper_source(package_data):
    return (
        "ORIGINAL_PACKAGE_DATA = 'unused'\n"
        f"TYPING_PACKAGE_DATA = {package_data!r}\n"
        "AFTER_ASSIGNMENT = True\n"
    )


def test_updates_legacy_package_data_and_is_idempotent(tmp_path):
    helper = tmp_path / "typing_build_support.py"
    helper.write_text(_helper_source(LEGACY_PACKAGE_DATA), encoding="utf-8")

    assert patch_internal_typing_support(helper)
    patched = helper.read_text(encoding="utf-8")
    assert CURRENT_DECLARATION in patched
    assert not patch_internal_typing_support(helper)

    namespace = {}
    exec(patched, namespace)
    assert namespace["TYPING_PACKAGE_DATA"] == CURRENT_PACKAGE_DATA
    assert namespace["AFTER_ASSIGNMENT"]


def test_rejects_unknown_package_data(tmp_path):
    helper = tmp_path / "typing_build_support.py"
    helper.write_text(_helper_source("package_data={}"), encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported TYPING_PACKAGE_DATA"):
        patch_internal_typing_support(helper)


def test_rejects_missing_package_data_assignment(tmp_path):
    helper = tmp_path / "typing_build_support.py"
    helper.write_text("OTHER_VALUE = True\n", encoding="utf-8")

    with pytest.raises(ValueError, match="found 0"):
        patch_internal_typing_support(helper)


def test_rejects_nonliteral_package_data(tmp_path):
    helper = tmp_path / "typing_build_support.py"
    helper.write_text("TYPING_PACKAGE_DATA = build_value()\n", encoding="utf-8")

    with pytest.raises(ValueError, match="must be a string literal"):
        patch_internal_typing_support(helper)
