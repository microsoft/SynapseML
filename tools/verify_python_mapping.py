#!/usr/bin/env python

"""
Lightweight preflight check for Java→Python stage mapping.

Goal: fail fast when SynapseML Java class names cannot be resolved to
their corresponding Python wrapper classes, instead of discovering
the issue piecemeal via individual constructor tests.

Intended usage:

    # Inside the same conda env used for Python tests (e.g., synapseml)
    python tools/verify_python_mapping.py

This script assumes the synapseml-core (and relevant module) wheels are
installed in the active environment.
"""

from __future__ import annotations

import importlib
import sys
from dataclasses import dataclass
from typing import List, Optional, Tuple, Type


@dataclass
class StageExpectation:
    java_class: str
    description: str


# Sentinel stages that have historically exposed mapping / export issues.
STAGE_EXPECTATIONS: List[StageExpectation] = [
    StageExpectation(
        java_class="com.microsoft.azure.synapse.ml.explainers.ImageSHAP",
        description="ImageSHAP explainer",
    ),
    StageExpectation(
        java_class="com.microsoft.azure.synapse.ml.io.http.JSONOutputParser",
        description="HTTP JSONOutputParser for SimpleHTTPTransformer",
    ),
    StageExpectation(
        java_class="com.microsoft.azure.synapse.ml.opencv.ImageTransformer",
        description="OpenCV ImageTransformer",
    ),
    StageExpectation(
        java_class="com.microsoft.azure.synapse.ml.recommendation.RankingTrainValidationSplitModel",
        description="RankingTrainValidationSplitModel",
    ),
]


def _java_to_python_fqcn(java_name: str) -> str:
    """
    Mirror the string-level mapping that java_params_patch applies:

      org.apache.spark.*              -> pyspark.*
      com.microsoft.azure.synapse.ml.* -> synapse.ml.*
    """
    py_name = java_name.replace("org.apache.spark", "pyspark")
    py_name = py_name.replace("com.microsoft.azure.synapse.ml", "synapse.ml")
    return py_name


def _resolve_python_target(py_name: str) -> Tuple[Optional[object], Optional[str]]:
    """
    Resolve a dotted Python name into an object, with a few fallbacks:

    - Treat py_name as module + attribute and try getattr.
    - If that yields a module, look for a class of the same name inside it.
    - If the attribute is missing, try importing a submodule and looking
      for a class of the same name there.
    """
    parts = py_name.split(".")
    if len(parts) < 2:
        return None, f"cannot split '{py_name}' into module + attribute"

    module_name = ".".join(parts[:-1])
    attr_name = parts[-1]

    try:
        mod = importlib.import_module(module_name)
    except Exception as e:  # noqa: BLE001
        return None, f"import_module('{module_name}') failed: {e!r}"

    # First try a direct attribute on the package.
    obj = getattr(mod, attr_name, None)

    # If we got a module, see if it defines a class of the same name.
    if obj is not None and not isinstance(obj, type):
        try:
            inner = getattr(obj, attr_name)
        except Exception:  # noqa: BLE001
            inner = None
        if isinstance(inner, type):
            obj = inner

    # If still no class, try importing a submodule and looking there.
    if obj is None or not isinstance(obj, type):
        submod = None
        try:
            submod = importlib.import_module(f"{module_name}.{attr_name}")
        except Exception:
            submod = None
        if submod is not None:
            candidate = getattr(submod, attr_name, None)
            if isinstance(candidate, type):
                obj = candidate

    if obj is None:
        return None, (
            f"could not resolve '{py_name}' to a class via package or submodule; "
            "check Python exports for this stage"
        )

    if not isinstance(obj, type):
        return None, (
            f"resolved '{py_name}' to {obj!r}, which is not a class; "
            "check for module vs class export mismatches"
        )

    return obj, None


def _verify_stage(java_class: str, description: str) -> Optional[str]:
    py_fqcn = _java_to_python_fqcn(java_class)
    cls, error = _resolve_python_target(py_fqcn)
    if error is not None:
        return (
            f"[FAIL] {description}: Java '{java_class}' → Python '{py_fqcn}': {error}"
        )

    # Optionally enforce that this is at least a JavaParams/JavaWrapper subclass.
    try:
        from pyspark.ml.wrapper import JavaParams, JavaWrapper  # type: ignore
    except Exception:  # noqa: BLE001
        # If pyspark isn't importable, we still checked that the export is a class.
        return None

    if not issubclass(cls, (JavaParams, JavaWrapper)):  # type: ignore[arg-type]
        return (
            f"[WARN] {description}: resolved '{py_fqcn}' to class {cls!r}, "
            "but it is not a JavaParams/JavaWrapper subclass; "
            "this may still be acceptable, but double-check the wrapper."
        )

    return None


def main() -> int:
    # Ensure the SynapseML mapping patch is present and importable.
    try:
        from synapse.ml.core.serialize import (  # type: ignore[attr-defined]
            java_params_patch as _java_params_patch,  # noqa: F401
        )
    except Exception as e:  # noqa: BLE001
        print(
            "ERROR: Could not import 'synapse.ml.core.serialize.java_params_patch'.\n"
            "Make sure the synapseml-core wheel for this branch is installed in "
            "the active Python environment before running this check.\n"
            f"Underlying error: {e!r}",
            file=sys.stderr,
        )
        return 1

    failures: List[str] = []
    warnings: List[str] = []

    for stage in STAGE_EXPECTATIONS:
        msg = _verify_stage(stage.java_class, stage.description)
        if msg is None:
            continue
        if msg.startswith("[FAIL]"):
            failures.append(msg)
        else:
            warnings.append(msg)

    if warnings:
        print("=== verify_python_mapping: warnings ===", file=sys.stderr)
        for w in warnings:
            print(w, file=sys.stderr)
        print(file=sys.stderr)

    if failures:
        print("=== verify_python_mapping: failures ===", file=sys.stderr)
        for f in failures:
            print(f, file=sys.stderr)
        print(
            "\nOne or more SynapseML Java stages could not be mapped cleanly to "
            "Python wrapper classes. This indicates a systemic mapping / "
            "packaging issue (java_params_patch + Python exports), not a single "
            "test flake.\n",
            file=sys.stderr,
        )
        return 1

    print(
        "verify_python_mapping: all sentinel stages mapped to Python wrapper "
        "classes successfully."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
