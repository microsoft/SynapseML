# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""
Secure dynamic class loading for SynapseML model deserialization.

All classname-based imports during model loading MUST go through
``secure_import_class`` to prevent arbitrary code execution via
crafted model metadata.
"""

import importlib

_ALLOWED_MODULE_PREFIXES = ("pyspark.", "synapse.ml.")


def secure_import_class(fully_qualified_name):
    """
    Import a Python class by its fully-qualified name, restricted to
    an allowlist of trusted module prefixes.

    Args:
        fully_qualified_name (str): e.g. ``"pyspark.ml.classification.LogisticRegression"``

    Returns:
        type: The resolved Python class.

    Raises:
        ImportError: If the name does not start with an allowed prefix.
    """
    if not fully_qualified_name.startswith(_ALLOWED_MODULE_PREFIXES):
        raise ImportError(
            f"Refusing to load class '{fully_qualified_name}': "
            f"module must start with one of {_ALLOWED_MODULE_PREFIXES}"
        )
    parts = fully_qualified_name.split(".")
    module = ".".join(parts[:-1])
    m = importlib.import_module(module)
    return getattr(m, parts[-1])
