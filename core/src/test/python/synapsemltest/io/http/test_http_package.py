# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.io.http import HTTPTransformer, http_udf


def test_http_package_exports_generated_classes_and_manual_functions():
    assert HTTPTransformer is not None
    assert callable(http_udf)
