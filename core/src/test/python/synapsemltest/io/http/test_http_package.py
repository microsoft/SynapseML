# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.io.http import HTTPTransformer, http_udf
from synapse.ml.io.http.HTTPTransformer import (
    HTTPTransformer as GeneratedHTTPTransformer,
)


def test_http_package_exports_generated_classes_and_manual_functions():
    assert HTTPTransformer is GeneratedHTTPTransformer
    assert callable(http_udf)
