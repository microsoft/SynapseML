# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import importlib
from pathlib import Path

import synapse.ml.recommendation as recommendation


def test_recommendation_wildcard_import_keeps_model_exports():
    namespace = {}
    exec("from synapse.ml.recommendation import *", namespace)

    for name in (
        "RankingAdapterModel",
        "RankingTrainValidationSplitModel",
        "RecommendationIndexerModel",
    ):
        assert name in namespace

    for path in Path(recommendation.__file__).parent.glob("*Model.py"):
        if path.stem.startswith("_"):
            continue
        module = importlib.import_module(f"synapse.ml.recommendation.{path.stem}")
        assert namespace[path.stem] is getattr(module, path.stem)
