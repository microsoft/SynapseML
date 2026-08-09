# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.


def test_recommendation_wildcard_import_keeps_model_exports():
    namespace = {}
    exec("from synapse.ml.recommendation import *", namespace)

    for name in (
        "RankingAdapterModel",
        "RankingTrainValidationSplitModel",
        "RecommendationIndexerModel",
    ):
        assert name in namespace
