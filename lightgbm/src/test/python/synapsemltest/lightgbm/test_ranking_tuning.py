# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import unittest

from pyspark.ml.linalg import Vectors
from synapse.ml.core.init_spark import init_spark


class LightGBMRankingTuningTests(unittest.TestCase):
    def test_tune_ranker_from_python(self):
        spark = init_spark()
        from synapse.ml.automl import TuneHyperparameters
        from synapse.ml.automl.HyperparamBuilder import (
            DiscreteHyperParam,
            HyperparamBuilder,
            RandomSpace,
        )
        from synapse.ml.lightgbm import LightGBMRanker

        data = spark.createDataFrame(
            [
                (f"q{q}", float(label), Vectors.dense(float(label), float(q)))
                for q in range(8)
                for label in range(3)
            ],
            ["query", "label", "features"],
        ).coalesce(1)
        ranker = LightGBMRanker(
            groupCol="query",
            numIterations=2,
            numTasks=1,
            numThreads=1,
            minDataInLeaf=1,
            verbosity=-1,
        )
        search = (
            HyperparamBuilder()
            .addHyperparam(ranker, ranker.numLeaves, DiscreteHyperParam([2, 3]))
            .build()
        )
        best = TuneHyperparameters(
            models=[ranker],
            paramSpace=RandomSpace(search).space(),
            evaluationMetric="ndcg@5",
            numFolds=2,
            numRuns=2,
            parallelism=1,
        ).fit(data)
        try:
            self.assertGreaterEqual(best.getBestMetric(), 0.0)
            self.assertLessEqual(best.getBestMetric(), 1.0)
            self.assertEqual(best.transform(data).count(), data.count())
        finally:
            best.getBestModel()._java_obj.getLightGBMBooster().freeNativeMemory()


if __name__ == "__main__":
    unittest.main()
