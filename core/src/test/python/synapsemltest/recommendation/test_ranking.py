# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# Prepare training and test data.
import unittest

from pyspark.sql import SQLContext
from synapse.ml.recommendation import RankingAdapter
from synapse.ml.recommendation import RankingEvaluator
from synapse.ml.recommendation import RankingTrainValidationSplit
from synapse.ml.recommendation import RecommendationIndexer
from synapse.ml.recommendation import SAR
from synapse.ml.core.init_spark import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import ParamGridBuilder

spark = init_spark()
sc = SQLContext(spark.sparkContext)

USER_ID = "originalCustomerID"
ITEM_ID = "newCategoryID"
RATING_ID = "rating"
USER_ID_INDEX = "customerID"
ITEM_ID_INDEX = "itemID"

ratings = (
    spark.createDataFrame(
        [
            (0, 1, 4, 4),
            (0, 3, 1, 1),
            (0, 4, 5, 5),
            (0, 5, 3, 3),
            (0, 7, 3, 3),
            (0, 9, 3, 3),
            (0, 10, 3, 3),
            (1, 1, 4, 4),
            (1, 2, 5, 5),
            (1, 3, 1, 1),
            (1, 6, 4, 4),
            (1, 7, 5, 5),
            (1, 8, 1, 1),
            (1, 10, 3, 3),
            (2, 1, 4, 4),
            (2, 2, 1, 1),
            (2, 3, 1, 1),
            (2, 4, 5, 5),
            (2, 5, 3, 3),
            (2, 6, 4, 4),
            (2, 8, 1, 1),
            (2, 9, 5, 5),
            (2, 10, 3, 3),
            (3, 2, 5, 5),
            (3, 3, 1, 1),
            (3, 4, 5, 5),
            (3, 5, 3, 3),
            (3, 6, 4, 4),
            (3, 7, 5, 5),
            (3, 8, 1, 1),
            (3, 9, 5, 5),
            (3, 10, 3, 3),
        ],
        ["originalCustomerID", "newCategoryID", "rating", "notTime"],
    )
    .coalesce(1)
    .cache()
)


class RankingSpec(unittest.TestCase):
    @staticmethod
    def adapter_evaluator(algo):
        recommendation_indexer = RecommendationIndexer(
            userInputCol=USER_ID,
            userOutputCol=USER_ID_INDEX,
            itemInputCol=ITEM_ID,
            itemOutputCol=ITEM_ID_INDEX,
        )

        adapter = RankingAdapter(mode="allUsers", k=5, recommender=algo)
        pipeline = Pipeline(stages=[recommendation_indexer, adapter])
        output = pipeline.fit(ratings).transform(ratings)
        print(str(output.take(1)) + "\n")

        metrics = ["ndcgAt", "fcp", "mrr"]
        for metric in metrics:
            print(
                metric
                + ": "
                + str(RankingEvaluator(k=3, metricName=metric).evaluate(output)),
            )

    # def test_adapter_evaluator_als(self):
    #     als = ALS(userCol=USER_ID_INDEX, itemCol=ITEM_ID_INDEX, ratingCol=RATING_ID)
    #     self.adapter_evaluator(als)
    #
    # def test_adapter_evaluator_sar(self):
    #     sar = SAR(userCol=USER_ID_INDEX, itemCol=ITEM_ID_INDEX, ratingCol=RATING_ID)
    #     self.adapter_evaluator(sar)

    def test_all_tiny(self):
        customer_index = StringIndexer(inputCol=USER_ID, outputCol=USER_ID_INDEX)
        ratings_index = StringIndexer(inputCol=ITEM_ID, outputCol=ITEM_ID_INDEX)

        pipeline = Pipeline(stages=[customer_index, ratings_index])
        transformed_df = pipeline.fit(ratings).transform(ratings)

        als = ALS(
            userCol=customer_index.getOutputCol(),
            ratingCol=RATING_ID,
            itemCol=ratings_index.getOutputCol(),
        )
        als_model = als.fit(transformed_df)
        users_recs = als_model.recommendForAllUsers(3)
        print("One Sample User Recommendation: " + str(users_recs.take(1)))

        param_grid = ParamGridBuilder().addGrid(als.regParam, [1.0]).build()

        evaluator = RankingEvaluator()

        tv_recommendation_split = (
            RankingTrainValidationSplit(estimator=als, evaluator=evaluator)
            .setEstimatorParamMaps(param_grid)
            .setUserCol(customer_index.getOutputCol())
            .setItemCol(ratings_index.getOutputCol())
            .setRatingCol("rating")
            .setRatingCol("rating")
            .setTrainRatio(0.8)
        )

        tv_model = tv_recommendation_split.fit(transformed_df)
        users_recs = tv_model.recommendForAllUsers(3)

        print("Sample User Recommendation: " + str(users_recs.take(1)))
        print("Validation Metrics: " + str(tv_model.validationMetrics))


if __name__ == "__main__":
    result = unittest.main()
