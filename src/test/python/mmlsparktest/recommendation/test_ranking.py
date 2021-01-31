# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# Prepare training and test data.
import unittest

from mmlspark.recommendation.RankingAdapter import RankingAdapter
from mmlspark.recommendation.RankingEvaluator import RankingEvaluator
from mmlspark.recommendation.RankingTrainValidationSplit import RankingTrainValidationSplit
from mmlspark.recommendation.RecommendationIndexer import RecommendationIndexer
from mmlspark.recommendation.SAR import SAR
from mmlsparktest.spark import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import *


class RankingSpec(unittest.TestCase):

    @staticmethod
    def getRatings():
        ratings = spark.createDataFrame([
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
            (3, 10, 3, 3)],
            ["originalCustomerID", "newCategoryID", "rating", "notTime"])
        return ratings

    def test_adapter_evaluator(self):
        ratings = self.getRatings()

        user_id = "originalCustomerID"
        item_id = "newCategoryID"
        rating_id = 'rating'

        user_id_index = "customerID"
        item_id_index = "itemID"

        recommendation_indexer = RecommendationIndexer(userInputCol=user_id, userOutputCol=user_id_index,
                                                       itemInputCol=item_id, itemOutputCol=item_id_index)

        als = ALS(userCol=user_id_index, itemCol=item_id_index, ratingCol=rating_id)

        adapter = RankingAdapter(mode='allUsers', k=5, recommender=als)

        pipeline = Pipeline(stages=[recommendation_indexer, adapter])
        output = pipeline.fit(ratings).transform(ratings)
        print(str(output.take(1)) + "\n")

        metrics = ['ndcgAt', 'fcp', 'mrr']
        for metric in metrics:
            print(metric + ": " + str(RankingEvaluator(k=3, metricName=metric).evaluate(output)))

    def test_adapter_evaluator_sar(self):
        ratings = self.getRatings()

        user_id = "originalCustomerID"
        item_id = "newCategoryID"
        rating_id = 'rating'

        user_id_index = "customerID"
        item_id_index = "itemID"

        recommendation_indexer = RecommendationIndexer(
            userInputCol=user_id, userOutputCol=user_id_index,
            itemInputCol=item_id, itemOutputCol=item_id_index)

        sar = SAR(userCol=user_id_index, itemCol=item_id_index, ratingCol=rating_id)

        adapter = RankingAdapter(mode='allUsers', k=5, recommender=sar)

        pipeline = Pipeline(stages=[recommendation_indexer, adapter])
        output = pipeline.fit(ratings).transform(ratings)
        print(str(output.take(1)) + "\n")

        metrics = ['ndcgAt', 'fcp', 'mrr']
        for metric in metrics:
            print(metric + ": " + str(RankingEvaluator(k=3, metricName=metric).evaluate(output)))

    def test_all_tiny(self):
        ratings = RankingSpec.getRatings()

        customerIndex = StringIndexer() \
            .setInputCol("originalCustomerID") \
            .setOutputCol("customerID")

        ratingsIndex = StringIndexer() \
            .setInputCol("newCategoryID") \
            .setOutputCol("itemID")

        pipeline = Pipeline(stages=[customerIndex, ratingsIndex])

        transformedDf = pipeline.fit(ratings).transform(ratings)

        als = ALS() \
            .setUserCol(customerIndex.getOutputCol()) \
            .setRatingCol('rating') \
            .setItemCol(ratingsIndex.getOutputCol())

        alsModel = als.fit(transformedDf)
        usersRecs = alsModel.recommendForAllUsers(3)
        print(usersRecs.take(1))

        paramGrid = ParamGridBuilder() \
            .addGrid(als.regParam, [1.0]) \
            .build()

        evaluator = RankingEvaluator()

        tvRecommendationSplit = RankingTrainValidationSplit() \
            .setEstimator(als) \
            .setEvaluator(evaluator) \
            .setEstimatorParamMaps(paramGrid) \
            .setTrainRatio(0.8) \
            .setUserCol(customerIndex.getOutputCol()) \
            .setRatingCol('rating') \
            .setItemCol(ratingsIndex.getOutputCol())

        tvmodel = tvRecommendationSplit.fit(transformedDf)

        usersRecs = tvmodel.bestModel._call_java("recommendForAllUsers", 3)

        print(usersRecs.take(1))
        print(tvmodel.validationMetrics)


if __name__ == "__main__":
    result = unittest.main()
