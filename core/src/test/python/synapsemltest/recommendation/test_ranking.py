# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# Prepare training and test data.
import tempfile
import unittest

from pyspark.sql import SQLContext
from synapse.ml.recommendation import RankingAdapter
from synapse.ml.recommendation import RankingEvaluator
from synapse.ml.recommendation import RankingTrainValidationSplit
from synapse.ml.recommendation import RecommendationIndexer
from synapse.ml.recommendation import SAR
from synapse.ml.recommendation import SARModel
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

    @staticmethod
    def direct_string_ratings():
        return spark.createDataFrame(
            [
                ("user-a", "item-10", 5.0),
                ("user-a", "item-20", 2.0),
                ("user-b", "item-10", 3.0),
                ("user-b", "item-30", 1.0),
                ("user-c", "item-20", 4.0),
                ("user-c", "item-30", 4.0),
            ],
            ["user", "item", "rating"],
        )

    def test_sar_direct_string_identifiers(self):
        data = self.direct_string_ratings()
        model = SAR(
            userCol="user",
            itemCol="item",
            ratingCol="rating",
            supportThreshold=1,
        ).fit(data)

        self.assertEqual(model.transform(data).count(), data.count())
        self.assertEqual(
            {
                row.user
                for row in model.recommendForAllUsers(2).select("user").collect()
            },
            {"user-a", "user-b", "user-c"},
        )
        subset = spark.createDataFrame(
            [("user-a",), ("unknown-user",), (None,)],
            "user string",
        )
        self.assertEqual(
            [row.user for row in model.recommendForUserSubset(subset, 2).collect()],
            ["user-a"],
        )
        self.assertEqual(
            model.recommendForAllUsers(2).schema["user"].dataType.typeName(),
            "string",
        )
        self.assertEqual(
            model.recommendForAllUsers(2)
            .schema["recommendations"]
            .dataType.elementType["item"]
            .dataType.typeName(),
            "string",
        )
        self.assertEqual(
            {
                row.item
                for row in model.recommendForAllItems(2).select("item").collect()
            },
            {"item-10", "item-20", "item-30"},
        )
        item_subset = spark.createDataFrame(
            [("item-10",), ("unknown-item",), (None,)],
            "item string",
        )
        self.assertEqual(
            [
                row.item
                for row in model.recommendForItemSubset(item_subset, 2).collect()
            ],
            ["item-10"],
        )

    def test_sar_string_model_save_load(self):
        data = self.direct_string_ratings()
        model = SAR(
            userCol="user",
            itemCol="item",
            ratingCol="rating",
            supportThreshold=1,
        ).fit(data)

        with tempfile.TemporaryDirectory() as directory:
            path = directory + "/sar-model"
            model.write().overwrite().save(path)
            loaded = SARModel.load(path)
            self.assertEqual(
                loaded.recommendForAllUsers(2).orderBy("user").collect(),
                model.recommendForAllUsers(2).orderBy("user").collect(),
            )

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
