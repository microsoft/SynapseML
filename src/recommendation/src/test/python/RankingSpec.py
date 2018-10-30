# Prepare training and test data.
import os
import pyspark
import unittest
import xmlrunner
from mmlspark.RankingAdapter import RankingAdapter
from mmlspark.RankingEvaluator import RankingEvaluator
from mmlspark.RankingTrainValidationSplit import RankingTrainValidationSplit, RankingTrainValidationSplitModel
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.tuning import *
from pyspark.ml.tuning import *
from pyspark.sql.types import *
from pyspark.ml.recommendation import ALS


class RankingSpec(unittest.TestCase):

    @staticmethod
    def getRatings():
        cSchema = StructType([StructField("originalCustomerID", IntegerType()),
                              StructField("newCategoryID", IntegerType()),
                              StructField("rating", IntegerType()),
                              StructField("notTime", IntegerType())])

        ratings = pyspark.sql.SparkSession.builder.getOrCreate().createDataFrame([
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
            (3, 10, 3, 3)], cSchema)
        return ratings

    @staticmethod
    def get_pyspark():
        return pyspark.sql.SparkSession.builder.master("local[*]").config('spark.driver.extraClassPath',
                                                                          "/home/dciborow/mmlspark2/BuildArtifacts/packages/m2/com/microsoft/ml/spark/mmlspark_2.11/0.0/mmlspark_2.11-0.0.jar").getOrCreate()

    def test_adapter_evaluator(self):
        self.get_pyspark()

        ratings = self.getRatings()

        user_id = "originalCustomerID"
        item_id = "newCategoryID"
        rating_id = 'rating'

        user_id_index = "customerID"
        item_id_index = "itemID"

        customer_indexer = StringIndexer(inputCol=user_id, outputCol=user_id_index).fit(ratings)
        items_indexer = StringIndexer(inputCol=item_id, outputCol=item_id_index).fit(ratings)

        als = ALS(userCol=user_id_index, itemCol=item_id_index, ratingCol=rating_id)

        adapter = RankingAdapter(mode='allUsers', k=5, recommender=als)

        pipeline = Pipeline(stages=[customer_indexer, items_indexer, adapter])
        output = pipeline.fit(ratings).transform(ratings)
        print(str(output.take(1)) + "\n")

        metrics = ['ndcgAt', 'fcp', 'mrr']
        for metric in metrics:
            print(metric + ": " + str(RankingEvaluator(k=3, metricName=metric).evaluate(output)))


if __name__ == "__main__":
    result = unittest.main(testRunner=xmlrunner.XMLTestRunner(output=os.getenv("TEST_RESULTS", "TestResults")), \
                           failfast=False, buffer=False, catchbreak=False)
