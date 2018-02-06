import pyspark
import unittest
from mmlspark.MsftRecommendationEvaluator import MsftRecommendationEvaluator
from mmlspark.SAR import SAR
from mmlspark.TrainValidRecommendSplit import TrainValidRecommendSplit
from pyspark.ml.tuning import *
from pyspark.sql.types import *


class RecommendTestHelper:

    @staticmethod
    def getSpark():
        # os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/dciborow/bin/python3"
        # os.environ["PYSPARK_PYTHON"] = "/home/dciborow/bin/python3"

        spark = pyspark.sql.SparkSession.builder.master("local[*]") \
            .config('spark.driver.extraClassPath',
                    "/home/dciborow/mmlspark2/BuildArtifacts/packages/m2/com/microsoft/ml/spark/mmlspark_2.11/0.0/mmlspark_2.11-0.0.jar") \
            .getOrCreate()

        return spark

    @staticmethod
    def getRatings():
        cSchema = StructType([StructField("user", IntegerType()),
                              StructField("item", IntegerType()),
                              StructField("rating", IntegerType()),
                              StructField("notTime", IntegerType())])

        ratings = RecommendTestHelper.getSpark().createDataFrame([
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


class SARSpec(unittest.TestCase):
    def test_simple(self):
        ratings = RecommendTestHelper.getRatings()

        # customerIndex = StringIndexer() \
        #     .setInputCol("customerIDOrg") \
        #     .setOutputCol("customerID")
        #
        # ratingsIndex = StringIndexer() \
        #     .setInputCol("itemIDOrg") \
        #     .setOutputCol("itemID")
        #
        # pipeline = Pipeline(stages=[customerIndex, ratingsIndex])
        #
        # transformedDf = pipeline.fit(ratings).transform(ratings)

        sar = SAR() \
            .setUserCol("user") \
            .setRatingCol('rating') \
            .setItemCol("item") \
            .setSupportThreshold(4)

        sarModel = sar.fit(ratings)
        usersRecs = sarModel.recommendForAllUsers(3)
        print(usersRecs.take(4))
        #
        # paramGrid = ParamGridBuilder() \
        #     .addGrid(alsWReg.regParam, [1.0]) \
        #     .build()
        #
        # evaluator = MsftRecommendationEvaluator().setSaveAll(True)
        #
        # tvRecommendationSplit = TrainValidRecommendSplit() \
        #     .setEstimator(sar) \
        #     .setEvaluator(evaluator) \
        #     .setEstimatorParamMaps(paramGrid) \
        #     .setTrainRatio(0.8) \
        #     .setUserCol(customerIndex.getOutputCol()) \
        #     .setRatingCol('rating') \
        #     .setItemCol(ratingsIndex.getOutputCol())
        #
        # tvmodel = tvRecommendationSplit.fit(transformedDf)
        #
        # usersRecs = tvmodel.bestModel._call_java("recommendForAllUsers", 3)


class TrainValidRecommendSplitSpec(unittest.TestCase):
    def test_validation(self):
        ratings = RecommendTestHelper.getRatings()

        # customerIndex = StringIndexer() \
        #     .setInputCol("originalCustomerID") \
        #     .setOutputCol("customerID")
        #
        # ratingsIndex = StringIndexer() \
        #     .setInputCol("newCategoryID") \
        #     .setOutputCol("itemID")
        #
        # pipeline = Pipeline(stages=[customerIndex, ratingsIndex])
        #
        # transformedDf = pipeline.fit(ratings).transform(ratings)

        sar = SAR() \
            .setUserCol("user") \
            .setRatingCol('rating') \
            .setItemCol("item") \
            .setSupportThreshold(4)

        paramGrid = ParamGridBuilder() \
            .addGrid(sar.supportThreshold, [4]) \
            .build()

        evaluator = MsftRecommendationEvaluator().setSaveAll(True)

        tvRecommendationSplit = TrainValidRecommendSplit() \
            .setEstimator(sar) \
            .setEvaluator(evaluator) \
            .setEstimatorParamMaps(paramGrid) \
            .setTrainRatio(0.8) \
            .setUserCol(sar.getUserCol()) \
            .setRatingCol(sar.getRatingCol()) \
            .setItemCol(sar.getItemCol())

        filtered = tvRecommendationSplit._call_java("filterRatings", ratings)

        output = tvRecommendationSplit._call_java("splitDF", filtered)
        train = next(iter(output))

        sarModel = sar.fit(train)
        usersRecs = sarModel.recommendForAllUsers(3)

        print(usersRecs.take(1))

        metrics = evaluator._call_java("getMetricsList").toString()
        print(metrics)

    def test_parameter_sweep(self):
        ratings = RecommendTestHelper.getRatings()

        # customerIndex = StringIndexer() \
        #     .setInputCol("originalCustomerID") \
        #     .setOutputCol("customerID")
        #
        # ratingsIndex = StringIndexer() \
        #     .setInputCol("newCategoryID") \
        #     .setOutputCol("itemID")
        #
        # pipeline = Pipeline(stages=[customerIndex, ratingsIndex])
        #
        # transformedDf = pipeline.fit(ratings).transform(ratings)

        sar = SAR() \
            .setUserCol("user") \
            .setRatingCol('rating') \
            .setItemCol("item") \
            .setSupportThreshold(4)

        paramGrid = ParamGridBuilder() \
            .addGrid(sar.supportThreshold, [4]) \
            .build()

        evaluator = MsftRecommendationEvaluator().setSaveAll(True)

        tvRecommendationSplit = TrainValidRecommendSplit() \
            .setEstimator(sar) \
            .setEvaluator(evaluator) \
            .setEstimatorParamMaps(paramGrid) \
            .setTrainRatio(0.8) \
            .setUserCol(sar.getUserCol()) \
            .setRatingCol(sar.getRatingCol()) \
            .setItemCol(sar.getItemCol())
        print(tvRecommendationSplit.getEstimatorParamMaps())
        tvmodel = tvRecommendationSplit._call_java("fit", ratings)

        filtered = tvRecommendationSplit._call_java("filterRatings", ratings)
        print(str(filtered.count()) + "\n")

        output = tvRecommendationSplit._call_java("splitDF", filtered)
        train = output(0)
        print(output)
        print(train.count())
        # tvmodel = tvRecommendationSplit.fit(ratings)

        usersRecs = tvmodel.bestModel._call_java("recommendForAllUsers", 3)

        print(usersRecs.take(1))
        print(tvmodel.validationMetrics)

        metrics = evaluator._call_java("getMetricsList").toString()
        print(metrics)


if __name__ == '__main__':
    unittest.main()
    # result = unittest.main(testRunner=xmlrunner.XMLTestRunner(output=os.getenv("TEST_RESULTS", "TestResults")), \
    #                   failfast=False, buffer=False, catchbreak=False)
