# Prepare training and test data.
import os
import pyspark
import unittest
import xmlrunner
from mmlspark.RankingEvaluator import RankingEvaluator
from mmlspark.RankingTrainValidationSplit import RankingTrainValidationSplit
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.tuning import *
from pyspark.ml.tuning import *
from pyspark.sql.types import *
from pyspark.ml.recommendation import ALS


class TrainValidRecommendSplitSpec(unittest.TestCase):

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

    def test_all_tiny(self):
        pyspark.sql.SparkSession.builder.master("local[*]") \
            .config('spark.driver.extraClassPath',
                    "/home/dciborow/mmlspark2/BuildArtifacts/packages/m2/com/microsoft/ml/spark/mmlspark_2.11/0.0/mmlspark_2.11-0.0.jar") \
            .getOrCreate()

        ratings = self.getRatings()

        customerIndex = StringIndexer() \
            .setInputCol("originalCustomerID") \
            .setOutputCol("customerID")

        ratingsIndex = StringIndexer() \
            .setInputCol("newCategoryID") \
            .setOutputCol("itemID")

        pipeline = Pipeline(stages=[customerIndex, ratingsIndex])

        transformedDf = pipeline.fit(ratings).transform(ratings)

        alsWReg = ALS() \
            .setUserCol(customerIndex.getOutputCol()) \
            .setRatingCol('rating') \
            .setItemCol(ratingsIndex.getOutputCol())

        paramGrid = ParamGridBuilder() \
            .addGrid(alsWReg.regParam, [1.0]) \
            .build()

        evaluator = RankingEvaluator()

        tvRecommendationSplit = RankingTrainValidationSplit() \
            .setEstimatorParamMaps(paramGrid) \
            .setEstimator(alsWReg) \
            .setEvaluator(evaluator) \
            .setTrainRatio(0.8) \
            .setCollectSubMetrics(True)

        tvmodel = tvRecommendationSplit.fit(transformedDf)
        print(tvmodel.recommendForAllUsers(3))
        print(tvmodel.subMetrics)
        # usersRecs = tvmodel.bestModel.recommendForAllUsers(3)

        # print(usersRecs.take(1))
        # print(tvmodel.validationMetrics)

    def ignore_all_large(self):
        os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/dciborow/bin/python3"
        os.environ["PYSPARK_PYTHON"] = "/home/dciborow/bin/python3"

        store_name = ""
        key = ""
        container = ""

        container = ""

        block_blob_service = BlockBlobService(account_name=store_name, account_key=key)

        data_rating = ""
        data_rating = ""

        temp_path = "/mnt"  # tempfile.gettempdir()

        file_content, file_customer, file_rating = [temp_path + "/" + s for s in
                                                    ["content.csv", "customer.csv", "rating.csv"]]

        block_blob_service.get_blob_to_path(container, data_rating, file_rating)

        spark = pyspark.sql.SparkSession.builder.master("local[*]") \
            .config('spark.driver.extraClassPath',
                    "/home/dciborow/mmlspark2/BuildArtifacts/packages/m2/com/microsoft/ml/spark/mmlspark_2.11/0.0/mmlspark_2.11-0.0.jar") \
            .config('spark.driver.memory', '50G') \
            .config('spark.python.worker.memory', '5G') \
            .getOrCreate()

        spark.sparkContext._jsc.hadoopConfiguration().set(
            'fs.azure.account.key.' + store_name + '.blob.core.windows.net', key)

        wasb = "wasb://" + container + "@" + store_name + ".blob.core.windows.net/" + data_rating
        ratings = spark.read.csv(file_rating, header='true', inferSchema='true').dropna().drop_duplicates()

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

        paramGrid = ParamGridBuilder() \
            .addGrid(als.rank, [80]) \
            .addGrid(als.maxIter, [20]) \
            .addGrid(als.regParam, [0.1]) \
            .build()

        evaluator = MsftRecommendationEvaluator().setK(10).setSaveAll(True)

        tvRecommendationSplit = TrainValidRecommendSplit() \
            .setEstimator(als) \
            .setEvaluator(evaluator) \
            .setEstimatorParamMaps(paramGrid) \
            .setTrainRatio(0.8) \
            .setUserCol(customerIndex.getOutputCol()) \
            .setRatingCol('rating') \
            .setItemCol(ratingsIndex.getOutputCol())

        tvmodel = tvRecommendationSplit.fit(transformedDf)

        usersRecs = tvmodel.bestModel._call_java("recommendForAllUsers", 10)

        print(usersRecs.take(1))
        print(tvmodel.validationMetrics)

        metrics = evaluator._call_java("getMetricsList").toString()
        print(metrics)


if __name__ == "__main__":
    result = unittest.main(testRunner=xmlrunner.XMLTestRunner(output=os.getenv("TEST_RESULTS", "TestResults")), \
                           failfast=False, buffer=False, catchbreak=False)
