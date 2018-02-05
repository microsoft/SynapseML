# Prepare training and test data.
import os
import pyspark
import unittest
import xmlrunner
from azure.storage.blob import BlockBlobService
from mmlspark.MsftRecommendation import MsftRecommendation
from mmlspark.MsftRecommendationEvaluator import MsftRecommendationEvaluator
from mmlspark.TrainValidRecommendSplit import TrainValidRecommendSplit
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendations import ALS
from pyspark.ml.tuning import *


class TrainValidRecommendSplitSpec(unittest.TestCase):
    def test_all_tiny(self):
        os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/dciborow/bin/python3"
        os.environ["PYSPARK_PYTHON"] = "/home/dciborow/bin/python3"

        store_name = "bigdatadevlogs"
        key = ""
        container = "ms-sampledata"

        block_blob_service = BlockBlobService(account_name=store_name, account_key=key)

        data_rating = "iwanttv/day1_sample/percentilerating_videoseen_20170901_20170228_tiny.csv"

        temp_path = "/mnt"  # tempfile.gettempdir()

        file_content, file_customer, file_rating = [temp_path + "/" + s for s in
                                                    ["content.csv", "customer.csv", "rating_tiny.csv"]]

        block_blob_service.get_blob_to_path(container, data_rating, file_rating)

        spark = pyspark.sql.SparkSession.builder.master("local[*]") \
            .config('spark.driver.extraClassPath',
                    "/home/dciborow/mmlspark2/BuildArtifacts/packages/m2/com/microsoft/ml/spark/mmlspark_2.11/0.0/mmlspark_2.11-0.0.jar") \
            .getOrCreate()

        spark.sparkContext._jsc.hadoopConfiguration().set(
            'fs.azure.account.key.[' + store_name + '].blob.core.windows.net', '[' + key + ']')

        wasb = "wasb://" + container + "@" + store_name + ".blob.core.windows.net/" + data_rating
        ratings = spark.read.csv(file_rating, header='true', inferSchema='true').drop_duplicates()

        customerIndex = StringIndexer() \
            .setInputCol("originalCustomerID") \
            .setOutputCol("customerID")

        ratingsIndex = StringIndexer() \
            .setInputCol("newCategoryID") \
            .setOutputCol("itemID")

        pipeline = Pipeline(stages=[customerIndex, ratingsIndex])

        transformedDf = pipeline.fit(ratings).transform(ratings)


        alsModel = alsWReg.fit(transformedDf)
        usersRecs = alsModel._call_java("recommendForAllUsers", 3)
        print(usersRecs.take(1))

        paramGrid = ParamGridBuilder() \
            .addGrid(alsWReg.regParam, [1.0]) \
            .build()

        evaluator = MsftRecommendationEvaluator().setSaveAll(True)

        tvRecommendationSplit = TrainValidRecommendSplit() \
            .setEstimator(alsWReg) \
            .setEvaluator(evaluator) \
            .setEstimatorParamMaps(paramGrid) \
            .setTrainRatio(0.8) \
            .setUserCol(customerIndex.getOutputCol()) \
            .setRatingCol('rating_total') \
            .setItemCol(ratingsIndex.getOutputCol())

        tvmodel = tvRecommendationSplit.fit(transformedDf)

        usersRecs = tvmodel.bestModel._call_java("recommendForAllUsers", 3)

        print(usersRecs.take(1))
        print(tvmodel.validationMetrics)

        metrics = evaluator._call_java("getMetricsList").toString()
        print(metrics)

    def test_all_large(self):
        os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/dciborow/bin/python3"
        os.environ["PYSPARK_PYTHON"] = "/home/dciborow/bin/python3"

        store_name = "bigdatadevlogs"
        key = "s3JDdUC4E7iYIOR0G6kSCPogZvnv46zk53VupX4xyKaJIPyUPVEt7ekqOWxzjUIctQlk9moHP14cTCdeqCRKfw==="
        container = "ms-sampledata"

        container = "datahub"

        block_blob_service = BlockBlobService(account_name=store_name, account_key=key)

        data_rating = "iwanttv/day1_sample/percentilerating_videoseen_20170901_20170228.csv"
        data_rating = "iwanttv/analyticalbasetable/recommendationengine/percentilerating/2017/Sep/IWanTVREPercentilerating-20170901-v4.1.1.csv"

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

        alsWReg = MsftRecommendation() \
            .setUserCol(customerIndex.getOutputCol()) \
            .setRatingCol('rating') \
            .setItemCol(ratingsIndex.getOutputCol())

        paramGrid = ParamGridBuilder() \
            .addGrid(alsWReg.rank, [80]) \
            .addGrid(alsWReg.maxIter, [20]) \
            .addGrid(alsWReg.regParam, [0.1]) \
            .build()

        evaluator = MsftRecommendationEvaluator().setK(10).setSaveAll(True)

        tvRecommendationSplit = TrainValidRecommendSplit() \
            .setEstimator(alsWReg) \
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
