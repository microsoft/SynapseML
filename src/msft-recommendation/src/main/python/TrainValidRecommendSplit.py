# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import numpy as np
import pyspark
import pyspark.sql.functions as F
import sys
from mmlspark.TrainTestSplit import *
from mmlspark.TrainValidRecommendSplitModel import TrainValidRecommendSplitModel as tvmodel
from pyspark import keyword_only
from pyspark.ml import Estimator
from pyspark.ml.param import Params, Param, TypeConverters
from pyspark.ml.tuning import ValidatorParams
from pyspark.ml.util import *
from pyspark.sql import Window
from pyspark.sql.functions import col, expr

if sys.version >= '3':
    basestring = str


@inherit_doc
class TrainValidRecommendSplit(Estimator, ValidatorParams):
    trainRatio = Param(Params._dummy(), "trainRatio", "Param for ratio between train and\
         validation data. Must be between 0 and 1.", typeConverter=TypeConverters.toFloat)
    userCol = Param(Params._dummy(), "userCol",
                    "userCol: column name for user ids. Ids must be within the integer value range. (default: user)")
    ratingCol = Param(Params._dummy(), "ratingCol", "ratingCol: column name for ratings (default: rating)")

    itemCol = Param(Params._dummy(), "itemCol",
                    "itemCol: column name for item ids. Ids must be within the integer value range. (default: item)")

    def setTrainRatio(self, value):
        """
        Sets the value of :py:attr:`trainRatio`.
        """
        return self._set(trainRatio=value)

    def getTrainRatio(self):
        """
        Gets the value of trainRatio or its default value.
        """
        return self.getOrDefault(self.trainRatio)

    def setItemCol(self, value):
        """

        Args:

            itemCol (str): column name for item ids. Ids must be within the integer value range. (default: item)

        """
        self._set(itemCol=value)
        return self

    def getItemCol(self):
        """

        Returns:

            str: column name for item ids. Ids must be within the integer value range. (default: item)
        """
        return self.getOrDefault(self.itemCol)

    def setRatingCol(self, value):
        """

        Args:

            ratingCol (str): column name for ratings (default: rating)

        """
        self._set(ratingCol=value)
        return self

    def getRatingCol(self):
        """

        Returns:

            str: column name for ratings (default: rating)
        """
        return self.getOrDefault(self.ratingCol)

    def setUserCol(self, value):
        """

        Args:

            userCol (str): column name for user ids. Ids must be within the integer value range. (default: user)

        """
        self._set(userCol=value)
        return self

    def getUserCol(self):
        """

        Returns:

            str: column name for user ids. Ids must be within the integer value range. (default: user)
        """
        return self.getOrDefault(self.userCol)

    @keyword_only
    def __init__(self, estimator=None, estimatorParamMaps=None, evaluator=None, seed=None):
        """
        __init__(self, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3,\
                 seed=None)
        """
        super(TrainValidRecommendSplit, self).__init__()
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @keyword_only
    def setParams(self, estimator=None, estimatorParamMaps=None, evaluator=None, seed=None):
        """
        setParams(self, estimator=None, estimatorParamMaps=None, evaluator=None, numFolds=3,\
                  seed=None):
        Sets params for cross validator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def copy(self, extra=None):
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This copies creates a deep copy of
        the embedded paramMap, and copies the embedded and extra parameters over.

        :param extra: Extra parameters to copy to the new instance
        :return: Copy of this instance
        """
        if extra is None:
            extra = dict()
        newCV = Params.copy(self, extra)
        if self.isSet(self.estimator):
            newCV.setEstimator(self.getEstimator().copy(extra))
        # estimatorParamMaps remain the same
        if self.isSet(self.evaluator):
            newCV.setEvaluator(self.getEvaluator().copy(extra))
        return newCV

    def _create_model(self, java_model):
        model = tvmodel()
        model._java_obj = java_model
        model._transfer_params_from_java()
        return model

    def _fit(self, dataset):
        rating = self.getOrDefault(self.ratingCol)
        est = self.getOrDefault(self.estimator)
        eva = self.getOrDefault(self.evaluator)
        epm = self.getOrDefault(self.estimatorParamMaps)
        num_models = len(epm)
        metrics = [0.0] * num_models

        customerID = self.getOrDefault(self.userCol)
        itemID = self.getOrDefault(self.itemCol)

        pyspark.sql.DataFrame.min_rating_filter = TrainTestSplit.min_rating_filter
        pyspark.sql.DataFrame.stratified_split = TrainTestSplit.stratified_split

        temp_train, temp_validation = dataset \
            .dropDuplicates() \
            .withColumnRenamed(self.getUserCol(), 'customerID') \
            .withColumnRenamed(self.getItemCol(), 'itemID') \
            .min_rating_filter(min_rating=6, by_customer=True) \
            .stratified_split(min_rating=3, by_customer=True, fixed_test_sample=False, ratio=0.5)

        train = temp_train \
            .withColumnRenamed('customerID', self.getUserCol()) \
            .withColumnRenamed('itemID', self.getItemCol())

        validation = temp_validation \
            .withColumnRenamed('customerID', self.getUserCol()) \
            .withColumnRenamed('itemID', self.getItemCol())

        train.cache()
        validation.cache()
        # eva.setNItems(validation.rdd.map(lambda r: r[1]).distinct().count())
        print("fit starting")

        numModels = len(epm)

        def prepare_test_data(dataset, recs, k):

            userColumn = est.getUserCol()
            itemColumn = est.getItemCol()

            perUserRecommendedItemsDF = recs \
                .select(userColumn, "recommendations." + itemColumn) \
                .withColumnRenamed(itemColumn, 'prediction')

            windowSpec = Window.partitionBy(userColumn).orderBy(col(rating).desc())

            perUserActualItemsDF = dataset \
                .select(userColumn, itemColumn, rating, F.rank().over(windowSpec).alias('rank')) \
                .where('rank <= {0}'.format(k)) \
                .groupBy(userColumn) \
                .agg(expr('collect_list(' + itemColumn + ') as label')) \
                .select(userColumn, "label")

            joined_rec_actual = perUserRecommendedItemsDF \
                .join(perUserActualItemsDF, on=userColumn) \
                .drop(userColumn)

            return joined_rec_actual

        models = est.fit(train, epm)
        for j in range(numModels):
            model = models[j]  # models[j]
            recs = model.recommendForAllUsers(eva.getK())
            prepared_test = prepare_test_data(model.transform(validation), recs, eva.getK())
            metric = eva.evaluate(prepared_test)
            metrics[j] += metric
        if eva.isLargerBetter():
            bestIndex = np.argmax(metrics)
        else:
            bestIndex = np.argmin(metrics)

        best_model = est.fit(dataset, epm[bestIndex])
        return self._copyValues(tvmodel(best_model, metrics))

    # def getEstimatorParamMaps(self):
    #     # Load information from java_stage to the instance.
    #     estimator = JavaParams._from_java(self.getEstimator())
    #     epms = [estimator._transfer_param_map_from_java(epm)
    #             for epm in self.getEstimatorParamMaps()]
    #
    #     return epms

    # def setEstimatorParamMaps(self, value):
    #     """
    #     Args:
    #         estimatorParamMaps (object): param maps for the estimator
    #
    #     """
    #
    #     gateway = SparkContext._gateway
    #     cls = SparkContext._jvm.org.apache.spark.ml.param.ParamMap
    #
    #     java_epms = gateway.new_array(cls, len(value))
    #
    #     for idx, epm in enumerate(value):
    #         java_epms[idx] = self.getEstimator()._transfer_param_map_to_java(epm)
    #
    #     self._set(estimatorParamMaps=java_epms)
    #     return self

    # def fit(self, dataset):
    #     return self._call_java("fit", dataset)
    #
    # def _fit(self, dataset):
    #     rating = self.getOrDefault(self.ratingCol)
    #     est = self.getOrDefault(self.estimator)
    #     eva = self.getOrDefault(self.evaluator)
    #     epm = self.getOrDefault(self.estimatorParamMaps)
    #     num_models = len(epm)
    #     metrics = [0.0] * num_models
    #
    #     customerID = self.getOrDefault(self.userCol)
    #     itemID = self.getOrDefault(self.itemCol)
    #
    #     def filter_ratings(dataset):
    #         minRatingsU = 4
    #
    #         tmpDF = dataset \
    #             .groupBy(customerID) \
    #             .agg({itemID: "count"}) \
    #             .withColumnRenamed('count(' + itemID + ')', 'nitems') \
    #             .where(col('nitems') >= minRatingsU)
    #
    #         minRatingsI = 4
    #
    #         tmp_df2 = dataset \
    #             .groupBy(itemID) \
    #             .agg({customerID: "count"}) \
    #             .withColumnRenamed('count(' + customerID + ')', 'ncustomers') \
    #             .where(col('ncustomers') >= minRatingsI)
    #
    #         input_df = tmp_df2 \
    #             .join(dataset, itemID) \
    #             .drop('ncustomers') \
    #             .join(tmpDF, customerID) \
    #             .drop('nitems')
    #
    #         return input_df
    #
    #     print("filter dataset")
    #     filtered_dataset = filter_ratings(dataset.dropDuplicates())
    #
    #     def split_df(input_df):
    #         # Stratified sampling by item into a train and test data set
    #         nusers_by_item = input_df \
    #             .groupBy(itemID) \
    #             .agg({customerID: "count"}) \
    #             .withColumnRenamed('count(' + customerID + ')', 'nusers').rdd
    #
    #         perm_indices = nusers_by_item.map(lambda r: (r[0], np.random.permutation(r[1]), r[1]))
    #
    #         RATIO = 0.75
    #         tr_idx = perm_indices.map(lambda r: (r[0], r[1][: int(round(r[2] * RATIO))]))
    #         test_idx = perm_indices.map(lambda r: (r[0], r[1][int(round(r[2] * RATIO)):]))
    #
    #         tr_by_item = input_df.rdd \
    #             .groupBy(lambda r: r[1]) \
    #             .join(tr_idx) \
    #             .flatMap(lambda r: [x for x in r[1][0]][: len(r[1][1])])
    #
    #         test_by_item = input_df.rdd \
    #             .groupBy(lambda r: r[1]) \
    #             .join(test_idx) \
    #             .flatMap(lambda r: [x for x in r[1][0]][: len(r[1][1])])
    #
    #         train_temp = tr_by_item \
    #             .map(lambda r: tuple(r)) \
    #             .toDF(schema=[customerID, itemID, customerID + 'Org', itemID + 'Org', rating])
    #
    #         test_temp = test_by_item \
    #             .map(lambda r: tuple(r)) \
    #             .toDF(schema=[customerID, itemID, customerID + 'Org', itemID + 'Org', rating])
    #
    #         return [train_temp, test_temp]
    #
    #     print("split dataset")
    #     # [train, validation] = self._call_java("splitDF", filtered_dataset)
    #     splitter = TrainTestSplit(dataset.dropDuplicates(), by_customer=True, customerCol=customerID, itemCol=itemID)
    #
    #     train, validation = splitter.stratified_split(3, fixed_test_sample=False, ratio=0.5)
    #     # [train, validation] = split_df(filtered_dataset)
    #     train.cache()
    #     validation.cache()
    #     eva.setNumberItems(validation.rdd.map(lambda r: r[1]).distinct().count())
    #     print("fit starting")
    #     models = est.fit(train, epm)
    #     train.unpersist()
    #
    #     def prepare_test_data(dataset, recs, k):
    #
    #         userColumn = est.getUserCol()
    #         itemColumn = est.getItemCol()
    #
    #         perUserRecommendedItemsDF = recs \
    #             .select(userColumn, "recommendations." + itemColumn) \
    #             .withColumnRenamed(itemColumn, 'prediction')
    #
    #         windowSpec = Window.partitionBy(userColumn).orderBy(col(rating).desc())
    #
    #         perUserActualItemsDF = dataset \
    #             .select(userColumn, itemColumn, rating, F.rank().over(windowSpec).alias('rank')) \
    #             .where('rank <= {0}'.format(k)) \
    #             .groupBy(userColumn) \
    #             .agg(expr('collect_list(' + itemColumn + ') as label')) \
    #             .select(userColumn, "label")
    #
    #         joined_rec_actual = perUserRecommendedItemsDF \
    #             .join(perUserActualItemsDF, on=userColumn) \
    #             .drop(userColumn)
    #
    #         return joined_rec_actual
    #
    #     def cast_model(model):
    #         recs = model._call_java("recommendForAllUsers", eva.getK())
    #         prepared_test = prepare_test_data(model.transform(validation), recs, eva.getK())
    #         metric = eva.evaluate(prepared_test)
    #         return metric
    #
    #     for j in range(num_models):
    #         metrics[j] += cast_model(models[j])
    #     validation.unpersist()
    #
    #     if eva.isLargerBetter():
    #         best_index = np.argmax(metrics)
    #     else:
    #         best_index = np.argmin(metrics)
    #     best_model = est.fit(dataset, epm[best_index])
    #     return self._copyValues(tvmodel(best_model=best_model, validationMetrics=metrics))
