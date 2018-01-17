# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import sys

if sys.version >= '3':
    basestring = str

import numpy as np

from pyspark.sql import Window

from pyspark.ml.param.shared import *
from pyspark import keyword_only
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaEstimator, JavaModel
from pyspark.ml.common import inherit_doc
from pyspark.ml.util import *
from pyspark.sql.functions import col, expr
import pyspark.sql.functions as F

from mmlspark.Utils import *


@inherit_doc
class TrainValidRecommendSplit(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaEstimator):
    """
    Args:

        alpha (double): alpha for implicit preference (default: 1.0)
        checkpointInterval (int): set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations (default: 10)
        coldStartStrategy (str): strategy for dealing with unknown or new users/items at prediction time. This may be useful in cross-validation or production scenarios, for handling user/item ids the model has not seen in the training data. Supported values: nan,drop. (default: nan)
        estimator (object): estimator for selection
        estimatorParamMaps (object): param maps for the estimator
        evaluator (object): evaluator used to select hyper-parameters that maximize the validated metric
        finalStorageLevel (str): StorageLevel for ALS model factors. (default: MEMORY_AND_DISK)
        implicitPrefs (bool): whether to use implicit preference (default: false)
        intermediateStorageLevel (str): StorageLevel for intermediate datasets. Cannot be 'NONE'. (default: MEMORY_AND_DISK)
        itemCol (str): column name for item ids. Ids must be within the integer value range. (default: item)
        maxIter (int): maximum number of iterations (>= 0) (default: 10)
        minRatingsI (int): min ratings for items > 0 (default: 1)
        minRatingsU (int): min ratings for users > 0 (default: 1)
        nonnegative (bool): whether to use nonnegative constraint for least squares (default: false)
        numItemBlocks (int): number of item blocks (default: 10)
        numUserBlocks (int): number of user blocks (default: 10)
        predictionCol (str): prediction column name (default: prediction)
        rank (int): rank of the factorization (default: 10)
        ratingCol (str): column name for ratings (default: rating)
        regParam (double): regularization parameter (>= 0) (default: 0.1)
        seed (long): random seed (default: 1893783045)
        trainRatio (double): ratio between training set and validation set (>= 0 && <= 1) (default: 0.75)
        userCol (str): column name for user ids. Ids must be within the integer value range. (default: user)
    """

    @keyword_only
    def __init__(self, alpha=1.0, checkpointInterval=10, coldStartStrategy="nan", estimator=None,
                 estimatorParamMaps=None, evaluator=None, finalStorageLevel="MEMORY_AND_DISK", implicitPrefs=False,
                 intermediateStorageLevel="MEMORY_AND_DISK", itemCol="item", maxIter=10, minRatingsI=1, minRatingsU=1,
                 nonnegative=False, numItemBlocks=10, numUserBlocks=10, predictionCol="prediction", rank=10,
                 ratingCol="rating", regParam=0.1, seed=1893783045, trainRatio=0.75, userCol="user"):
        super(TrainValidRecommendSplit, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.TrainValidRecommendSplit")
        self.alpha = Param(self, "alpha", "alpha: alpha for implicit preference (default: 1.0)")
        self._setDefault(alpha=1.0)
        self.checkpointInterval = Param(self, "checkpointInterval",
                                        "checkpointInterval: set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations (default: 10)")
        self._setDefault(checkpointInterval=10)
        self.coldStartStrategy = Param(self, "coldStartStrategy",
                                       "coldStartStrategy: strategy for dealing with unknown or new users/items at prediction time. This may be useful in cross-validation or production scenarios, for handling user/item ids the model has not seen in the training data. Supported values: nan,drop. (default: nan)")
        self._setDefault(coldStartStrategy="nan")
        self.estimator = Param(self, "estimator", "estimator: estimator for selection")
        self.estimatorParamMaps = Param(self, "estimatorParamMaps", "estimatorParamMaps: param maps for the estimator")
        self.evaluator = Param(self, "evaluator",
                               "evaluator: evaluator used to select hyper-parameters that maximize the validated metric")
        self.finalStorageLevel = Param(self, "finalStorageLevel",
                                       "finalStorageLevel: StorageLevel for ALS model factors. (default: MEMORY_AND_DISK)")
        self._setDefault(finalStorageLevel="MEMORY_AND_DISK")
        self.implicitPrefs = Param(self, "implicitPrefs",
                                   "implicitPrefs: whether to use implicit preference (default: false)")
        self._setDefault(implicitPrefs=False)
        self.intermediateStorageLevel = Param(self, "intermediateStorageLevel",
                                              "intermediateStorageLevel: StorageLevel for intermediate datasets. Cannot be 'NONE'. (default: MEMORY_AND_DISK)")
        self._setDefault(intermediateStorageLevel="MEMORY_AND_DISK")
        self.itemCol = Param(self, "itemCol",
                             "itemCol: column name for item ids. Ids must be within the integer value range. (default: item)")
        self._setDefault(itemCol="item")
        self.maxIter = Param(self, "maxIter", "maxIter: maximum number of iterations (>= 0) (default: 10)")
        self._setDefault(maxIter=10)
        self.minRatingsI = Param(self, "minRatingsI", "minRatingsI: min ratings for items > 0 (default: 1)")
        self._setDefault(minRatingsI=1)
        self.minRatingsU = Param(self, "minRatingsU", "minRatingsU: min ratings for users > 0 (default: 1)")
        self._setDefault(minRatingsU=1)
        self.nonnegative = Param(self, "nonnegative",
                                 "nonnegative: whether to use nonnegative constraint for least squares (default: false)")
        self._setDefault(nonnegative=False)
        self.numItemBlocks = Param(self, "numItemBlocks", "numItemBlocks: number of item blocks (default: 10)")
        self._setDefault(numItemBlocks=10)
        self.numUserBlocks = Param(self, "numUserBlocks", "numUserBlocks: number of user blocks (default: 10)")
        self._setDefault(numUserBlocks=10)
        self.predictionCol = Param(self, "predictionCol", "predictionCol: prediction column name (default: prediction)")
        self._setDefault(predictionCol="prediction")
        self.rank = Param(self, "rank", "rank: rank of the factorization (default: 10)")
        self._setDefault(rank=10)
        self.ratingCol = Param(self, "ratingCol", "ratingCol: column name for ratings (default: rating)")
        self._setDefault(ratingCol="rating")
        self.regParam = Param(self, "regParam", "regParam: regularization parameter (>= 0) (default: 0.1)")
        self._setDefault(regParam=0.1)
        self.seed = Param(self, "seed", "seed: random seed (default: 1893783045)")
        self._setDefault(seed=1893783045)
        self.trainRatio = Param(self, "trainRatio",
                                "trainRatio: ratio between training set and validation set (>= 0 && <= 1) (default: 0.75)")
        self._setDefault(trainRatio=0.75)
        self.userCol = Param(self, "userCol",
                             "userCol: column name for user ids. Ids must be within the integer value range. (default: user)")
        self._setDefault(userCol="user")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, alpha=1.0, checkpointInterval=10, coldStartStrategy="nan", estimator=None,
                  estimatorParamMaps=None, evaluator=None, finalStorageLevel="MEMORY_AND_DISK", implicitPrefs=False,
                  intermediateStorageLevel="MEMORY_AND_DISK", itemCol="item", maxIter=10, minRatingsI=1, minRatingsU=1,
                  nonnegative=False, numItemBlocks=10, numUserBlocks=10, predictionCol="prediction", rank=10,
                  ratingCol="rating", regParam=0.1, seed=1893783045, trainRatio=0.75, userCol="user"):
        """
        Set the (keyword only) parameters

        Args:

            alpha (double): alpha for implicit preference (default: 1.0)
            checkpointInterval (int): set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations (default: 10)
            coldStartStrategy (str): strategy for dealing with unknown or new users/items at prediction time. This may be useful in cross-validation or production scenarios, for handling user/item ids the model has not seen in the training data. Supported values: nan,drop. (default: nan)
            estimator (object): estimator for selection
            estimatorParamMaps (object): param maps for the estimator
            evaluator (object): evaluator used to select hyper-parameters that maximize the validated metric
            finalStorageLevel (str): StorageLevel for ALS model factors. (default: MEMORY_AND_DISK)
            implicitPrefs (bool): whether to use implicit preference (default: false)
            intermediateStorageLevel (str): StorageLevel for intermediate datasets. Cannot be 'NONE'. (default: MEMORY_AND_DISK)
            itemCol (str): column name for item ids. Ids must be within the integer value range. (default: item)
            maxIter (int): maximum number of iterations (>= 0) (default: 10)
            minRatingsI (int): min ratings for items > 0 (default: 1)
            minRatingsU (int): min ratings for users > 0 (default: 1)
            nonnegative (bool): whether to use nonnegative constraint for least squares (default: false)
            numItemBlocks (int): number of item blocks (default: 10)
            numUserBlocks (int): number of user blocks (default: 10)
            predictionCol (str): prediction column name (default: prediction)
            rank (int): rank of the factorization (default: 10)
            ratingCol (str): column name for ratings (default: rating)
            regParam (double): regularization parameter (>= 0) (default: 0.1)
            seed (long): random seed (default: 1893783045)
            trainRatio (double): ratio between training set and validation set (>= 0 && <= 1) (default: 0.75)
            userCol (str): column name for user ids. Ids must be within the integer value range. (default: user)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setAlpha(self, value):
        """
        Args:
            alpha (double): alpha for implicit preference (default: 1.0)
        """
        self._set(alpha=value)
        return self

    def getAlpha(self):
        """
        Returns:
            double: alpha for implicit preference (default: 1.0)
        """
        return self.getOrDefault(self.alpha)

    def setCheckpointInterval(self, value):
        """
        Args:
            checkpointInterval (int): set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations (default: 10)
        """
        self._set(checkpointInterval=value)
        return self

    def getCheckpointInterval(self):
        """
        Returns:
            int: set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations (default: 10)
        """
        return self.getOrDefault(self.checkpointInterval)

    def setColdStartStrategy(self, value):
        """
        Args:
            coldStartStrategy (str): strategy for dealing with unknown or new users/items at prediction time. This may be useful in cross-validation or production scenarios, for handling user/item ids the model has not seen in the training data. Supported values: nan,drop. (default: nan)
        """
        self._set(coldStartStrategy=value)
        return self

    def getColdStartStrategy(self):
        """
        Returns:
            str: strategy for dealing with unknown or new users/items at prediction time. This may be useful in cross-validation or production scenarios, for handling user/item ids the model has not seen in the training data. Supported values: nan,drop. (default: nan)
        """
        return self.getOrDefault(self.coldStartStrategy)

    def setEstimator(self, value):
        """
        Args:
            estimator (object): estimator for selection        """
        self._set(estimator=value)
        return self

    def getEstimator(self):
        """
        Returns:
            object: estimator for selection
        """
        return self.getOrDefault(self.estimator)

    def setEstimatorParamMaps(self, value):
        """
        Args:
            estimatorParamMaps (object): param maps for the estimator
        """
        self._set(estimatorParamMaps=value)
        return self

    def getEstimatorParamMaps(self):
        """
        Returns:
            object: param maps for the estimator
        """
        return self.getOrDefault(self.estimatorParamMaps)

    def setEvaluator(self, value):
        """
        Args:
            evaluator (object): evaluator used to select hyper-parameters that maximize the validated metric
        """
        self._set(evaluator=value)
        return self

    def getEvaluator(self):
        """
        Returns:
            object: evaluator used to select hyper-parameters that maximize the validated metric
        """
        return self.getOrDefault(self.evaluator)

    def setFinalStorageLevel(self, value):
        """
        Args:
            finalStorageLevel (str): StorageLevel for ALS model factors. (default: MEMORY_AND_DISK)
        """
        self._set(finalStorageLevel=value)
        return self

    def getFinalStorageLevel(self):
        """
        Returns:
            str: StorageLevel for ALS model factors. (default: MEMORY_AND_DISK)
        """
        return self.getOrDefault(self.finalStorageLevel)

    def setImplicitPrefs(self, value):
        """
        Args:
            implicitPrefs (bool): whether to use implicit preference (default: false)
        """
        self._set(implicitPrefs=value)
        return self

    def getImplicitPrefs(self):
        """
        Returns:
            bool: whether to use implicit preference (default: false)
        """
        return self.getOrDefault(self.implicitPrefs)

    def setIntermediateStorageLevel(self, value):
        """
        Args:
            intermediateStorageLevel (str): StorageLevel for intermediate datasets. Cannot be 'NONE'. (default: MEMORY_AND_DISK)
        """
        self._set(intermediateStorageLevel=value)
        return self

    def getIntermediateStorageLevel(self):
        """
        Returns:str: StorageLevel for intermediate datasets. Cannot be 'NONE'. (default: MEMORY_AND_DISK)
        """
        return self.getOrDefault(self.intermediateStorageLevel)

    def setItemCol(self, value):
        """
        Args:
            itemCol (str): column name for item ids. Ids must be within the integer value range. (default: item)
        """
        self._set(itemCol=value)
        return self

    def getItemCol(self):
        """
        Returns:            str: column name for item ids. Ids must be within the integer value range. (default: item)
        """
        return self.getOrDefault(self.itemCol)

    def setMaxIter(self, value):
        """
        Args:
            maxIter (int): maximum number of iterations (>= 0) (default: 10)
        """
        self._set(maxIter=value)
        return self

    def getMaxIter(self):
        """
        Returns:
            int: maximum number of iterations (>= 0) (default: 10)
        """
        return self.getOrDefault(self.maxIter)

    def setMinRatingsI(self, value):
        """
        Args:
            minRatingsI (int): min ratings for items > 0 (default: 1)
        """
        self._set(minRatingsI=value)
        return self

    def getMinRatingsI(self):
        """
        Returns:
            int: min ratings for items > 0 (default: 1)
        """
        return self.getOrDefault(self.minRatingsI)

    def setMinRatingsU(self, value):
        """
        Args:
            minRatingsU (int): min ratings for users > 0 (default: 1)
        """
        self._set(minRatingsU=value)
        return self

    def getMinRatingsU(self):
        """
        Returns:
            int: min ratings for users > 0 (default: 1)
        """
        return self.getOrDefault(self.minRatingsU)

    def setNonnegative(self, value):
        """
        Args:
            nonnegative (bool): whether to use nonnegative constraint for least squares (default: false)
        """
        self._set(nonnegative=value)
        return self

    def getNonnegative(self):
        """
        Returns:
            bool: whether to use nonnegative constraint for least squares (default: false)
        """
        return self.getOrDefault(self.nonnegative)

    def setNumItemBlocks(self, value):
        """
        Args:
            numItemBlocks (int): number of item blocks (default: 10)
        """
        self._set(numItemBlocks=value)
        return self

    def getNumItemBlocks(self):
        """
        Returns:
            int: number of item blocks (default: 10)
        """
        return self.getOrDefault(self.numItemBlocks)

    def setNumUserBlocks(self, value):
        """
        Args:
            numUserBlocks (int): number of user blocks (default: 10)
        """
        self._set(numUserBlocks=value)
        return self

    def getNumUserBlocks(self):
        """
        Returns:
            int: number of user blocks (default: 10)
        """
        return self.getOrDefault(self.numUserBlocks)

    def setPredictionCol(self, value):
        """
        Args:
            predictionCol (str): prediction column name (default: prediction)
        """
        self._set(predictionCol=value)
        return self

    def getPredictionCol(self):
        """
        Returns:
            str: prediction column name (default: prediction)
        """
        return self.getOrDefault(self.predictionCol)

    def setRank(self, value):
        """
        Args:
            rank (int): rank of the factorization (default: 10)
        """
        self._set(rank=value)
        return self

    def getRank(self):
        """
        Returns:
            int: rank of the factorization (default: 10)
        """
        return self.getOrDefault(self.rank)

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

    def setRegParam(self, value):
        """
        Args:
            regParam (double): regularization parameter (>= 0) (default: 0.1)
        """
        self._set(regParam=value)
        return self

    def getRegParam(self):
        """
        Returns:
            double: regularization parameter (>= 0) (default: 0.1)
        """
        return self.getOrDefault(self.regParam)

    def setSeed(self, value):
        """
        Args:
            seed (long): random seed (default: 1893783045)
        """
        self._set(seed=value)
        return self

    def getSeed(self):
        """
        Returns:
            long: random seed (default: 1893783045)
        """
        return self.getOrDefault(self.seed)

    def setTrainRatio(self, value):
        """
        Args:
            trainRatio (double): ratio between training set and validation set (>= 0 && <= 1) (default: 0.75)
        """
        self._set(trainRatio=value)
        return self

    def getTrainRatio(self):
        """
        Returns:
            double: ratio between training set and validation set (>= 0 && <= 1) (default: 0.75)
        """
        return self.getOrDefault(self.trainRatio)

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

    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.TrainValidRecommendSplit"

    @staticmethod
    def _from_java(java_stage):
        module_name = TrainValidRecommendSplit.__module__
        module_name = module_name.rsplit(".", 1)[0] + ".TrainValidRecommendSplit"
        return from_java(java_stage, module_name)

    def _create_model(self, java_model):
        return TrainValidRecommendSplitModel(java_model)

    def _fit(self, dataset):
        rating = self.getOrDefault(self.ratingCol)
        est = self.getOrDefault(self.estimator)
        eva = self.getOrDefault(self.evaluator)
        epm = self.getOrDefault(self.estimatorParamMaps)
        num_models = len(epm)
        metrics = [0.0] * num_models

        customerID = self.getOrDefault(self.userCol)
        itemID = self.getOrDefault(self.itemCol)

        def filter_ratings(dataset):
            minRatingsU = 4

            tmpDF = dataset \
                .groupBy(customerID) \
                .agg({itemID: "count"}) \
                .withColumnRenamed('count(' + itemID + ')', 'nitems') \
                .where(col('nitems') >= minRatingsU)

            minRatingsI = 4

            tmp_df2 = dataset \
                .groupBy(itemID) \
                .agg({customerID: "count"}) \
                .withColumnRenamed('count(' + customerID + ')', 'ncustomers') \
                .where(col('ncustomers') >= minRatingsI)

            input_df = tmp_df2 \
                .join(dataset, itemID) \
                .drop('ncustomers') \
                .join(tmpDF, customerID) \
                .drop('nitems')

            return input_df

        filtered_dataset = filter_ratings(dataset.dropDuplicates())

        def split_df(input_df):
            # Stratified sampling by item into a train and test data set
            nusers_by_item = input_df \
                .groupBy(itemID) \
                .agg({customerID: "count"}) \
                .withColumnRenamed('count(' + customerID + ')', 'nusers').rdd

            perm_indices = nusers_by_item.map(lambda r: (r[0], np.random.permutation(r[1]), r[1]))

            RATIO = 0.75
            tr_idx = perm_indices.map(lambda r: (r[0], r[1][: int(round(r[2] * RATIO))]))
            test_idx = perm_indices.map(lambda r: (r[0], r[1][int(round(r[2] * RATIO)):]))

            tr_by_item = input_df.rdd \
                .groupBy(lambda r: r[1]) \
                .join(tr_idx) \
                .flatMap(lambda r: [x for x in r[1][0]][: len(r[1][1])])

            test_by_item = input_df.rdd \
                .groupBy(lambda r: r[1]) \
                .join(test_idx) \
                .flatMap(lambda r: [x for x in r[1][0]][: len(r[1][1])])

            train_temp = tr_by_item \
                .map(lambda r: tuple(r)) \
                .toDF(schema=[customerID, itemID, customerID + 'Org', itemID + 'Org', rating])

            test_temp = test_by_item \
                .map(lambda r: tuple(r)) \
                .toDF(schema=[customerID, itemID, customerID + 'Org', itemID + 'Org', rating])

            return [train_temp, test_temp]

        [train, validation] = split_df(filtered_dataset)
        train.cache()
        validation.cache()

        models = est.fit(train, epm)
        train.unpersist()

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

        def cast_model(model):
            recs = model._call_java("recommendForAllUsers", eva.getK())
            prepared_test = prepare_test_data(model.transform(validation), recs, eva.getK())
            metric = eva.evaluate(prepared_test)
            return metric

        for j in range(num_models):
            metrics[j] += cast_model(models[j])
        validation.unpersist()

        if eva.isLargerBetter():
            best_index = np.argmax(metrics)
        else:
            best_index = np.argmin(metrics)
        best_model = est.fit(dataset, epm[best_index])
        return self._copyValues(TrainValidRecommendSplitModel(best_model, metrics))


class TrainValidRecommendSplitModel(ComplexParamsMixin, JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`TrainValidRecommendSplit`.

    This class is left empty on purpose.
    All necessary methods are exposed through inheritance.
    """

    def __init__(self, bestModel, validationMetrics=[]):
        super(TrainValidRecommendSplitModel, self).__init__()
        #: best model from cross validation
        self.bestModel = bestModel
        #: evaluated validation metrics
        self.validationMetrics = validationMetrics

    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.TrainValidRecommendSplitModel"

    @staticmethod
    def _from_java(java_stage):
        module_name = TrainValidRecommendSplitModel.__module__
        module_name = module_name.rsplit(".", 1)[0] + ".TrainValidRecommendSplitModel"
        return from_java(java_stage, module_name)

    def copy(self, extra=None):
        """
        Creates a copy of this instance with a randomly generated uid
        and some extra params. This copies the underlying bestModel,
        creates a deep copy of the embedded paramMap, and
        copies the embedded and extra parameters over.
        And, this creates a shallow copy of the validationMetrics.

        :param extra: Extra parameters to copy to the new instance
        :return: Copy of this instance
        """
        if extra is None:
            extra = dict()
        bestModel = self.bestModel.copy(extra)
        validationMetrics = list(self.validationMetrics)
        return TrainValidRecommendSplitModel(bestModel, validationMetrics)
