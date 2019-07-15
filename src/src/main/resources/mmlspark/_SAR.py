# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.


import sys
if sys.version >= '3':
    basestring = str

from pyspark.ml.param.shared import *
from pyspark import keyword_only
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaTransformer, JavaEstimator, JavaModel
from pyspark.ml.common import inherit_doc
from mmlspark.Utils import *

@inherit_doc
class _SAR(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaEstimator):
    """


    Args:

        activityTimeFormat (str): Time format for events, default: yyyy/MM/dd'T'h:mm:ss (default: yyyy/MM/dd'T'h:mm:ss)
        alpha (double): alpha for implicit preference (default: 1.0)
        checkpointInterval (int): set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext (default: 10)
        coldStartStrategy (str): strategy for dealing with unknown or new users/items at prediction time. This may be useful in cross-validation or production scenarios, for handling user/item ids the model has not seen in the training data. Supported values: nan,drop. (default: nan)
        finalStorageLevel (str): StorageLevel for ALS model factors. (default: MEMORY_AND_DISK)
        implicitPrefs (bool): whether to use implicit preference (default: false)
        intermediateStorageLevel (str): StorageLevel for intermediate datasets. Cannot be 'NONE'. (default: MEMORY_AND_DISK)
        itemCol (str): column name for item ids. Ids must be within the integer value range. (default: item)
        maxIter (int): maximum number of iterations (>= 0) (default: 10)
        nonnegative (bool): whether to use nonnegative constraint for least squares (default: false)
        numItemBlocks (int): number of item blocks (default: 10)
        numUserBlocks (int): number of user blocks (default: 10)
        predictionCol (str): prediction column name (default: prediction)
        rank (int): rank of the factorization (default: 10)
        ratingCol (str): column name for ratings (default: rating)
        regParam (double): regularization parameter (>= 0) (default: 0.1)
        seed (long): random seed (default: 1766449073)
        similarityFunction (str): Defines the similarity function to be used by the model. Lift favors serendipity, Co-occurrence favors predictability, and Jaccard is a nice compromise between the two. (default: jaccard)
        startTime (str): Set time custom now time if using historical data
        startTimeFormat (str): Format for start time (default: EEE MMM dd HH:mm:ss Z yyyy)
        supportThreshold (int): Minimum number of ratings per item (default: 4)
        timeCol (str): Time of activity (default: time)
        timeDecayCoeff (int): Use to scale time decay coeff to different half life dur (default: 30)
        userCol (str): column name for user ids. Ids must be within the integer value range. (default: user)
    """

    @keyword_only
    def __init__(self, activityTimeFormat="yyyy/MM/dd'T'h:mm:ss", alpha=1.0, checkpointInterval=10, coldStartStrategy="nan", finalStorageLevel="MEMORY_AND_DISK", implicitPrefs=False, intermediateStorageLevel="MEMORY_AND_DISK", itemCol="item", maxIter=10, nonnegative=False, numItemBlocks=10, numUserBlocks=10, predictionCol="prediction", rank=10, ratingCol="rating", regParam=0.1, seed=1766449073, similarityFunction="jaccard", startTime=None, startTimeFormat="EEE MMM dd HH:mm:ss Z yyyy", supportThreshold=4, timeCol="time", timeDecayCoeff=30, userCol="user"):
        super(_SAR, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.SAR")
        self.activityTimeFormat = Param(self, "activityTimeFormat", "activityTimeFormat: Time format for events, default: yyyy/MM/dd'T'h:mm:ss (default: yyyy/MM/dd'T'h:mm:ss)")
        self._setDefault(activityTimeFormat="yyyy/MM/dd'T'h:mm:ss")
        self.alpha = Param(self, "alpha", "alpha: alpha for implicit preference (default: 1.0)")
        self._setDefault(alpha=1.0)
        self.checkpointInterval = Param(self, "checkpointInterval", "checkpointInterval: set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext (default: 10)")
        self._setDefault(checkpointInterval=10)
        self.coldStartStrategy = Param(self, "coldStartStrategy", "coldStartStrategy: strategy for dealing with unknown or new users/items at prediction time. This may be useful in cross-validation or production scenarios, for handling user/item ids the model has not seen in the training data. Supported values: nan,drop. (default: nan)")
        self._setDefault(coldStartStrategy="nan")
        self.finalStorageLevel = Param(self, "finalStorageLevel", "finalStorageLevel: StorageLevel for ALS model factors. (default: MEMORY_AND_DISK)")
        self._setDefault(finalStorageLevel="MEMORY_AND_DISK")
        self.implicitPrefs = Param(self, "implicitPrefs", "implicitPrefs: whether to use implicit preference (default: false)")
        self._setDefault(implicitPrefs=False)
        self.intermediateStorageLevel = Param(self, "intermediateStorageLevel", "intermediateStorageLevel: StorageLevel for intermediate datasets. Cannot be 'NONE'. (default: MEMORY_AND_DISK)")
        self._setDefault(intermediateStorageLevel="MEMORY_AND_DISK")
        self.itemCol = Param(self, "itemCol", "itemCol: column name for item ids. Ids must be within the integer value range. (default: item)")
        self._setDefault(itemCol="item")
        self.maxIter = Param(self, "maxIter", "maxIter: maximum number of iterations (>= 0) (default: 10)")
        self._setDefault(maxIter=10)
        self.nonnegative = Param(self, "nonnegative", "nonnegative: whether to use nonnegative constraint for least squares (default: false)")
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
        self.seed = Param(self, "seed", "seed: random seed (default: 1766449073)")
        self._setDefault(seed=1766449073)
        self.similarityFunction = Param(self, "similarityFunction", "similarityFunction: Defines the similarity function to be used by the model. Lift favors serendipity, Co-occurrence favors predictability, and Jaccard is a nice compromise between the two. (default: jaccard)")
        self._setDefault(similarityFunction="jaccard")
        self.startTime = Param(self, "startTime", "startTime: Set time custom now time if using historical data")
        self.startTimeFormat = Param(self, "startTimeFormat", "startTimeFormat: Format for start time (default: EEE MMM dd HH:mm:ss Z yyyy)")
        self._setDefault(startTimeFormat="EEE MMM dd HH:mm:ss Z yyyy")
        self.supportThreshold = Param(self, "supportThreshold", "supportThreshold: Minimum number of ratings per item (default: 4)")
        self._setDefault(supportThreshold=4)
        self.timeCol = Param(self, "timeCol", "timeCol: Time of activity (default: time)")
        self._setDefault(timeCol="time")
        self.timeDecayCoeff = Param(self, "timeDecayCoeff", "timeDecayCoeff: Use to scale time decay coeff to different half life dur (default: 30)")
        self._setDefault(timeDecayCoeff=30)
        self.userCol = Param(self, "userCol", "userCol: column name for user ids. Ids must be within the integer value range. (default: user)")
        self._setDefault(userCol="user")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, activityTimeFormat="yyyy/MM/dd'T'h:mm:ss", alpha=1.0, checkpointInterval=10, coldStartStrategy="nan", finalStorageLevel="MEMORY_AND_DISK", implicitPrefs=False, intermediateStorageLevel="MEMORY_AND_DISK", itemCol="item", maxIter=10, nonnegative=False, numItemBlocks=10, numUserBlocks=10, predictionCol="prediction", rank=10, ratingCol="rating", regParam=0.1, seed=1766449073, similarityFunction="jaccard", startTime=None, startTimeFormat="EEE MMM dd HH:mm:ss Z yyyy", supportThreshold=4, timeCol="time", timeDecayCoeff=30, userCol="user"):
        """
        Set the (keyword only) parameters

        Args:

            activityTimeFormat (str): Time format for events, default: yyyy/MM/dd'T'h:mm:ss (default: yyyy/MM/dd'T'h:mm:ss)
            alpha (double): alpha for implicit preference (default: 1.0)
            checkpointInterval (int): set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext (default: 10)
            coldStartStrategy (str): strategy for dealing with unknown or new users/items at prediction time. This may be useful in cross-validation or production scenarios, for handling user/item ids the model has not seen in the training data. Supported values: nan,drop. (default: nan)
            finalStorageLevel (str): StorageLevel for ALS model factors. (default: MEMORY_AND_DISK)
            implicitPrefs (bool): whether to use implicit preference (default: false)
            intermediateStorageLevel (str): StorageLevel for intermediate datasets. Cannot be 'NONE'. (default: MEMORY_AND_DISK)
            itemCol (str): column name for item ids. Ids must be within the integer value range. (default: item)
            maxIter (int): maximum number of iterations (>= 0) (default: 10)
            nonnegative (bool): whether to use nonnegative constraint for least squares (default: false)
            numItemBlocks (int): number of item blocks (default: 10)
            numUserBlocks (int): number of user blocks (default: 10)
            predictionCol (str): prediction column name (default: prediction)
            rank (int): rank of the factorization (default: 10)
            ratingCol (str): column name for ratings (default: rating)
            regParam (double): regularization parameter (>= 0) (default: 0.1)
            seed (long): random seed (default: 1766449073)
            similarityFunction (str): Defines the similarity function to be used by the model. Lift favors serendipity, Co-occurrence favors predictability, and Jaccard is a nice compromise between the two. (default: jaccard)
            startTime (str): Set time custom now time if using historical data
            startTimeFormat (str): Format for start time (default: EEE MMM dd HH:mm:ss Z yyyy)
            supportThreshold (int): Minimum number of ratings per item (default: 4)
            timeCol (str): Time of activity (default: time)
            timeDecayCoeff (int): Use to scale time decay coeff to different half life dur (default: 30)
            userCol (str): column name for user ids. Ids must be within the integer value range. (default: user)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setActivityTimeFormat(self, value):
        """

        Args:

            activityTimeFormat (str): Time format for events, default: yyyy/MM/dd'T'h:mm:ss (default: yyyy/MM/dd'T'h:mm:ss)

        """
        self._set(activityTimeFormat=value)
        return self


    def getActivityTimeFormat(self):
        """

        Returns:

            str: Time format for events, default: yyyy/MM/dd'T'h:mm:ss (default: yyyy/MM/dd'T'h:mm:ss)
        """
        return self.getOrDefault(self.activityTimeFormat)


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

            checkpointInterval (int): set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext (default: 10)

        """
        self._set(checkpointInterval=value)
        return self


    def getCheckpointInterval(self):
        """

        Returns:

            int: set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext (default: 10)
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

        Returns:

            str: StorageLevel for intermediate datasets. Cannot be 'NONE'. (default: MEMORY_AND_DISK)
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

        Returns:

            str: column name for item ids. Ids must be within the integer value range. (default: item)
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

            seed (long): random seed (default: 1766449073)

        """
        self._set(seed=value)
        return self


    def getSeed(self):
        """

        Returns:

            long: random seed (default: 1766449073)
        """
        return self.getOrDefault(self.seed)


    def setSimilarityFunction(self, value):
        """

        Args:

            similarityFunction (str): Defines the similarity function to be used by the model. Lift favors serendipity, Co-occurrence favors predictability, and Jaccard is a nice compromise between the two. (default: jaccard)

        """
        self._set(similarityFunction=value)
        return self


    def getSimilarityFunction(self):
        """

        Returns:

            str: Defines the similarity function to be used by the model. Lift favors serendipity, Co-occurrence favors predictability, and Jaccard is a nice compromise between the two. (default: jaccard)
        """
        return self.getOrDefault(self.similarityFunction)


    def setStartTime(self, value):
        """

        Args:

            startTime (str): Set time custom now time if using historical data

        """
        self._set(startTime=value)
        return self


    def getStartTime(self):
        """

        Returns:

            str: Set time custom now time if using historical data
        """
        return self.getOrDefault(self.startTime)


    def setStartTimeFormat(self, value):
        """

        Args:

            startTimeFormat (str): Format for start time (default: EEE MMM dd HH:mm:ss Z yyyy)

        """
        self._set(startTimeFormat=value)
        return self


    def getStartTimeFormat(self):
        """

        Returns:

            str: Format for start time (default: EEE MMM dd HH:mm:ss Z yyyy)
        """
        return self.getOrDefault(self.startTimeFormat)


    def setSupportThreshold(self, value):
        """

        Args:

            supportThreshold (int): Minimum number of ratings per item (default: 4)

        """
        self._set(supportThreshold=value)
        return self


    def getSupportThreshold(self):
        """

        Returns:

            int: Minimum number of ratings per item (default: 4)
        """
        return self.getOrDefault(self.supportThreshold)


    def setTimeCol(self, value):
        """

        Args:

            timeCol (str): Time of activity (default: time)

        """
        self._set(timeCol=value)
        return self


    def getTimeCol(self):
        """

        Returns:

            str: Time of activity (default: time)
        """
        return self.getOrDefault(self.timeCol)


    def setTimeDecayCoeff(self, value):
        """

        Args:

            timeDecayCoeff (int): Use to scale time decay coeff to different half life dur (default: 30)

        """
        self._set(timeDecayCoeff=value)
        return self


    def getTimeDecayCoeff(self):
        """

        Returns:

            int: Use to scale time decay coeff to different half life dur (default: 30)
        """
        return self.getOrDefault(self.timeDecayCoeff)


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
        return "com.microsoft.ml.spark.SAR"

    @staticmethod
    def _from_java(java_stage):
        module_name=_SAR.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".SAR"
        return from_java(java_stage, module_name)

    def _create_model(self, java_model):
        return _SARModel(java_model)


class _SARModel(ComplexParamsMixin, JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`_SAR`.

    This class is left empty on purpose.
    All necessary methods are exposed through inheritance.
    """

    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.SARModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=_SARModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".SARModel"
        return from_java(java_stage, module_name)

