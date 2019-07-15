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
class _LightGBMRegressor(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaEstimator):
    """


    Args:

        alpha (double): parameter for Huber loss and Quantile regression (default: 0.9)
        baggingFraction (double): Bagging fraction (default: 1.0)
        baggingFreq (int): Bagging frequency (default: 0)
        baggingSeed (int): Bagging seed (default: 3)
        boostFromAverage (bool): Adjusts initial score to the mean of labels for faster convergence (default: true)
        boostingType (str): Default gbdt = traditional Gradient Boosting Decision Tree. Options are: gbdt, gbrt, rf (Random Forest), random_forest, dart (Dropouts meet Multiple Additive Regression Trees), goss (Gradient-based One-Side Sampling).  (default: gbdt)
        categoricalSlotIndexes (list): List of categorical column indexes, the slot index in the features column
        categoricalSlotNames (list): List of categorical column slot names, the slot name in the features column
        defaultListenPort (int): The default listen port on executors, used for testing (default: 12400)
        earlyStoppingRound (int): Early stopping round (default: 0)
        featureFraction (double): Feature fraction (default: 1.0)
        featuresCol (str): features column name (default: features)
        initScoreCol (str): The name of the initial score column, used for continued training
        labelCol (str): label column name (default: label)
        lambdaL1 (double): L1 regularization (default: 0.0)
        lambdaL2 (double): L2 regularization (default: 0.0)
        learningRate (double): Learning rate or shrinkage rate (default: 0.1)
        maxBin (int): Max bin (default: 255)
        maxDepth (int): Max depth (default: -1)
        minSumHessianInLeaf (double): Minimal sum hessian in one leaf (default: 0.001)
        modelString (str): LightGBM model to retrain (default: )
        numBatches (int): If greater than 0, splits data into separate batches during training (default: 0)
        numIterations (int): Number of iterations, LightGBM constructs num_class * num_iterations trees (default: 100)
        numLeaves (int): Number of leaves (default: 31)
        objective (str): The Objective. For regression applications, this can be: regression_l2, regression_l1, huber, fair, poisson, quantile, mape, gamma or tweedie. For classification applications, this can be: binary, multiclass, or multiclassova.  (default: regression)
        parallelism (str): Tree learner parallelism, can be set to data_parallel or voting_parallel (default: data_parallel)
        predictionCol (str): prediction column name (default: prediction)
        timeout (double): Timeout in seconds (default: 1200.0)
        tweedieVariancePower (double): control the variance of tweedie distribution, must be between 1 and 2 (default: 1.5)
        validationIndicatorCol (str): Indicates whether the row is for training or validation
        verbosity (int): Verbosity where lt 0 is Fatal, eq 0 is Error, eq 1 is Info, gt 1 is Debug (default: 1)
        weightCol (str): The name of the weight column
    """

    @keyword_only
    def __init__(self, alpha=0.9, baggingFraction=1.0, baggingFreq=0, baggingSeed=3, boostFromAverage=True, boostingType="gbdt", categoricalSlotIndexes=None, categoricalSlotNames=None, defaultListenPort=12400, earlyStoppingRound=0, featureFraction=1.0, featuresCol="features", initScoreCol=None, labelCol="label", lambdaL1=0.0, lambdaL2=0.0, learningRate=0.1, maxBin=255, maxDepth=-1, minSumHessianInLeaf=0.001, modelString="", numBatches=0, numIterations=100, numLeaves=31, objective="regression", parallelism="data_parallel", predictionCol="prediction", timeout=1200.0, tweedieVariancePower=1.5, validationIndicatorCol=None, verbosity=1, weightCol=None):
        super(_LightGBMRegressor, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.LightGBMRegressor")
        self.alpha = Param(self, "alpha", "alpha: parameter for Huber loss and Quantile regression (default: 0.9)")
        self._setDefault(alpha=0.9)
        self.baggingFraction = Param(self, "baggingFraction", "baggingFraction: Bagging fraction (default: 1.0)")
        self._setDefault(baggingFraction=1.0)
        self.baggingFreq = Param(self, "baggingFreq", "baggingFreq: Bagging frequency (default: 0)")
        self._setDefault(baggingFreq=0)
        self.baggingSeed = Param(self, "baggingSeed", "baggingSeed: Bagging seed (default: 3)")
        self._setDefault(baggingSeed=3)
        self.boostFromAverage = Param(self, "boostFromAverage", "boostFromAverage: Adjusts initial score to the mean of labels for faster convergence (default: true)")
        self._setDefault(boostFromAverage=True)
        self.boostingType = Param(self, "boostingType", "boostingType: Default gbdt = traditional Gradient Boosting Decision Tree. Options are: gbdt, gbrt, rf (Random Forest), random_forest, dart (Dropouts meet Multiple Additive Regression Trees), goss (Gradient-based One-Side Sampling).  (default: gbdt)")
        self._setDefault(boostingType="gbdt")
        self.categoricalSlotIndexes = Param(self, "categoricalSlotIndexes", "categoricalSlotIndexes: List of categorical column indexes, the slot index in the features column")
        self.categoricalSlotNames = Param(self, "categoricalSlotNames", "categoricalSlotNames: List of categorical column slot names, the slot name in the features column")
        self.defaultListenPort = Param(self, "defaultListenPort", "defaultListenPort: The default listen port on executors, used for testing (default: 12400)")
        self._setDefault(defaultListenPort=12400)
        self.earlyStoppingRound = Param(self, "earlyStoppingRound", "earlyStoppingRound: Early stopping round (default: 0)")
        self._setDefault(earlyStoppingRound=0)
        self.featureFraction = Param(self, "featureFraction", "featureFraction: Feature fraction (default: 1.0)")
        self._setDefault(featureFraction=1.0)
        self.featuresCol = Param(self, "featuresCol", "featuresCol: features column name (default: features)")
        self._setDefault(featuresCol="features")
        self.initScoreCol = Param(self, "initScoreCol", "initScoreCol: The name of the initial score column, used for continued training")
        self.labelCol = Param(self, "labelCol", "labelCol: label column name (default: label)")
        self._setDefault(labelCol="label")
        self.lambdaL1 = Param(self, "lambdaL1", "lambdaL1: L1 regularization (default: 0.0)")
        self._setDefault(lambdaL1=0.0)
        self.lambdaL2 = Param(self, "lambdaL2", "lambdaL2: L2 regularization (default: 0.0)")
        self._setDefault(lambdaL2=0.0)
        self.learningRate = Param(self, "learningRate", "learningRate: Learning rate or shrinkage rate (default: 0.1)")
        self._setDefault(learningRate=0.1)
        self.maxBin = Param(self, "maxBin", "maxBin: Max bin (default: 255)")
        self._setDefault(maxBin=255)
        self.maxDepth = Param(self, "maxDepth", "maxDepth: Max depth (default: -1)")
        self._setDefault(maxDepth=-1)
        self.minSumHessianInLeaf = Param(self, "minSumHessianInLeaf", "minSumHessianInLeaf: Minimal sum hessian in one leaf (default: 0.001)")
        self._setDefault(minSumHessianInLeaf=0.001)
        self.modelString = Param(self, "modelString", "modelString: LightGBM model to retrain (default: )")
        self._setDefault(modelString="")
        self.numBatches = Param(self, "numBatches", "numBatches: If greater than 0, splits data into separate batches during training (default: 0)")
        self._setDefault(numBatches=0)
        self.numIterations = Param(self, "numIterations", "numIterations: Number of iterations, LightGBM constructs num_class * num_iterations trees (default: 100)")
        self._setDefault(numIterations=100)
        self.numLeaves = Param(self, "numLeaves", "numLeaves: Number of leaves (default: 31)")
        self._setDefault(numLeaves=31)
        self.objective = Param(self, "objective", "objective: The Objective. For regression applications, this can be: regression_l2, regression_l1, huber, fair, poisson, quantile, mape, gamma or tweedie. For classification applications, this can be: binary, multiclass, or multiclassova.  (default: regression)")
        self._setDefault(objective="regression")
        self.parallelism = Param(self, "parallelism", "parallelism: Tree learner parallelism, can be set to data_parallel or voting_parallel (default: data_parallel)")
        self._setDefault(parallelism="data_parallel")
        self.predictionCol = Param(self, "predictionCol", "predictionCol: prediction column name (default: prediction)")
        self._setDefault(predictionCol="prediction")
        self.timeout = Param(self, "timeout", "timeout: Timeout in seconds (default: 1200.0)")
        self._setDefault(timeout=1200.0)
        self.tweedieVariancePower = Param(self, "tweedieVariancePower", "tweedieVariancePower: control the variance of tweedie distribution, must be between 1 and 2 (default: 1.5)")
        self._setDefault(tweedieVariancePower=1.5)
        self.validationIndicatorCol = Param(self, "validationIndicatorCol", "validationIndicatorCol: Indicates whether the row is for training or validation")
        self.verbosity = Param(self, "verbosity", "verbosity: Verbosity where lt 0 is Fatal, eq 0 is Error, eq 1 is Info, gt 1 is Debug (default: 1)")
        self._setDefault(verbosity=1)
        self.weightCol = Param(self, "weightCol", "weightCol: The name of the weight column")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, alpha=0.9, baggingFraction=1.0, baggingFreq=0, baggingSeed=3, boostFromAverage=True, boostingType="gbdt", categoricalSlotIndexes=None, categoricalSlotNames=None, defaultListenPort=12400, earlyStoppingRound=0, featureFraction=1.0, featuresCol="features", initScoreCol=None, labelCol="label", lambdaL1=0.0, lambdaL2=0.0, learningRate=0.1, maxBin=255, maxDepth=-1, minSumHessianInLeaf=0.001, modelString="", numBatches=0, numIterations=100, numLeaves=31, objective="regression", parallelism="data_parallel", predictionCol="prediction", timeout=1200.0, tweedieVariancePower=1.5, validationIndicatorCol=None, verbosity=1, weightCol=None):
        """
        Set the (keyword only) parameters

        Args:

            alpha (double): parameter for Huber loss and Quantile regression (default: 0.9)
            baggingFraction (double): Bagging fraction (default: 1.0)
            baggingFreq (int): Bagging frequency (default: 0)
            baggingSeed (int): Bagging seed (default: 3)
            boostFromAverage (bool): Adjusts initial score to the mean of labels for faster convergence (default: true)
            boostingType (str): Default gbdt = traditional Gradient Boosting Decision Tree. Options are: gbdt, gbrt, rf (Random Forest), random_forest, dart (Dropouts meet Multiple Additive Regression Trees), goss (Gradient-based One-Side Sampling).  (default: gbdt)
            categoricalSlotIndexes (list): List of categorical column indexes, the slot index in the features column
            categoricalSlotNames (list): List of categorical column slot names, the slot name in the features column
            defaultListenPort (int): The default listen port on executors, used for testing (default: 12400)
            earlyStoppingRound (int): Early stopping round (default: 0)
            featureFraction (double): Feature fraction (default: 1.0)
            featuresCol (str): features column name (default: features)
            initScoreCol (str): The name of the initial score column, used for continued training
            labelCol (str): label column name (default: label)
            lambdaL1 (double): L1 regularization (default: 0.0)
            lambdaL2 (double): L2 regularization (default: 0.0)
            learningRate (double): Learning rate or shrinkage rate (default: 0.1)
            maxBin (int): Max bin (default: 255)
            maxDepth (int): Max depth (default: -1)
            minSumHessianInLeaf (double): Minimal sum hessian in one leaf (default: 0.001)
            modelString (str): LightGBM model to retrain (default: )
            numBatches (int): If greater than 0, splits data into separate batches during training (default: 0)
            numIterations (int): Number of iterations, LightGBM constructs num_class * num_iterations trees (default: 100)
            numLeaves (int): Number of leaves (default: 31)
            objective (str): The Objective. For regression applications, this can be: regression_l2, regression_l1, huber, fair, poisson, quantile, mape, gamma or tweedie. For classification applications, this can be: binary, multiclass, or multiclassova.  (default: regression)
            parallelism (str): Tree learner parallelism, can be set to data_parallel or voting_parallel (default: data_parallel)
            predictionCol (str): prediction column name (default: prediction)
            timeout (double): Timeout in seconds (default: 1200.0)
            tweedieVariancePower (double): control the variance of tweedie distribution, must be between 1 and 2 (default: 1.5)
            validationIndicatorCol (str): Indicates whether the row is for training or validation
            verbosity (int): Verbosity where lt 0 is Fatal, eq 0 is Error, eq 1 is Info, gt 1 is Debug (default: 1)
            weightCol (str): The name of the weight column
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setAlpha(self, value):
        """

        Args:

            alpha (double): parameter for Huber loss and Quantile regression (default: 0.9)

        """
        self._set(alpha=value)
        return self


    def getAlpha(self):
        """

        Returns:

            double: parameter for Huber loss and Quantile regression (default: 0.9)
        """
        return self.getOrDefault(self.alpha)


    def setBaggingFraction(self, value):
        """

        Args:

            baggingFraction (double): Bagging fraction (default: 1.0)

        """
        self._set(baggingFraction=value)
        return self


    def getBaggingFraction(self):
        """

        Returns:

            double: Bagging fraction (default: 1.0)
        """
        return self.getOrDefault(self.baggingFraction)


    def setBaggingFreq(self, value):
        """

        Args:

            baggingFreq (int): Bagging frequency (default: 0)

        """
        self._set(baggingFreq=value)
        return self


    def getBaggingFreq(self):
        """

        Returns:

            int: Bagging frequency (default: 0)
        """
        return self.getOrDefault(self.baggingFreq)


    def setBaggingSeed(self, value):
        """

        Args:

            baggingSeed (int): Bagging seed (default: 3)

        """
        self._set(baggingSeed=value)
        return self


    def getBaggingSeed(self):
        """

        Returns:

            int: Bagging seed (default: 3)
        """
        return self.getOrDefault(self.baggingSeed)


    def setBoostFromAverage(self, value):
        """

        Args:

            boostFromAverage (bool): Adjusts initial score to the mean of labels for faster convergence (default: true)

        """
        self._set(boostFromAverage=value)
        return self


    def getBoostFromAverage(self):
        """

        Returns:

            bool: Adjusts initial score to the mean of labels for faster convergence (default: true)
        """
        return self.getOrDefault(self.boostFromAverage)


    def setBoostingType(self, value):
        """

        Args:

            boostingType (str): Default gbdt = traditional Gradient Boosting Decision Tree. Options are: gbdt, gbrt, rf (Random Forest), random_forest, dart (Dropouts meet Multiple Additive Regression Trees), goss (Gradient-based One-Side Sampling).  (default: gbdt)

        """
        self._set(boostingType=value)
        return self


    def getBoostingType(self):
        """

        Returns:

            str: Default gbdt = traditional Gradient Boosting Decision Tree. Options are: gbdt, gbrt, rf (Random Forest), random_forest, dart (Dropouts meet Multiple Additive Regression Trees), goss (Gradient-based One-Side Sampling).  (default: gbdt)
        """
        return self.getOrDefault(self.boostingType)


    def setCategoricalSlotIndexes(self, value):
        """

        Args:

            categoricalSlotIndexes (list): List of categorical column indexes, the slot index in the features column

        """
        self._set(categoricalSlotIndexes=value)
        return self


    def getCategoricalSlotIndexes(self):
        """

        Returns:

            list: List of categorical column indexes, the slot index in the features column
        """
        return self.getOrDefault(self.categoricalSlotIndexes)


    def setCategoricalSlotNames(self, value):
        """

        Args:

            categoricalSlotNames (list): List of categorical column slot names, the slot name in the features column

        """
        self._set(categoricalSlotNames=value)
        return self


    def getCategoricalSlotNames(self):
        """

        Returns:

            list: List of categorical column slot names, the slot name in the features column
        """
        return self.getOrDefault(self.categoricalSlotNames)


    def setDefaultListenPort(self, value):
        """

        Args:

            defaultListenPort (int): The default listen port on executors, used for testing (default: 12400)

        """
        self._set(defaultListenPort=value)
        return self


    def getDefaultListenPort(self):
        """

        Returns:

            int: The default listen port on executors, used for testing (default: 12400)
        """
        return self.getOrDefault(self.defaultListenPort)


    def setEarlyStoppingRound(self, value):
        """

        Args:

            earlyStoppingRound (int): Early stopping round (default: 0)

        """
        self._set(earlyStoppingRound=value)
        return self


    def getEarlyStoppingRound(self):
        """

        Returns:

            int: Early stopping round (default: 0)
        """
        return self.getOrDefault(self.earlyStoppingRound)


    def setFeatureFraction(self, value):
        """

        Args:

            featureFraction (double): Feature fraction (default: 1.0)

        """
        self._set(featureFraction=value)
        return self


    def getFeatureFraction(self):
        """

        Returns:

            double: Feature fraction (default: 1.0)
        """
        return self.getOrDefault(self.featureFraction)


    def setFeaturesCol(self, value):
        """

        Args:

            featuresCol (str): features column name (default: features)

        """
        self._set(featuresCol=value)
        return self


    def getFeaturesCol(self):
        """

        Returns:

            str: features column name (default: features)
        """
        return self.getOrDefault(self.featuresCol)


    def setInitScoreCol(self, value):
        """

        Args:

            initScoreCol (str): The name of the initial score column, used for continued training

        """
        self._set(initScoreCol=value)
        return self


    def getInitScoreCol(self):
        """

        Returns:

            str: The name of the initial score column, used for continued training
        """
        return self.getOrDefault(self.initScoreCol)


    def setLabelCol(self, value):
        """

        Args:

            labelCol (str): label column name (default: label)

        """
        self._set(labelCol=value)
        return self


    def getLabelCol(self):
        """

        Returns:

            str: label column name (default: label)
        """
        return self.getOrDefault(self.labelCol)


    def setLambdaL1(self, value):
        """

        Args:

            lambdaL1 (double): L1 regularization (default: 0.0)

        """
        self._set(lambdaL1=value)
        return self


    def getLambdaL1(self):
        """

        Returns:

            double: L1 regularization (default: 0.0)
        """
        return self.getOrDefault(self.lambdaL1)


    def setLambdaL2(self, value):
        """

        Args:

            lambdaL2 (double): L2 regularization (default: 0.0)

        """
        self._set(lambdaL2=value)
        return self


    def getLambdaL2(self):
        """

        Returns:

            double: L2 regularization (default: 0.0)
        """
        return self.getOrDefault(self.lambdaL2)


    def setLearningRate(self, value):
        """

        Args:

            learningRate (double): Learning rate or shrinkage rate (default: 0.1)

        """
        self._set(learningRate=value)
        return self


    def getLearningRate(self):
        """

        Returns:

            double: Learning rate or shrinkage rate (default: 0.1)
        """
        return self.getOrDefault(self.learningRate)


    def setMaxBin(self, value):
        """

        Args:

            maxBin (int): Max bin (default: 255)

        """
        self._set(maxBin=value)
        return self


    def getMaxBin(self):
        """

        Returns:

            int: Max bin (default: 255)
        """
        return self.getOrDefault(self.maxBin)


    def setMaxDepth(self, value):
        """

        Args:

            maxDepth (int): Max depth (default: -1)

        """
        self._set(maxDepth=value)
        return self


    def getMaxDepth(self):
        """

        Returns:

            int: Max depth (default: -1)
        """
        return self.getOrDefault(self.maxDepth)


    def setMinSumHessianInLeaf(self, value):
        """

        Args:

            minSumHessianInLeaf (double): Minimal sum hessian in one leaf (default: 0.001)

        """
        self._set(minSumHessianInLeaf=value)
        return self


    def getMinSumHessianInLeaf(self):
        """

        Returns:

            double: Minimal sum hessian in one leaf (default: 0.001)
        """
        return self.getOrDefault(self.minSumHessianInLeaf)


    def setModelString(self, value):
        """

        Args:

            modelString (str): LightGBM model to retrain (default: )

        """
        self._set(modelString=value)
        return self


    def getModelString(self):
        """

        Returns:

            str: LightGBM model to retrain (default: )
        """
        return self.getOrDefault(self.modelString)


    def setNumBatches(self, value):
        """

        Args:

            numBatches (int): If greater than 0, splits data into separate batches during training (default: 0)

        """
        self._set(numBatches=value)
        return self


    def getNumBatches(self):
        """

        Returns:

            int: If greater than 0, splits data into separate batches during training (default: 0)
        """
        return self.getOrDefault(self.numBatches)


    def setNumIterations(self, value):
        """

        Args:

            numIterations (int): Number of iterations, LightGBM constructs num_class * num_iterations trees (default: 100)

        """
        self._set(numIterations=value)
        return self


    def getNumIterations(self):
        """

        Returns:

            int: Number of iterations, LightGBM constructs num_class * num_iterations trees (default: 100)
        """
        return self.getOrDefault(self.numIterations)


    def setNumLeaves(self, value):
        """

        Args:

            numLeaves (int): Number of leaves (default: 31)

        """
        self._set(numLeaves=value)
        return self


    def getNumLeaves(self):
        """

        Returns:

            int: Number of leaves (default: 31)
        """
        return self.getOrDefault(self.numLeaves)


    def setObjective(self, value):
        """

        Args:

            objective (str): The Objective. For regression applications, this can be: regression_l2, regression_l1, huber, fair, poisson, quantile, mape, gamma or tweedie. For classification applications, this can be: binary, multiclass, or multiclassova.  (default: regression)

        """
        self._set(objective=value)
        return self


    def getObjective(self):
        """

        Returns:

            str: The Objective. For regression applications, this can be: regression_l2, regression_l1, huber, fair, poisson, quantile, mape, gamma or tweedie. For classification applications, this can be: binary, multiclass, or multiclassova.  (default: regression)
        """
        return self.getOrDefault(self.objective)


    def setParallelism(self, value):
        """

        Args:

            parallelism (str): Tree learner parallelism, can be set to data_parallel or voting_parallel (default: data_parallel)

        """
        self._set(parallelism=value)
        return self


    def getParallelism(self):
        """

        Returns:

            str: Tree learner parallelism, can be set to data_parallel or voting_parallel (default: data_parallel)
        """
        return self.getOrDefault(self.parallelism)


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


    def setTimeout(self, value):
        """

        Args:

            timeout (double): Timeout in seconds (default: 1200.0)

        """
        self._set(timeout=value)
        return self


    def getTimeout(self):
        """

        Returns:

            double: Timeout in seconds (default: 1200.0)
        """
        return self.getOrDefault(self.timeout)


    def setTweedieVariancePower(self, value):
        """

        Args:

            tweedieVariancePower (double): control the variance of tweedie distribution, must be between 1 and 2 (default: 1.5)

        """
        self._set(tweedieVariancePower=value)
        return self


    def getTweedieVariancePower(self):
        """

        Returns:

            double: control the variance of tweedie distribution, must be between 1 and 2 (default: 1.5)
        """
        return self.getOrDefault(self.tweedieVariancePower)


    def setValidationIndicatorCol(self, value):
        """

        Args:

            validationIndicatorCol (str): Indicates whether the row is for training or validation

        """
        self._set(validationIndicatorCol=value)
        return self


    def getValidationIndicatorCol(self):
        """

        Returns:

            str: Indicates whether the row is for training or validation
        """
        return self.getOrDefault(self.validationIndicatorCol)


    def setVerbosity(self, value):
        """

        Args:

            verbosity (int): Verbosity where lt 0 is Fatal, eq 0 is Error, eq 1 is Info, gt 1 is Debug (default: 1)

        """
        self._set(verbosity=value)
        return self


    def getVerbosity(self):
        """

        Returns:

            int: Verbosity where lt 0 is Fatal, eq 0 is Error, eq 1 is Info, gt 1 is Debug (default: 1)
        """
        return self.getOrDefault(self.verbosity)


    def setWeightCol(self, value):
        """

        Args:

            weightCol (str): The name of the weight column

        """
        self._set(weightCol=value)
        return self


    def getWeightCol(self):
        """

        Returns:

            str: The name of the weight column
        """
        return self.getOrDefault(self.weightCol)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.LightGBMRegressor"

    @staticmethod
    def _from_java(java_stage):
        module_name=_LightGBMRegressor.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".LightGBMRegressor"
        return from_java(java_stage, module_name)

    def _create_model(self, java_model):
        return _LightGBMRegressionModel(java_model)


class _LightGBMRegressionModel(ComplexParamsMixin, JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`_LightGBMRegressor`.

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
        return "com.microsoft.ml.spark.LightGBMRegressionModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=_LightGBMRegressionModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".LightGBMRegressionModel"
        return from_java(java_stage, module_name)

