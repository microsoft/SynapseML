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
from mmlspark.TypeConversionUtils import generateTypeConverter, complexTypeConverter

@inherit_doc
class _TuneHyperparameters(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaEstimator):
    """


    Args:

        evaluationMetric (str): Metric to evaluate models with
        models (object): Estimators to run
        numFolds (int): Number of folds
        numRuns (int): Termination criteria for randomized search
        parallelism (int): The number of models to run in parallel
        paramSpace (object): Parameter space for generating hyperparameters
        seed (long): Random number generator seed (default: 0)
    """

    @keyword_only
    def __init__(self, evaluationMetric=None, models=None, numFolds=None, numRuns=None, parallelism=None, paramSpace=None, seed=0):
        super(_TuneHyperparameters, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.TuneHyperparameters")
        self._cache = {}
        self.evaluationMetric = Param(self, "evaluationMetric", "evaluationMetric: Metric to evaluate models with")
        self.models = Param(self, "models", "models: Estimators to run", generateTypeConverter("models", self._cache, complexTypeConverter))
        self.numFolds = Param(self, "numFolds", "numFolds: Number of folds")
        self.numRuns = Param(self, "numRuns", "numRuns: Termination criteria for randomized search")
        self.parallelism = Param(self, "parallelism", "parallelism: The number of models to run in parallel")
        self.paramSpace = Param(self, "paramSpace", "paramSpace: Parameter space for generating hyperparameters")
        self.seed = Param(self, "seed", "seed: Random number generator seed (default: 0)")
        self._setDefault(seed=0)
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, evaluationMetric=None, models=None, numFolds=None, numRuns=None, parallelism=None, paramSpace=None, seed=0):
        """
        Set the (keyword only) parameters

        Args:

            evaluationMetric (str): Metric to evaluate models with
            models (object): Estimators to run
            numFolds (int): Number of folds
            numRuns (int): Termination criteria for randomized search
            parallelism (int): The number of models to run in parallel
            paramSpace (object): Parameter space for generating hyperparameters
            seed (long): Random number generator seed (default: 0)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setEvaluationMetric(self, value):
        """

        Args:

            evaluationMetric (str): Metric to evaluate models with

        """
        self._set(evaluationMetric=value)
        return self


    def getEvaluationMetric(self):
        """

        Returns:

            str: Metric to evaluate models with
        """
        return self.getOrDefault(self.evaluationMetric)


    def setModels(self, value):
        """

        Args:

            models (object): Estimators to run

        """
        self._set(models=value)
        return self


    def getModels(self):
        """

        Returns:

            object: Estimators to run
        """
        return self._cache.get("models", None)


    def setNumFolds(self, value):
        """

        Args:

            numFolds (int): Number of folds

        """
        self._set(numFolds=value)
        return self


    def getNumFolds(self):
        """

        Returns:

            int: Number of folds
        """
        return self.getOrDefault(self.numFolds)


    def setNumRuns(self, value):
        """

        Args:

            numRuns (int): Termination criteria for randomized search

        """
        self._set(numRuns=value)
        return self


    def getNumRuns(self):
        """

        Returns:

            int: Termination criteria for randomized search
        """
        return self.getOrDefault(self.numRuns)


    def setParallelism(self, value):
        """

        Args:

            parallelism (int): The number of models to run in parallel

        """
        self._set(parallelism=value)
        return self


    def getParallelism(self):
        """

        Returns:

            int: The number of models to run in parallel
        """
        return self.getOrDefault(self.parallelism)


    def setParamSpace(self, value):
        """

        Args:

            paramSpace (object): Parameter space for generating hyperparameters

        """
        self._set(paramSpace=value)
        return self


    def getParamSpace(self):
        """

        Returns:

            object: Parameter space for generating hyperparameters
        """
        return self.getOrDefault(self.paramSpace)


    def setSeed(self, value):
        """

        Args:

            seed (long): Random number generator seed (default: 0)

        """
        self._set(seed=value)
        return self


    def getSeed(self):
        """

        Returns:

            long: Random number generator seed (default: 0)
        """
        return self.getOrDefault(self.seed)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.TuneHyperparameters"

    @staticmethod
    def _from_java(java_stage):
        module_name=_TuneHyperparameters.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".TuneHyperparameters"
        return from_java(java_stage, module_name)

    def _create_model(self, java_model):
        return _TuneHyperparametersModel(java_model)


class _TuneHyperparametersModel(ComplexParamsMixin, JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`_TuneHyperparameters`.

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
        return "com.microsoft.ml.spark.TuneHyperparametersModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=_TuneHyperparametersModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".TuneHyperparametersModel"
        return from_java(java_stage, module_name)

