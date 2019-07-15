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
class _RankingTrainValidationSplitModel(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        bestModel (object): The internal ALS model used splitter
        validationMetrics (object): Best Model
    """

    @keyword_only
    def __init__(self, bestModel=None, validationMetrics=None):
        super(_RankingTrainValidationSplitModel, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.RankingTrainValidationSplitModel")
        self._cache = {}
        self.bestModel = Param(self, "bestModel", "bestModel: The internal ALS model used splitter", generateTypeConverter("bestModel", self._cache, complexTypeConverter))
        self.validationMetrics = Param(self, "validationMetrics", "validationMetrics: Best Model")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, bestModel=None, validationMetrics=None):
        """
        Set the (keyword only) parameters

        Args:

            bestModel (object): The internal ALS model used splitter
            validationMetrics (object): Best Model
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setBestModel(self, value):
        """

        Args:

            bestModel (object): The internal ALS model used splitter

        """
        self._set(bestModel=value)
        return self


    def getBestModel(self):
        """

        Returns:

            object: The internal ALS model used splitter
        """
        return self._cache.get("bestModel", None)


    def setValidationMetrics(self, value):
        """

        Args:

            validationMetrics (object): Best Model

        """
        self._set(validationMetrics=value)
        return self


    def getValidationMetrics(self):
        """

        Returns:

            object: Best Model
        """
        return self.getOrDefault(self.validationMetrics)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.RankingTrainValidationSplitModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=_RankingTrainValidationSplitModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".RankingTrainValidationSplitModel"
        return from_java(java_stage, module_name)
