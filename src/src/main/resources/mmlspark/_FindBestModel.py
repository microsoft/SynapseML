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
class _FindBestModel(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaEstimator):
    """


    Args:

        evaluationMetric (str): Metric to evaluate models with (default: accuracy)
        models (object): List of models to be evaluated
    """

    @keyword_only
    def __init__(self, evaluationMetric="accuracy", models=None):
        super(_FindBestModel, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.FindBestModel")
        self._cache = {}
        self.evaluationMetric = Param(self, "evaluationMetric", "evaluationMetric: Metric to evaluate models with (default: accuracy)")
        self._setDefault(evaluationMetric="accuracy")
        self.models = Param(self, "models", "models: List of models to be evaluated", generateTypeConverter("models", self._cache, complexTypeConverter))
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, evaluationMetric="accuracy", models=None):
        """
        Set the (keyword only) parameters

        Args:

            evaluationMetric (str): Metric to evaluate models with (default: accuracy)
            models (object): List of models to be evaluated
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setEvaluationMetric(self, value):
        """

        Args:

            evaluationMetric (str): Metric to evaluate models with (default: accuracy)

        """
        self._set(evaluationMetric=value)
        return self


    def getEvaluationMetric(self):
        """

        Returns:

            str: Metric to evaluate models with (default: accuracy)
        """
        return self.getOrDefault(self.evaluationMetric)


    def setModels(self, value):
        """

        Args:

            models (object): List of models to be evaluated

        """
        self._set(models=value)
        return self


    def getModels(self):
        """

        Returns:

            object: List of models to be evaluated
        """
        return self._cache.get("models", None)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.FindBestModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=_FindBestModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".FindBestModel"
        return from_java(java_stage, module_name)

    def _create_model(self, java_model):
        return _BestModel(java_model)


class _BestModel(ComplexParamsMixin, JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`_FindBestModel`.

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
        return "com.microsoft.ml.spark.BestModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=_BestModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".BestModel"
        return from_java(java_stage, module_name)

