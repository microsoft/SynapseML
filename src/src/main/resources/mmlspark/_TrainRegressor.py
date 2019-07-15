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
class _TrainRegressor(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaEstimator):
    """


    Args:

        featuresCol (str): The name of the features column (default: [self.uid]_features)
        labelCol (str): The name of the label column
        model (object): Regressor to run
        numFeatures (int): Number of features to hash to (default: 0)
    """

    @keyword_only
    def __init__(self, featuresCol=None, labelCol=None, model=None, numFeatures=0):
        super(_TrainRegressor, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.TrainRegressor")
        self._cache = {}
        self.featuresCol = Param(self, "featuresCol", "featuresCol: The name of the features column (default: [self.uid]_features)")
        self._setDefault(featuresCol=self.uid + "_features")
        self.labelCol = Param(self, "labelCol", "labelCol: The name of the label column")
        self.model = Param(self, "model", "model: Regressor to run", generateTypeConverter("model", self._cache, complexTypeConverter))
        self.numFeatures = Param(self, "numFeatures", "numFeatures: Number of features to hash to (default: 0)")
        self._setDefault(numFeatures=0)
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCol=None, labelCol=None, model=None, numFeatures=0):
        """
        Set the (keyword only) parameters

        Args:

            featuresCol (str): The name of the features column (default: [self.uid]_features)
            labelCol (str): The name of the label column
            model (object): Regressor to run
            numFeatures (int): Number of features to hash to (default: 0)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setFeaturesCol(self, value):
        """

        Args:

            featuresCol (str): The name of the features column (default: [self.uid]_features)

        """
        self._set(featuresCol=value)
        return self


    def getFeaturesCol(self):
        """

        Returns:

            str: The name of the features column (default: [self.uid]_features)
        """
        return self.getOrDefault(self.featuresCol)


    def setLabelCol(self, value):
        """

        Args:

            labelCol (str): The name of the label column

        """
        self._set(labelCol=value)
        return self


    def getLabelCol(self):
        """

        Returns:

            str: The name of the label column
        """
        return self.getOrDefault(self.labelCol)


    def setModel(self, value):
        """

        Args:

            model (object): Regressor to run

        """
        self._set(model=value)
        return self


    def getModel(self):
        """

        Returns:

            object: Regressor to run
        """
        return self._cache.get("model", None)


    def setNumFeatures(self, value):
        """

        Args:

            numFeatures (int): Number of features to hash to (default: 0)

        """
        self._set(numFeatures=value)
        return self


    def getNumFeatures(self):
        """

        Returns:

            int: Number of features to hash to (default: 0)
        """
        return self.getOrDefault(self.numFeatures)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.TrainRegressor"

    @staticmethod
    def _from_java(java_stage):
        module_name=_TrainRegressor.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".TrainRegressor"
        return from_java(java_stage, module_name)

    def _create_model(self, java_model):
        return _TrainedRegressorModel(java_model)


class _TrainedRegressorModel(ComplexParamsMixin, JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`_TrainRegressor`.

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
        return "com.microsoft.ml.spark.TrainedRegressorModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=_TrainedRegressorModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".TrainedRegressorModel"
        return from_java(java_stage, module_name)

