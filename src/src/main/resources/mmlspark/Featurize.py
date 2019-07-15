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
class Featurize(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaEstimator):
    """
    Featurizes a dataset.  Converts the specified columns to feature columns.

    Args:

        allowImages (bool): Allow featurization of images (default: false)
        featureColumns (dict): Feature columns
        numberOfFeatures (int): Number of features to hash string columns to (default: 262144)
        oneHotEncodeCategoricals (bool): One-hot encode categoricals (default: true)
    """

    @keyword_only
    def __init__(self, allowImages=False, featureColumns=None, numberOfFeatures=262144, oneHotEncodeCategoricals=True):
        super(Featurize, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.Featurize")
        self.allowImages = Param(self, "allowImages", "allowImages: Allow featurization of images (default: false)")
        self._setDefault(allowImages=False)
        self.featureColumns = Param(self, "featureColumns", "featureColumns: Feature columns")
        self.numberOfFeatures = Param(self, "numberOfFeatures", "numberOfFeatures: Number of features to hash string columns to (default: 262144)")
        self._setDefault(numberOfFeatures=262144)
        self.oneHotEncodeCategoricals = Param(self, "oneHotEncodeCategoricals", "oneHotEncodeCategoricals: One-hot encode categoricals (default: true)")
        self._setDefault(oneHotEncodeCategoricals=True)
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, allowImages=False, featureColumns=None, numberOfFeatures=262144, oneHotEncodeCategoricals=True):
        """
        Set the (keyword only) parameters

        Args:

            allowImages (bool): Allow featurization of images (default: false)
            featureColumns (dict): Feature columns
            numberOfFeatures (int): Number of features to hash string columns to (default: 262144)
            oneHotEncodeCategoricals (bool): One-hot encode categoricals (default: true)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setAllowImages(self, value):
        """

        Args:

            allowImages (bool): Allow featurization of images (default: false)

        """
        self._set(allowImages=value)
        return self


    def getAllowImages(self):
        """

        Returns:

            bool: Allow featurization of images (default: false)
        """
        return self.getOrDefault(self.allowImages)


    def setFeatureColumns(self, value):
        """

        Args:

            featureColumns (dict): Feature columns

        """
        self._set(featureColumns=value)
        return self


    def getFeatureColumns(self):
        """

        Returns:

            dict: Feature columns
        """
        return self.getOrDefault(self.featureColumns)


    def setNumberOfFeatures(self, value):
        """

        Args:

            numberOfFeatures (int): Number of features to hash string columns to (default: 262144)

        """
        self._set(numberOfFeatures=value)
        return self


    def getNumberOfFeatures(self):
        """

        Returns:

            int: Number of features to hash string columns to (default: 262144)
        """
        return self.getOrDefault(self.numberOfFeatures)


    def setOneHotEncodeCategoricals(self, value):
        """

        Args:

            oneHotEncodeCategoricals (bool): One-hot encode categoricals (default: true)

        """
        self._set(oneHotEncodeCategoricals=value)
        return self


    def getOneHotEncodeCategoricals(self):
        """

        Returns:

            bool: One-hot encode categoricals (default: true)
        """
        return self.getOrDefault(self.oneHotEncodeCategoricals)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.Featurize"

    @staticmethod
    def _from_java(java_stage):
        module_name=Featurize.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".Featurize"
        return from_java(java_stage, module_name)

    def _create_model(self, java_model):
        return PipelineModel(java_model)


class PipelineModel(ComplexParamsMixin, JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`Featurize`.

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
        return "org.apache.spark.ml.PipelineModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=PipelineModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".PipelineModel"
        return from_java(java_stage, module_name)

