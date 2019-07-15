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
class AssembleFeatures(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaEstimator):
    """
    Assembles feature columns into a vector column of features.

    Args:

        allowImages (bool): Allow featurization of images (default: false)
        columnsToFeaturize (list): Columns to featurize
        featuresCol (str): The name of the features column (default: features)
        numberOfFeatures (int): Number of features to hash string columns to
        oneHotEncodeCategoricals (bool): One-hot encode categoricals (default: true)
    """

    @keyword_only
    def __init__(self, allowImages=False, columnsToFeaturize=None, featuresCol="features", numberOfFeatures=None, oneHotEncodeCategoricals=True):
        super(AssembleFeatures, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.AssembleFeatures")
        self.allowImages = Param(self, "allowImages", "allowImages: Allow featurization of images (default: false)")
        self._setDefault(allowImages=False)
        self.columnsToFeaturize = Param(self, "columnsToFeaturize", "columnsToFeaturize: Columns to featurize")
        self.featuresCol = Param(self, "featuresCol", "featuresCol: The name of the features column (default: features)")
        self._setDefault(featuresCol="features")
        self.numberOfFeatures = Param(self, "numberOfFeatures", "numberOfFeatures: Number of features to hash string columns to")
        self.oneHotEncodeCategoricals = Param(self, "oneHotEncodeCategoricals", "oneHotEncodeCategoricals: One-hot encode categoricals (default: true)")
        self._setDefault(oneHotEncodeCategoricals=True)
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, allowImages=False, columnsToFeaturize=None, featuresCol="features", numberOfFeatures=None, oneHotEncodeCategoricals=True):
        """
        Set the (keyword only) parameters

        Args:

            allowImages (bool): Allow featurization of images (default: false)
            columnsToFeaturize (list): Columns to featurize
            featuresCol (str): The name of the features column (default: features)
            numberOfFeatures (int): Number of features to hash string columns to
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


    def setColumnsToFeaturize(self, value):
        """

        Args:

            columnsToFeaturize (list): Columns to featurize

        """
        self._set(columnsToFeaturize=value)
        return self


    def getColumnsToFeaturize(self):
        """

        Returns:

            list: Columns to featurize
        """
        return self.getOrDefault(self.columnsToFeaturize)


    def setFeaturesCol(self, value):
        """

        Args:

            featuresCol (str): The name of the features column (default: features)

        """
        self._set(featuresCol=value)
        return self


    def getFeaturesCol(self):
        """

        Returns:

            str: The name of the features column (default: features)
        """
        return self.getOrDefault(self.featuresCol)


    def setNumberOfFeatures(self, value):
        """

        Args:

            numberOfFeatures (int): Number of features to hash string columns to

        """
        self._set(numberOfFeatures=value)
        return self


    def getNumberOfFeatures(self):
        """

        Returns:

            int: Number of features to hash string columns to
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
        return "com.microsoft.ml.spark.AssembleFeatures"

    @staticmethod
    def _from_java(java_stage):
        module_name=AssembleFeatures.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".AssembleFeatures"
        return from_java(java_stage, module_name)

    def _create_model(self, java_model):
        return AssembleFeaturesModel(java_model)


class AssembleFeaturesModel(ComplexParamsMixin, JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`AssembleFeatures`.

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
        return "com.microsoft.ml.spark.AssembleFeaturesModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=AssembleFeaturesModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".AssembleFeaturesModel"
        return from_java(java_stage, module_name)

