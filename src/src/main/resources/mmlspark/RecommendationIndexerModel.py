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
class RecommendationIndexerModel(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        itemIndexModel (object): itemIndexModel
        itemInputCol (str): Item Input Col
        itemOutputCol (str): Item Output Col
        ratingCol (str): Rating Col
        userIndexModel (object): userIndexModel
        userInputCol (str): User Input Col
        userOutputCol (str): User Output Col
    """

    @keyword_only
    def __init__(self, itemIndexModel=None, itemInputCol=None, itemOutputCol=None, ratingCol=None, userIndexModel=None, userInputCol=None, userOutputCol=None):
        super(RecommendationIndexerModel, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.RecommendationIndexerModel")
        self._cache = {}
        self.itemIndexModel = Param(self, "itemIndexModel", "itemIndexModel: itemIndexModel", generateTypeConverter("itemIndexModel", self._cache, complexTypeConverter))
        self.itemInputCol = Param(self, "itemInputCol", "itemInputCol: Item Input Col")
        self.itemOutputCol = Param(self, "itemOutputCol", "itemOutputCol: Item Output Col")
        self.ratingCol = Param(self, "ratingCol", "ratingCol: Rating Col")
        self.userIndexModel = Param(self, "userIndexModel", "userIndexModel: userIndexModel", generateTypeConverter("userIndexModel", self._cache, complexTypeConverter))
        self.userInputCol = Param(self, "userInputCol", "userInputCol: User Input Col")
        self.userOutputCol = Param(self, "userOutputCol", "userOutputCol: User Output Col")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, itemIndexModel=None, itemInputCol=None, itemOutputCol=None, ratingCol=None, userIndexModel=None, userInputCol=None, userOutputCol=None):
        """
        Set the (keyword only) parameters

        Args:

            itemIndexModel (object): itemIndexModel
            itemInputCol (str): Item Input Col
            itemOutputCol (str): Item Output Col
            ratingCol (str): Rating Col
            userIndexModel (object): userIndexModel
            userInputCol (str): User Input Col
            userOutputCol (str): User Output Col
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setItemIndexModel(self, value):
        """

        Args:

            itemIndexModel (object): itemIndexModel

        """
        self._set(itemIndexModel=value)
        return self


    def getItemIndexModel(self):
        """

        Returns:

            object: itemIndexModel
        """
        return self._cache.get("itemIndexModel", None)


    def setItemInputCol(self, value):
        """

        Args:

            itemInputCol (str): Item Input Col

        """
        self._set(itemInputCol=value)
        return self


    def getItemInputCol(self):
        """

        Returns:

            str: Item Input Col
        """
        return self.getOrDefault(self.itemInputCol)


    def setItemOutputCol(self, value):
        """

        Args:

            itemOutputCol (str): Item Output Col

        """
        self._set(itemOutputCol=value)
        return self


    def getItemOutputCol(self):
        """

        Returns:

            str: Item Output Col
        """
        return self.getOrDefault(self.itemOutputCol)


    def setRatingCol(self, value):
        """

        Args:

            ratingCol (str): Rating Col

        """
        self._set(ratingCol=value)
        return self


    def getRatingCol(self):
        """

        Returns:

            str: Rating Col
        """
        return self.getOrDefault(self.ratingCol)


    def setUserIndexModel(self, value):
        """

        Args:

            userIndexModel (object): userIndexModel

        """
        self._set(userIndexModel=value)
        return self


    def getUserIndexModel(self):
        """

        Returns:

            object: userIndexModel
        """
        return self._cache.get("userIndexModel", None)


    def setUserInputCol(self, value):
        """

        Args:

            userInputCol (str): User Input Col

        """
        self._set(userInputCol=value)
        return self


    def getUserInputCol(self):
        """

        Returns:

            str: User Input Col
        """
        return self.getOrDefault(self.userInputCol)


    def setUserOutputCol(self, value):
        """

        Args:

            userOutputCol (str): User Output Col

        """
        self._set(userOutputCol=value)
        return self


    def getUserOutputCol(self):
        """

        Returns:

            str: User Output Col
        """
        return self.getOrDefault(self.userOutputCol)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.RecommendationIndexerModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=RecommendationIndexerModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".RecommendationIndexerModel"
        return from_java(java_stage, module_name)
