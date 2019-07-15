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
class RankingAdapterModel(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        itemCol (str): Column of items
        k (int): number of items (default: 10)
        labelCol (str): The name of the label column (default: label)
        minRatingsPerItem (int): min ratings for items > 0 (default: 1)
        minRatingsPerUser (int): min ratings for users > 0 (default: 1)
        mode (str): recommendation mode (default: allUsers)
        ratingCol (str): Column of ratings
        recommender (object): estimator for selection
        recommenderModel (object): recommenderModel
        userCol (str): Column of users
    """

    @keyword_only
    def __init__(self, itemCol=None, k=10, labelCol="label", minRatingsPerItem=1, minRatingsPerUser=1, mode="allUsers", ratingCol=None, recommender=None, recommenderModel=None, userCol=None):
        super(RankingAdapterModel, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.RankingAdapterModel")
        self._cache = {}
        self.itemCol = Param(self, "itemCol", "itemCol: Column of items")
        self.k = Param(self, "k", "k: number of items (default: 10)")
        self._setDefault(k=10)
        self.labelCol = Param(self, "labelCol", "labelCol: The name of the label column (default: label)")
        self._setDefault(labelCol="label")
        self.minRatingsPerItem = Param(self, "minRatingsPerItem", "minRatingsPerItem: min ratings for items > 0 (default: 1)")
        self._setDefault(minRatingsPerItem=1)
        self.minRatingsPerUser = Param(self, "minRatingsPerUser", "minRatingsPerUser: min ratings for users > 0 (default: 1)")
        self._setDefault(minRatingsPerUser=1)
        self.mode = Param(self, "mode", "mode: recommendation mode (default: allUsers)")
        self._setDefault(mode="allUsers")
        self.ratingCol = Param(self, "ratingCol", "ratingCol: Column of ratings")
        self.recommender = Param(self, "recommender", "recommender: estimator for selection", generateTypeConverter("recommender", self._cache, complexTypeConverter))
        self.recommenderModel = Param(self, "recommenderModel", "recommenderModel: recommenderModel", generateTypeConverter("recommenderModel", self._cache, complexTypeConverter))
        self.userCol = Param(self, "userCol", "userCol: Column of users")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, itemCol=None, k=10, labelCol="label", minRatingsPerItem=1, minRatingsPerUser=1, mode="allUsers", ratingCol=None, recommender=None, recommenderModel=None, userCol=None):
        """
        Set the (keyword only) parameters

        Args:

            itemCol (str): Column of items
            k (int): number of items (default: 10)
            labelCol (str): The name of the label column (default: label)
            minRatingsPerItem (int): min ratings for items > 0 (default: 1)
            minRatingsPerUser (int): min ratings for users > 0 (default: 1)
            mode (str): recommendation mode (default: allUsers)
            ratingCol (str): Column of ratings
            recommender (object): estimator for selection
            recommenderModel (object): recommenderModel
            userCol (str): Column of users
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setItemCol(self, value):
        """

        Args:

            itemCol (str): Column of items

        """
        self._set(itemCol=value)
        return self


    def getItemCol(self):
        """

        Returns:

            str: Column of items
        """
        return self.getOrDefault(self.itemCol)


    def setK(self, value):
        """

        Args:

            k (int): number of items (default: 10)

        """
        self._set(k=value)
        return self


    def getK(self):
        """

        Returns:

            int: number of items (default: 10)
        """
        return self.getOrDefault(self.k)


    def setLabelCol(self, value):
        """

        Args:

            labelCol (str): The name of the label column (default: label)

        """
        self._set(labelCol=value)
        return self


    def getLabelCol(self):
        """

        Returns:

            str: The name of the label column (default: label)
        """
        return self.getOrDefault(self.labelCol)


    def setMinRatingsPerItem(self, value):
        """

        Args:

            minRatingsPerItem (int): min ratings for items > 0 (default: 1)

        """
        self._set(minRatingsPerItem=value)
        return self


    def getMinRatingsPerItem(self):
        """

        Returns:

            int: min ratings for items > 0 (default: 1)
        """
        return self.getOrDefault(self.minRatingsPerItem)


    def setMinRatingsPerUser(self, value):
        """

        Args:

            minRatingsPerUser (int): min ratings for users > 0 (default: 1)

        """
        self._set(minRatingsPerUser=value)
        return self


    def getMinRatingsPerUser(self):
        """

        Returns:

            int: min ratings for users > 0 (default: 1)
        """
        return self.getOrDefault(self.minRatingsPerUser)


    def setMode(self, value):
        """

        Args:

            mode (str): recommendation mode (default: allUsers)

        """
        self._set(mode=value)
        return self


    def getMode(self):
        """

        Returns:

            str: recommendation mode (default: allUsers)
        """
        return self.getOrDefault(self.mode)


    def setRatingCol(self, value):
        """

        Args:

            ratingCol (str): Column of ratings

        """
        self._set(ratingCol=value)
        return self


    def getRatingCol(self):
        """

        Returns:

            str: Column of ratings
        """
        return self.getOrDefault(self.ratingCol)


    def setRecommender(self, value):
        """

        Args:

            recommender (object): estimator for selection

        """
        self._set(recommender=value)
        return self


    def getRecommender(self):
        """

        Returns:

            object: estimator for selection
        """
        return self._cache.get("recommender", None)


    def setRecommenderModel(self, value):
        """

        Args:

            recommenderModel (object): recommenderModel

        """
        self._set(recommenderModel=value)
        return self


    def getRecommenderModel(self):
        """

        Returns:

            object: recommenderModel
        """
        return self._cache.get("recommenderModel", None)


    def setUserCol(self, value):
        """

        Args:

            userCol (str): Column of users

        """
        self._set(userCol=value)
        return self


    def getUserCol(self):
        """

        Returns:

            str: Column of users
        """
        return self.getOrDefault(self.userCol)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.RankingAdapterModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=RankingAdapterModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".RankingAdapterModel"
        return from_java(java_stage, module_name)
