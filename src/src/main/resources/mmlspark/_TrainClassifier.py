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
class _TrainClassifier(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaEstimator):
    """


    Args:

        featuresCol (str): The name of the features column (default: [self.uid]_features)
        labelCol (str): The name of the label column
        labels (list): Sorted label values on the labels column
        model (object): Classifier to run
        numFeatures (int): Number of features to hash to (default: 0)
        reindexLabel (bool): Re-index the label column (default: true)
    """

    @keyword_only
    def __init__(self, featuresCol=None, labelCol=None, labels=None, model=None, numFeatures=0, reindexLabel=True):
        super(_TrainClassifier, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.TrainClassifier")
        self._cache = {}
        self.featuresCol = Param(self, "featuresCol", "featuresCol: The name of the features column (default: [self.uid]_features)")
        self._setDefault(featuresCol=self.uid + "_features")
        self.labelCol = Param(self, "labelCol", "labelCol: The name of the label column")
        self.labels = Param(self, "labels", "labels: Sorted label values on the labels column")
        self.model = Param(self, "model", "model: Classifier to run", generateTypeConverter("model", self._cache, complexTypeConverter))
        self.numFeatures = Param(self, "numFeatures", "numFeatures: Number of features to hash to (default: 0)")
        self._setDefault(numFeatures=0)
        self.reindexLabel = Param(self, "reindexLabel", "reindexLabel: Re-index the label column (default: true)")
        self._setDefault(reindexLabel=True)
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, featuresCol=None, labelCol=None, labels=None, model=None, numFeatures=0, reindexLabel=True):
        """
        Set the (keyword only) parameters

        Args:

            featuresCol (str): The name of the features column (default: [self.uid]_features)
            labelCol (str): The name of the label column
            labels (list): Sorted label values on the labels column
            model (object): Classifier to run
            numFeatures (int): Number of features to hash to (default: 0)
            reindexLabel (bool): Re-index the label column (default: true)
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


    def setLabels(self, value):
        """

        Args:

            labels (list): Sorted label values on the labels column

        """
        self._set(labels=value)
        return self


    def getLabels(self):
        """

        Returns:

            list: Sorted label values on the labels column
        """
        return self.getOrDefault(self.labels)


    def setModel(self, value):
        """

        Args:

            model (object): Classifier to run

        """
        self._set(model=value)
        return self


    def getModel(self):
        """

        Returns:

            object: Classifier to run
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


    def setReindexLabel(self, value):
        """

        Args:

            reindexLabel (bool): Re-index the label column (default: true)

        """
        self._set(reindexLabel=value)
        return self


    def getReindexLabel(self):
        """

        Returns:

            bool: Re-index the label column (default: true)
        """
        return self.getOrDefault(self.reindexLabel)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.TrainClassifier"

    @staticmethod
    def _from_java(java_stage):
        module_name=_TrainClassifier.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".TrainClassifier"
        return from_java(java_stage, module_name)

    def _create_model(self, java_model):
        return _TrainedClassifierModel(java_model)


class _TrainedClassifierModel(ComplexParamsMixin, JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by :class:`_TrainClassifier`.

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
        return "com.microsoft.ml.spark.TrainedClassifierModel"

    @staticmethod
    def _from_java(java_stage):
        module_name=_TrainedClassifierModel.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".TrainedClassifierModel"
        return from_java(java_stage, module_name)

