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
class ImageSetAugmenter(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        flipLeftRight (bool): Symmetric Left-Right (default: true)
        flipUpDown (bool): Symmetric Up-Down (default: false)
        inputCol (str): The name of the input column (default: image)
        outputCol (str): The name of the output column (default: [self.uid]_output)
    """

    @keyword_only
    def __init__(self, flipLeftRight=True, flipUpDown=False, inputCol="image", outputCol=None):
        super(ImageSetAugmenter, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.ImageSetAugmenter")
        self.flipLeftRight = Param(self, "flipLeftRight", "flipLeftRight: Symmetric Left-Right (default: true)")
        self._setDefault(flipLeftRight=True)
        self.flipUpDown = Param(self, "flipUpDown", "flipUpDown: Symmetric Up-Down (default: false)")
        self._setDefault(flipUpDown=False)
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column (default: image)")
        self._setDefault(inputCol="image")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, flipLeftRight=True, flipUpDown=False, inputCol="image", outputCol=None):
        """
        Set the (keyword only) parameters

        Args:

            flipLeftRight (bool): Symmetric Left-Right (default: true)
            flipUpDown (bool): Symmetric Up-Down (default: false)
            inputCol (str): The name of the input column (default: image)
            outputCol (str): The name of the output column (default: [self.uid]_output)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setFlipLeftRight(self, value):
        """

        Args:

            flipLeftRight (bool): Symmetric Left-Right (default: true)

        """
        self._set(flipLeftRight=value)
        return self


    def getFlipLeftRight(self):
        """

        Returns:

            bool: Symmetric Left-Right (default: true)
        """
        return self.getOrDefault(self.flipLeftRight)


    def setFlipUpDown(self, value):
        """

        Args:

            flipUpDown (bool): Symmetric Up-Down (default: false)

        """
        self._set(flipUpDown=value)
        return self


    def getFlipUpDown(self):
        """

        Returns:

            bool: Symmetric Up-Down (default: false)
        """
        return self.getOrDefault(self.flipUpDown)


    def setInputCol(self, value):
        """

        Args:

            inputCol (str): The name of the input column (default: image)

        """
        self._set(inputCol=value)
        return self


    def getInputCol(self):
        """

        Returns:

            str: The name of the input column (default: image)
        """
        return self.getOrDefault(self.inputCol)


    def setOutputCol(self, value):
        """

        Args:

            outputCol (str): The name of the output column (default: [self.uid]_output)

        """
        self._set(outputCol=value)
        return self


    def getOutputCol(self):
        """

        Returns:

            str: The name of the output column (default: [self.uid]_output)
        """
        return self.getOrDefault(self.outputCol)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.ImageSetAugmenter"

    @staticmethod
    def _from_java(java_stage):
        module_name=ImageSetAugmenter.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".ImageSetAugmenter"
        return from_java(java_stage, module_name)
