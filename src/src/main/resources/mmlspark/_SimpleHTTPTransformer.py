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
class _SimpleHTTPTransformer(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        concurrency (int): max number of concurrent calls (default: 1)
        concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
        errorCol (str): column to hold http errors (default: [self.uid]_errors)
        flattenOutputBatches (bool): whether to flatten the output batches
        handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
        inputCol (str): The name of the input column
        inputParser (object): format to parse the column to (default: JSONInputParser_1e823ee240ad)
        miniBatcher (object): Minibatcher to use
        outputCol (str): The name of the output column
        outputParser (object): format to parse the column to
        timeout (double): number of seconds to wait before closing the connection (default: 60.0)
    """

    @keyword_only
    def __init__(self, concurrency=1, concurrentTimeout=100.0, errorCol=None, flattenOutputBatches=None, handler=None, inputCol=None, inputParser=None, miniBatcher=None, outputCol=None, outputParser=None, timeout=60.0):
        super(_SimpleHTTPTransformer, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.SimpleHTTPTransformer")
        self._cache = {}
        self.concurrency = Param(self, "concurrency", "concurrency: max number of concurrent calls (default: 1)")
        self._setDefault(concurrency=1)
        self.concurrentTimeout = Param(self, "concurrentTimeout", "concurrentTimeout: max number seconds to wait on futures if concurrency >= 1 (default: 100.0)")
        self._setDefault(concurrentTimeout=100.0)
        self.errorCol = Param(self, "errorCol", "errorCol: column to hold http errors (default: [self.uid]_errors)")
        self._setDefault(errorCol=self.uid + "_errors")
        self.flattenOutputBatches = Param(self, "flattenOutputBatches", "flattenOutputBatches: whether to flatten the output batches")
        self.handler = Param(self, "handler", "handler: Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))", generateTypeConverter("handler", self._cache, complexTypeConverter))
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column")
        self.inputParser = Param(self, "inputParser", "inputParser: format to parse the column to (default: JSONInputParser_1e823ee240ad)", generateTypeConverter("inputParser", self._cache, complexTypeConverter))
        self.miniBatcher = Param(self, "miniBatcher", "miniBatcher: Minibatcher to use", generateTypeConverter("miniBatcher", self._cache, complexTypeConverter))
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column")
        self.outputParser = Param(self, "outputParser", "outputParser: format to parse the column to", generateTypeConverter("outputParser", self._cache, complexTypeConverter))
        self.timeout = Param(self, "timeout", "timeout: number of seconds to wait before closing the connection (default: 60.0)")
        self._setDefault(timeout=60.0)
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, concurrency=1, concurrentTimeout=100.0, errorCol=None, flattenOutputBatches=None, handler=None, inputCol=None, inputParser=None, miniBatcher=None, outputCol=None, outputParser=None, timeout=60.0):
        """
        Set the (keyword only) parameters

        Args:

            concurrency (int): max number of concurrent calls (default: 1)
            concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
            errorCol (str): column to hold http errors (default: [self.uid]_errors)
            flattenOutputBatches (bool): whether to flatten the output batches
            handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
            inputCol (str): The name of the input column
            inputParser (object): format to parse the column to (default: JSONInputParser_1e823ee240ad)
            miniBatcher (object): Minibatcher to use
            outputCol (str): The name of the output column
            outputParser (object): format to parse the column to
            timeout (double): number of seconds to wait before closing the connection (default: 60.0)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setConcurrency(self, value):
        """

        Args:

            concurrency (int): max number of concurrent calls (default: 1)

        """
        self._set(concurrency=value)
        return self


    def getConcurrency(self):
        """

        Returns:

            int: max number of concurrent calls (default: 1)
        """
        return self.getOrDefault(self.concurrency)


    def setConcurrentTimeout(self, value):
        """

        Args:

            concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)

        """
        self._set(concurrentTimeout=value)
        return self


    def getConcurrentTimeout(self):
        """

        Returns:

            double: max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
        """
        return self.getOrDefault(self.concurrentTimeout)


    def setErrorCol(self, value):
        """

        Args:

            errorCol (str): column to hold http errors (default: [self.uid]_errors)

        """
        self._set(errorCol=value)
        return self


    def getErrorCol(self):
        """

        Returns:

            str: column to hold http errors (default: [self.uid]_errors)
        """
        return self.getOrDefault(self.errorCol)


    def setFlattenOutputBatches(self, value):
        """

        Args:

            flattenOutputBatches (bool): whether to flatten the output batches

        """
        self._set(flattenOutputBatches=value)
        return self


    def getFlattenOutputBatches(self):
        """

        Returns:

            bool: whether to flatten the output batches
        """
        return self.getOrDefault(self.flattenOutputBatches)


    def setHandler(self, value):
        """

        Args:

            handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))

        """
        self._set(handler=value)
        return self


    def getHandler(self):
        """

        Returns:

            object: Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
        """
        return self._cache.get("handler", None)


    def setInputCol(self, value):
        """

        Args:

            inputCol (str): The name of the input column

        """
        self._set(inputCol=value)
        return self


    def getInputCol(self):
        """

        Returns:

            str: The name of the input column
        """
        return self.getOrDefault(self.inputCol)


    def setInputParser(self, value):
        """

        Args:

            inputParser (object): format to parse the column to (default: JSONInputParser_1e823ee240ad)

        """
        self._set(inputParser=value)
        return self


    def getInputParser(self):
        """

        Returns:

            object: format to parse the column to (default: JSONInputParser_1e823ee240ad)
        """
        return self._cache.get("inputParser", None)


    def setMiniBatcher(self, value):
        """

        Args:

            miniBatcher (object): Minibatcher to use

        """
        self._set(miniBatcher=value)
        return self


    def getMiniBatcher(self):
        """

        Returns:

            object: Minibatcher to use
        """
        return self._cache.get("miniBatcher", None)


    def setOutputCol(self, value):
        """

        Args:

            outputCol (str): The name of the output column

        """
        self._set(outputCol=value)
        return self


    def getOutputCol(self):
        """

        Returns:

            str: The name of the output column
        """
        return self.getOrDefault(self.outputCol)


    def setOutputParser(self, value):
        """

        Args:

            outputParser (object): format to parse the column to

        """
        self._set(outputParser=value)
        return self


    def getOutputParser(self):
        """

        Returns:

            object: format to parse the column to
        """
        return self._cache.get("outputParser", None)


    def setTimeout(self, value):
        """

        Args:

            timeout (double): number of seconds to wait before closing the connection (default: 60.0)

        """
        self._set(timeout=value)
        return self


    def getTimeout(self):
        """

        Returns:

            double: number of seconds to wait before closing the connection (default: 60.0)
        """
        return self.getOrDefault(self.timeout)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.SimpleHTTPTransformer"

    @staticmethod
    def _from_java(java_stage):
        module_name=_SimpleHTTPTransformer.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".SimpleHTTPTransformer"
        return from_java(java_stage, module_name)
