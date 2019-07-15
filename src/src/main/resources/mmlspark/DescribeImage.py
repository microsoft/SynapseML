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
class DescribeImage(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        concurrency (int): max number of concurrent calls (default: 1)
        concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
        errorCol (str): column to hold http errors (default: [self.uid]_error)
        handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
        imageBytes (object): bytestream of the image to use
        imageUrl (object): the url of the image to use
        language (object): Language of image description (default: ServiceParamData(None,Some(en)))
        maxCandidates (object): Maximum candidate descriptions to return (default: ServiceParamData(None,Some(1)))
        outputCol (str): The name of the output column (default: [self.uid]_output)
        subscriptionKey (object): the API key to use
        timeout (double): number of seconds to wait before closing the connection (default: 60.0)
        url (str): Url of the service
    """

    @keyword_only
    def __init__(self, concurrency=1, concurrentTimeout=100.0, errorCol=None, handler=None, imageBytes=None, imageUrl=None, language=None, maxCandidates=None, outputCol=None, subscriptionKey=None, timeout=60.0, url=None):
        super(DescribeImage, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.DescribeImage")
        self._cache = {}
        self.concurrency = Param(self, "concurrency", "concurrency: max number of concurrent calls (default: 1)")
        self._setDefault(concurrency=1)
        self.concurrentTimeout = Param(self, "concurrentTimeout", "concurrentTimeout: max number seconds to wait on futures if concurrency >= 1 (default: 100.0)")
        self._setDefault(concurrentTimeout=100.0)
        self.errorCol = Param(self, "errorCol", "errorCol: column to hold http errors (default: [self.uid]_error)")
        self._setDefault(errorCol=self.uid + "_error")
        self.handler = Param(self, "handler", "handler: Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))", generateTypeConverter("handler", self._cache, complexTypeConverter))
        self.imageBytes = Param(self, "imageBytes", "imageBytes: bytestream of the image to use")
        self.imageUrl = Param(self, "imageUrl", "imageUrl: the url of the image to use")
        self.language = Param(self, "language", "language: Language of image description (default: ServiceParamData(None,Some(en)))")
        self.maxCandidates = Param(self, "maxCandidates", "maxCandidates: Maximum candidate descriptions to return (default: ServiceParamData(None,Some(1)))")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        self.subscriptionKey = Param(self, "subscriptionKey", "subscriptionKey: the API key to use")
        self.timeout = Param(self, "timeout", "timeout: number of seconds to wait before closing the connection (default: 60.0)")
        self._setDefault(timeout=60.0)
        self.url = Param(self, "url", "url: Url of the service")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, concurrency=1, concurrentTimeout=100.0, errorCol=None, handler=None, imageBytes=None, imageUrl=None, language=None, maxCandidates=None, outputCol=None, subscriptionKey=None, timeout=60.0, url=None):
        """
        Set the (keyword only) parameters

        Args:

            concurrency (int): max number of concurrent calls (default: 1)
            concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
            errorCol (str): column to hold http errors (default: [self.uid]_error)
            handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
            imageBytes (object): bytestream of the image to use
            imageUrl (object): the url of the image to use
            language (object): Language of image description (default: ServiceParamData(None,Some(en)))
            maxCandidates (object): Maximum candidate descriptions to return (default: ServiceParamData(None,Some(1)))
            outputCol (str): The name of the output column (default: [self.uid]_output)
            subscriptionKey (object): the API key to use
            timeout (double): number of seconds to wait before closing the connection (default: 60.0)
            url (str): Url of the service
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

            errorCol (str): column to hold http errors (default: [self.uid]_error)

        """
        self._set(errorCol=value)
        return self


    def getErrorCol(self):
        """

        Returns:

            str: column to hold http errors (default: [self.uid]_error)
        """
        return self.getOrDefault(self.errorCol)


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


    def setImageBytes(self, value):
        """

        Args:

            imageBytes (object): bytestream of the image to use

        """
        self._java_obj = self._java_obj.setImageBytes(value)
        return self


    def setImageBytesCol(self, value):
        """

        Args:

            imageBytes (object): bytestream of the image to use

        """
        self._java_obj = self._java_obj.setImageBytesCol(value)
        return self




    def getImageBytes(self):
        """

        Returns:

            object: bytestream of the image to use
        """
        return self._cache.get("imageBytes", None)


    def setImageUrl(self, value):
        """

        Args:

            imageUrl (object): the url of the image to use

        """
        self._java_obj = self._java_obj.setImageUrl(value)
        return self


    def setImageUrlCol(self, value):
        """

        Args:

            imageUrl (object): the url of the image to use

        """
        self._java_obj = self._java_obj.setImageUrlCol(value)
        return self




    def getImageUrl(self):
        """

        Returns:

            object: the url of the image to use
        """
        return self._cache.get("imageUrl", None)


    def setLanguage(self, value):
        """

        Args:

            language (object): Language of image description (default: ServiceParamData(None,Some(en)))

        """
        self._java_obj = self._java_obj.setLanguage(value)
        return self


    def setLanguageCol(self, value):
        """

        Args:

            language (object): Language of image description (default: ServiceParamData(None,Some(en)))

        """
        self._java_obj = self._java_obj.setLanguageCol(value)
        return self




    def getLanguage(self):
        """

        Returns:

            object: Language of image description (default: ServiceParamData(None,Some(en)))
        """
        return self._cache.get("language", None)


    def setMaxCandidates(self, value):
        """

        Args:

            maxCandidates (object): Maximum candidate descriptions to return (default: ServiceParamData(None,Some(1)))

        """
        self._java_obj = self._java_obj.setMaxCandidates(value)
        return self


    def setMaxCandidatesCol(self, value):
        """

        Args:

            maxCandidates (object): Maximum candidate descriptions to return (default: ServiceParamData(None,Some(1)))

        """
        self._java_obj = self._java_obj.setMaxCandidatesCol(value)
        return self




    def getMaxCandidates(self):
        """

        Returns:

            object: Maximum candidate descriptions to return (default: ServiceParamData(None,Some(1)))
        """
        return self._cache.get("maxCandidates", None)


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


    def setSubscriptionKey(self, value):
        """

        Args:

            subscriptionKey (object): the API key to use

        """
        self._java_obj = self._java_obj.setSubscriptionKey(value)
        return self


    def setSubscriptionKeyCol(self, value):
        """

        Args:

            subscriptionKey (object): the API key to use

        """
        self._java_obj = self._java_obj.setSubscriptionKeyCol(value)
        return self




    def getSubscriptionKey(self):
        """

        Returns:

            object: the API key to use
        """
        return self._cache.get("subscriptionKey", None)


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


    def setUrl(self, value):
        """

        Args:

            url (str): Url of the service

        """
        self._set(url=value)
        return self


    def getUrl(self):
        """

        Returns:

            str: Url of the service
        """
        return self.getOrDefault(self.url)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.DescribeImage"

    @staticmethod
    def _from_java(java_stage):
        module_name=DescribeImage.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".DescribeImage"
        return from_java(java_stage, module_name)
