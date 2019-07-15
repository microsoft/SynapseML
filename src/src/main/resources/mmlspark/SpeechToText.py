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
class SpeechToText(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        audioData (object): The data sent to the service must be a .wav files
        concurrency (int): max number of concurrent calls (default: 1)
        concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
        errorCol (str): column to hold http errors (default: [self.uid]_error)
        format (object): Specifies the result format. Accepted values are simple and detailed. Default is simple.
        handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
        language (object): Identifies the spoken language that is being recognized.
        outputCol (str): The name of the output column (default: [self.uid]_output)
        profanity (object): Specifies how to handle profanity in recognition results.Accepted values are masked, which replaces profanity with asterisks,removed, which remove all profanity from the result, or raw,which includes the profanity in the result. The default setting is masked.
        subscriptionKey (object): the API key to use
        timeout (double): number of seconds to wait before closing the connection (default: 60.0)
        url (str): Url of the service
    """

    @keyword_only
    def __init__(self, audioData=None, concurrency=1, concurrentTimeout=100.0, errorCol=None, format=None, handler=None, language=None, outputCol=None, profanity=None, subscriptionKey=None, timeout=60.0, url=None):
        super(SpeechToText, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.SpeechToText")
        self._cache = {}
        self.audioData = Param(self, "audioData", "audioData: The data sent to the service must be a .wav files")
        self.concurrency = Param(self, "concurrency", "concurrency: max number of concurrent calls (default: 1)")
        self._setDefault(concurrency=1)
        self.concurrentTimeout = Param(self, "concurrentTimeout", "concurrentTimeout: max number seconds to wait on futures if concurrency >= 1 (default: 100.0)")
        self._setDefault(concurrentTimeout=100.0)
        self.errorCol = Param(self, "errorCol", "errorCol: column to hold http errors (default: [self.uid]_error)")
        self._setDefault(errorCol=self.uid + "_error")
        self.format = Param(self, "format", "format: Specifies the result format. Accepted values are simple and detailed. Default is simple.")
        self.handler = Param(self, "handler", "handler: Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))", generateTypeConverter("handler", self._cache, complexTypeConverter))
        self.language = Param(self, "language", "language: Identifies the spoken language that is being recognized.")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        self.profanity = Param(self, "profanity", "profanity: Specifies how to handle profanity in recognition results.Accepted values are masked, which replaces profanity with asterisks,removed, which remove all profanity from the result, or raw,which includes the profanity in the result. The default setting is masked.")
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
    def setParams(self, audioData=None, concurrency=1, concurrentTimeout=100.0, errorCol=None, format=None, handler=None, language=None, outputCol=None, profanity=None, subscriptionKey=None, timeout=60.0, url=None):
        """
        Set the (keyword only) parameters

        Args:

            audioData (object): The data sent to the service must be a .wav files
            concurrency (int): max number of concurrent calls (default: 1)
            concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
            errorCol (str): column to hold http errors (default: [self.uid]_error)
            format (object): Specifies the result format. Accepted values are simple and detailed. Default is simple.
            handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
            language (object): Identifies the spoken language that is being recognized.
            outputCol (str): The name of the output column (default: [self.uid]_output)
            profanity (object): Specifies how to handle profanity in recognition results.Accepted values are masked, which replaces profanity with asterisks,removed, which remove all profanity from the result, or raw,which includes the profanity in the result. The default setting is masked.
            subscriptionKey (object): the API key to use
            timeout (double): number of seconds to wait before closing the connection (default: 60.0)
            url (str): Url of the service
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setAudioData(self, value):
        """

        Args:

            audioData (object): The data sent to the service must be a .wav files

        """
        self._java_obj = self._java_obj.setAudioData(value)
        return self


    def setAudioDataCol(self, value):
        """

        Args:

            audioData (object): The data sent to the service must be a .wav files

        """
        self._java_obj = self._java_obj.setAudioDataCol(value)
        return self




    def getAudioData(self):
        """

        Returns:

            object: The data sent to the service must be a .wav files
        """
        return self._cache.get("audioData", None)


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


    def setFormat(self, value):
        """

        Args:

            format (object): Specifies the result format. Accepted values are simple and detailed. Default is simple.

        """
        self._java_obj = self._java_obj.setFormat(value)
        return self


    def setFormatCol(self, value):
        """

        Args:

            format (object): Specifies the result format. Accepted values are simple and detailed. Default is simple.

        """
        self._java_obj = self._java_obj.setFormatCol(value)
        return self




    def getFormat(self):
        """

        Returns:

            object: Specifies the result format. Accepted values are simple and detailed. Default is simple.
        """
        return self._cache.get("format", None)


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


    def setLanguage(self, value):
        """

        Args:

            language (object): Identifies the spoken language that is being recognized.

        """
        self._java_obj = self._java_obj.setLanguage(value)
        return self


    def setLanguageCol(self, value):
        """

        Args:

            language (object): Identifies the spoken language that is being recognized.

        """
        self._java_obj = self._java_obj.setLanguageCol(value)
        return self




    def getLanguage(self):
        """

        Returns:

            object: Identifies the spoken language that is being recognized.
        """
        return self._cache.get("language", None)


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


    def setProfanity(self, value):
        """

        Args:

            profanity (object): Specifies how to handle profanity in recognition results.Accepted values are masked, which replaces profanity with asterisks,removed, which remove all profanity from the result, or raw,which includes the profanity in the result. The default setting is masked.

        """
        self._java_obj = self._java_obj.setProfanity(value)
        return self


    def setProfanityCol(self, value):
        """

        Args:

            profanity (object): Specifies how to handle profanity in recognition results.Accepted values are masked, which replaces profanity with asterisks,removed, which remove all profanity from the result, or raw,which includes the profanity in the result. The default setting is masked.

        """
        self._java_obj = self._java_obj.setProfanityCol(value)
        return self




    def getProfanity(self):
        """

        Returns:

            object: Specifies how to handle profanity in recognition results.Accepted values are masked, which replaces profanity with asterisks,removed, which remove all profanity from the result, or raw,which includes the profanity in the result. The default setting is masked.
        """
        return self._cache.get("profanity", None)


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
        return "com.microsoft.ml.spark.SpeechToText"

    @staticmethod
    def _from_java(java_stage):
        module_name=SpeechToText.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".SpeechToText"
        return from_java(java_stage, module_name)
