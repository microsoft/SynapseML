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
class FindSimilarFace(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        concurrency (int): max number of concurrent calls (default: 1)
        concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
        errorCol (str): column to hold http errors (default: [self.uid]_error)
        faceId (object): faceId of the query face. User needs to call FaceDetect first to get a valid faceId. Note that this faceId is not persisted and will expire 24 hours after the detection call.
        faceIds (object): An array of candidate faceIds. All of them are created by FaceDetect and the faceIds will expire 24 hours after the detection call. The number of faceIds is limited to 1000. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.
        faceListId (object): An existing user-specified unique candidate face list, created in FaceList - Create. Face list contains a set of persistedFaceIds which are persisted and will never expire. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.
        handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
        largeFaceListId (object): An existing user-specified unique candidate large face list, created in LargeFaceList - Create. Large face list contains a set of persistedFaceIds which are persisted and will never expire. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.
        maxNumOfCandidatesReturned (object): Optional parameter. The number of top similar faces returned. The valid range is [1, 1000].It defaults to 20.
        mode (object): Optional parameter. Similar face searching mode. It can be 'matchPerson' or 'matchFace'. It defaults to 'matchPerson'.
        outputCol (str): The name of the output column (default: [self.uid]_output)
        subscriptionKey (object): the API key to use
        timeout (double): number of seconds to wait before closing the connection (default: 60.0)
        url (str): Url of the service
    """

    @keyword_only
    def __init__(self, concurrency=1, concurrentTimeout=100.0, errorCol=None, faceId=None, faceIds=None, faceListId=None, handler=None, largeFaceListId=None, maxNumOfCandidatesReturned=None, mode=None, outputCol=None, subscriptionKey=None, timeout=60.0, url=None):
        super(FindSimilarFace, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.FindSimilarFace")
        self._cache = {}
        self.concurrency = Param(self, "concurrency", "concurrency: max number of concurrent calls (default: 1)")
        self._setDefault(concurrency=1)
        self.concurrentTimeout = Param(self, "concurrentTimeout", "concurrentTimeout: max number seconds to wait on futures if concurrency >= 1 (default: 100.0)")
        self._setDefault(concurrentTimeout=100.0)
        self.errorCol = Param(self, "errorCol", "errorCol: column to hold http errors (default: [self.uid]_error)")
        self._setDefault(errorCol=self.uid + "_error")
        self.faceId = Param(self, "faceId", "faceId: faceId of the query face. User needs to call FaceDetect first to get a valid faceId. Note that this faceId is not persisted and will expire 24 hours after the detection call.")
        self.faceIds = Param(self, "faceIds", "faceIds:  An array of candidate faceIds. All of them are created by FaceDetect and the faceIds will expire 24 hours after the detection call. The number of faceIds is limited to 1000. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.")
        self.faceListId = Param(self, "faceListId", "faceListId:  An existing user-specified unique candidate face list, created in FaceList - Create. Face list contains a set of persistedFaceIds which are persisted and will never expire. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.")
        self.handler = Param(self, "handler", "handler: Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))", generateTypeConverter("handler", self._cache, complexTypeConverter))
        self.largeFaceListId = Param(self, "largeFaceListId", "largeFaceListId:  An existing user-specified unique candidate large face list, created in LargeFaceList - Create. Large face list contains a set of persistedFaceIds which are persisted and will never expire. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.")
        self.maxNumOfCandidatesReturned = Param(self, "maxNumOfCandidatesReturned", "maxNumOfCandidatesReturned:  Optional parameter. The number of top similar faces returned. The valid range is [1, 1000].It defaults to 20.")
        self.mode = Param(self, "mode", "mode:  Optional parameter. Similar face searching mode. It can be 'matchPerson' or 'matchFace'. It defaults to 'matchPerson'.")
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
    def setParams(self, concurrency=1, concurrentTimeout=100.0, errorCol=None, faceId=None, faceIds=None, faceListId=None, handler=None, largeFaceListId=None, maxNumOfCandidatesReturned=None, mode=None, outputCol=None, subscriptionKey=None, timeout=60.0, url=None):
        """
        Set the (keyword only) parameters

        Args:

            concurrency (int): max number of concurrent calls (default: 1)
            concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
            errorCol (str): column to hold http errors (default: [self.uid]_error)
            faceId (object): faceId of the query face. User needs to call FaceDetect first to get a valid faceId. Note that this faceId is not persisted and will expire 24 hours after the detection call.
            faceIds (object): An array of candidate faceIds. All of them are created by FaceDetect and the faceIds will expire 24 hours after the detection call. The number of faceIds is limited to 1000. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.
            faceListId (object): An existing user-specified unique candidate face list, created in FaceList - Create. Face list contains a set of persistedFaceIds which are persisted and will never expire. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.
            handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
            largeFaceListId (object): An existing user-specified unique candidate large face list, created in LargeFaceList - Create. Large face list contains a set of persistedFaceIds which are persisted and will never expire. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.
            maxNumOfCandidatesReturned (object): Optional parameter. The number of top similar faces returned. The valid range is [1, 1000].It defaults to 20.
            mode (object): Optional parameter. Similar face searching mode. It can be 'matchPerson' or 'matchFace'. It defaults to 'matchPerson'.
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


    def setFaceId(self, value):
        """

        Args:

            faceId (object): faceId of the query face. User needs to call FaceDetect first to get a valid faceId. Note that this faceId is not persisted and will expire 24 hours after the detection call.

        """
        self._java_obj = self._java_obj.setFaceId(value)
        return self


    def setFaceIdCol(self, value):
        """

        Args:

            faceId (object): faceId of the query face. User needs to call FaceDetect first to get a valid faceId. Note that this faceId is not persisted and will expire 24 hours after the detection call.

        """
        self._java_obj = self._java_obj.setFaceIdCol(value)
        return self




    def getFaceId(self):
        """

        Returns:

            object: faceId of the query face. User needs to call FaceDetect first to get a valid faceId. Note that this faceId is not persisted and will expire 24 hours after the detection call.
        """
        return self._cache.get("faceId", None)


    def setFaceIds(self, value):
        """

        Args:

            faceIds (object): An array of candidate faceIds. All of them are created by FaceDetect and the faceIds will expire 24 hours after the detection call. The number of faceIds is limited to 1000. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.

        """
        self._java_obj = self._java_obj.setFaceIds(value)
        return self


    def setFaceIdsCol(self, value):
        """

        Args:

            faceIds (object): An array of candidate faceIds. All of them are created by FaceDetect and the faceIds will expire 24 hours after the detection call. The number of faceIds is limited to 1000. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.

        """
        self._java_obj = self._java_obj.setFaceIdsCol(value)
        return self




    def getFaceIds(self):
        """

        Returns:

            object: An array of candidate faceIds. All of them are created by FaceDetect and the faceIds will expire 24 hours after the detection call. The number of faceIds is limited to 1000. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.
        """
        return self._cache.get("faceIds", None)


    def setFaceListId(self, value):
        """

        Args:

            faceListId (object): An existing user-specified unique candidate face list, created in FaceList - Create. Face list contains a set of persistedFaceIds which are persisted and will never expire. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.

        """
        self._java_obj = self._java_obj.setFaceListId(value)
        return self


    def setFaceListIdCol(self, value):
        """

        Args:

            faceListId (object): An existing user-specified unique candidate face list, created in FaceList - Create. Face list contains a set of persistedFaceIds which are persisted and will never expire. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.

        """
        self._java_obj = self._java_obj.setFaceListIdCol(value)
        return self




    def getFaceListId(self):
        """

        Returns:

            object: An existing user-specified unique candidate face list, created in FaceList - Create. Face list contains a set of persistedFaceIds which are persisted and will never expire. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.
        """
        return self._cache.get("faceListId", None)


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


    def setLargeFaceListId(self, value):
        """

        Args:

            largeFaceListId (object): An existing user-specified unique candidate large face list, created in LargeFaceList - Create. Large face list contains a set of persistedFaceIds which are persisted and will never expire. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.

        """
        self._java_obj = self._java_obj.setLargeFaceListId(value)
        return self


    def setLargeFaceListIdCol(self, value):
        """

        Args:

            largeFaceListId (object): An existing user-specified unique candidate large face list, created in LargeFaceList - Create. Large face list contains a set of persistedFaceIds which are persisted and will never expire. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.

        """
        self._java_obj = self._java_obj.setLargeFaceListIdCol(value)
        return self




    def getLargeFaceListId(self):
        """

        Returns:

            object: An existing user-specified unique candidate large face list, created in LargeFaceList - Create. Large face list contains a set of persistedFaceIds which are persisted and will never expire. Parameter faceListId, largeFaceListId and faceIds should not be provided at the same time.
        """
        return self._cache.get("largeFaceListId", None)


    def setMaxNumOfCandidatesReturned(self, value):
        """

        Args:

            maxNumOfCandidatesReturned (object): Optional parameter. The number of top similar faces returned. The valid range is [1, 1000].It defaults to 20.

        """
        self._java_obj = self._java_obj.setMaxNumOfCandidatesReturned(value)
        return self


    def setMaxNumOfCandidatesReturnedCol(self, value):
        """

        Args:

            maxNumOfCandidatesReturned (object): Optional parameter. The number of top similar faces returned. The valid range is [1, 1000].It defaults to 20.

        """
        self._java_obj = self._java_obj.setMaxNumOfCandidatesReturnedCol(value)
        return self




    def getMaxNumOfCandidatesReturned(self):
        """

        Returns:

            object: Optional parameter. The number of top similar faces returned. The valid range is [1, 1000].It defaults to 20.
        """
        return self._cache.get("maxNumOfCandidatesReturned", None)


    def setMode(self, value):
        """

        Args:

            mode (object): Optional parameter. Similar face searching mode. It can be 'matchPerson' or 'matchFace'. It defaults to 'matchPerson'.

        """
        self._java_obj = self._java_obj.setMode(value)
        return self


    def setModeCol(self, value):
        """

        Args:

            mode (object): Optional parameter. Similar face searching mode. It can be 'matchPerson' or 'matchFace'. It defaults to 'matchPerson'.

        """
        self._java_obj = self._java_obj.setModeCol(value)
        return self




    def getMode(self):
        """

        Returns:

            object: Optional parameter. Similar face searching mode. It can be 'matchPerson' or 'matchFace'. It defaults to 'matchPerson'.
        """
        return self._cache.get("mode", None)


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
        return "com.microsoft.ml.spark.FindSimilarFace"

    @staticmethod
    def _from_java(java_stage):
        module_name=FindSimilarFace.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".FindSimilarFace"
        return from_java(java_stage, module_name)
