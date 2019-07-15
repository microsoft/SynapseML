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
class AddDocuments(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        actionCol (str): You can combine actions, such as an upload and a delete, in the same batch.upload: An upload action is similar to an 'upsert'where the document will be inserted if it is new and updated/replacedif it exists. Note that all fields are replaced in the update case.merge: Merge updates an existing document with the specified fields.If the document doesn't exist, the merge will fail. Any fieldyou specify in a merge will replace the existing field in the document.This includes fields of type Collection(Edm.String). For example, ifthe document contains a field 'tags' with value ['budget'] and you executea merge with value ['economy', 'pool'] for 'tags', the final valueof the 'tags' field will be ['economy', 'pool']. It will not be ['budget', 'economy', 'pool'].mergeOrUpload: This action behaves like merge if a document with the given key already exists in the index. If the document does not exist, it behaves like upload with a new document.delete: Delete removes the specified document from the index. Note that any field you specify in a delete operation, other than the key field, will be ignored. If you want to  remove an individual field from a document, use merge  instead and simply set the field explicitly to null.     (default: @search.action)
        batchSize (int): The max size of the buffer (default: 100)
        concurrency (int): max number of concurrent calls (default: 1)
        concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
        errorCol (str): column to hold http errors (default: [self.uid]_error)
        handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
        indexName (str): 
        outputCol (str): The name of the output column (default: [self.uid]_output)
        serviceName (str): 
        subscriptionKey (object): the API key to use
        timeout (double): number of seconds to wait before closing the connection (default: 60.0)
        url (str): Url of the service
    """

    @keyword_only
    def __init__(self, actionCol="@search.action", batchSize=100, concurrency=1, concurrentTimeout=100.0, errorCol=None, handler=None, indexName=None, outputCol=None, serviceName=None, subscriptionKey=None, timeout=60.0, url=None):
        super(AddDocuments, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.AddDocuments")
        self._cache = {}
        self.actionCol = Param(self, "actionCol", "actionCol: You can combine actions, such as an upload and a delete, in the same batch.upload: An upload action is similar to an 'upsert'where the document will be inserted if it is new and updated/replacedif it exists. Note that all fields are replaced in the update case.merge: Merge updates an existing document with the specified fields.If the document doesn't exist, the merge will fail. Any fieldyou specify in a merge will replace the existing field in the document.This includes fields of type Collection(Edm.String). For example, ifthe document contains a field 'tags' with value ['budget'] and you executea merge with value ['economy', 'pool'] for 'tags', the final valueof the 'tags' field will be ['economy', 'pool']. It will not be ['budget', 'economy', 'pool'].mergeOrUpload: This action behaves like merge if a document with the given key already exists in the index. If the document does not exist, it behaves like upload with a new document.delete: Delete removes the specified document from the index. Note that any field you specify in a delete operation, other than the key field, will be ignored. If you want to  remove an individual field from a document, use merge  instead and simply set the field explicitly to null.     (default: @search.action)")
        self._setDefault(actionCol="@search.action")
        self.batchSize = Param(self, "batchSize", "batchSize: The max size of the buffer (default: 100)")
        self._setDefault(batchSize=100)
        self.concurrency = Param(self, "concurrency", "concurrency: max number of concurrent calls (default: 1)")
        self._setDefault(concurrency=1)
        self.concurrentTimeout = Param(self, "concurrentTimeout", "concurrentTimeout: max number seconds to wait on futures if concurrency >= 1 (default: 100.0)")
        self._setDefault(concurrentTimeout=100.0)
        self.errorCol = Param(self, "errorCol", "errorCol: column to hold http errors (default: [self.uid]_error)")
        self._setDefault(errorCol=self.uid + "_error")
        self.handler = Param(self, "handler", "handler: Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))", generateTypeConverter("handler", self._cache, complexTypeConverter))
        self.indexName = Param(self, "indexName", "indexName:")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        self.serviceName = Param(self, "serviceName", "serviceName:")
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
    def setParams(self, actionCol="@search.action", batchSize=100, concurrency=1, concurrentTimeout=100.0, errorCol=None, handler=None, indexName=None, outputCol=None, serviceName=None, subscriptionKey=None, timeout=60.0, url=None):
        """
        Set the (keyword only) parameters

        Args:

            actionCol (str): You can combine actions, such as an upload and a delete, in the same batch.upload: An upload action is similar to an 'upsert'where the document will be inserted if it is new and updated/replacedif it exists. Note that all fields are replaced in the update case.merge: Merge updates an existing document with the specified fields.If the document doesn't exist, the merge will fail. Any fieldyou specify in a merge will replace the existing field in the document.This includes fields of type Collection(Edm.String). For example, ifthe document contains a field 'tags' with value ['budget'] and you executea merge with value ['economy', 'pool'] for 'tags', the final valueof the 'tags' field will be ['economy', 'pool']. It will not be ['budget', 'economy', 'pool'].mergeOrUpload: This action behaves like merge if a document with the given key already exists in the index. If the document does not exist, it behaves like upload with a new document.delete: Delete removes the specified document from the index. Note that any field you specify in a delete operation, other than the key field, will be ignored. If you want to  remove an individual field from a document, use merge  instead and simply set the field explicitly to null.     (default: @search.action)
            batchSize (int): The max size of the buffer (default: 100)
            concurrency (int): max number of concurrent calls (default: 1)
            concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
            errorCol (str): column to hold http errors (default: [self.uid]_error)
            handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
            indexName (str): 
            outputCol (str): The name of the output column (default: [self.uid]_output)
            serviceName (str): 
            subscriptionKey (object): the API key to use
            timeout (double): number of seconds to wait before closing the connection (default: 60.0)
            url (str): Url of the service
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setActionCol(self, value):
        """

        Args:

            actionCol (str): You can combine actions, such as an upload and a delete, in the same batch.upload: An upload action is similar to an 'upsert'where the document will be inserted if it is new and updated/replacedif it exists. Note that all fields are replaced in the update case.merge: Merge updates an existing document with the specified fields.If the document doesn't exist, the merge will fail. Any fieldyou specify in a merge will replace the existing field in the document.This includes fields of type Collection(Edm.String). For example, ifthe document contains a field 'tags' with value ['budget'] and you executea merge with value ['economy', 'pool'] for 'tags', the final valueof the 'tags' field will be ['economy', 'pool']. It will not be ['budget', 'economy', 'pool'].mergeOrUpload: This action behaves like merge if a document with the given key already exists in the index. If the document does not exist, it behaves like upload with a new document.delete: Delete removes the specified document from the index. Note that any field you specify in a delete operation, other than the key field, will be ignored. If you want to  remove an individual field from a document, use merge  instead and simply set the field explicitly to null.     (default: @search.action)

        """
        self._set(actionCol=value)
        return self


    def getActionCol(self):
        """

        Returns:

            str: You can combine actions, such as an upload and a delete, in the same batch.upload: An upload action is similar to an 'upsert'where the document will be inserted if it is new and updated/replacedif it exists. Note that all fields are replaced in the update case.merge: Merge updates an existing document with the specified fields.If the document doesn't exist, the merge will fail. Any fieldyou specify in a merge will replace the existing field in the document.This includes fields of type Collection(Edm.String). For example, ifthe document contains a field 'tags' with value ['budget'] and you executea merge with value ['economy', 'pool'] for 'tags', the final valueof the 'tags' field will be ['economy', 'pool']. It will not be ['budget', 'economy', 'pool'].mergeOrUpload: This action behaves like merge if a document with the given key already exists in the index. If the document does not exist, it behaves like upload with a new document.delete: Delete removes the specified document from the index. Note that any field you specify in a delete operation, other than the key field, will be ignored. If you want to  remove an individual field from a document, use merge  instead and simply set the field explicitly to null.     (default: @search.action)
        """
        return self.getOrDefault(self.actionCol)


    def setBatchSize(self, value):
        """

        Args:

            batchSize (int): The max size of the buffer (default: 100)

        """
        self._set(batchSize=value)
        return self


    def getBatchSize(self):
        """

        Returns:

            int: The max size of the buffer (default: 100)
        """
        return self.getOrDefault(self.batchSize)


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


    def setIndexName(self, value):
        """

        Args:

            indexName (str): 

        """
        self._set(indexName=value)
        return self


    def getIndexName(self):
        """

        Returns:

            str: 
        """
        return self.getOrDefault(self.indexName)


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


    def setServiceName(self, value):
        """

        Args:

            serviceName (str): 

        """
        self._set(serviceName=value)
        return self


    def getServiceName(self):
        """

        Returns:

            str: 
        """
        return self.getOrDefault(self.serviceName)


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
        return "com.microsoft.ml.spark.AddDocuments"

    @staticmethod
    def _from_java(java_stage):
        module_name=AddDocuments.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".AddDocuments"
        return from_java(java_stage, module_name)
