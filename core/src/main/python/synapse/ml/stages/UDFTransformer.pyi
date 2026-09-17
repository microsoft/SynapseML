# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from typing import Any, List, Optional

from pyspark.ml.param import Param
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaTransformer
from pyspark.sql.functions import UserDefinedFunction
from synapse.ml.core.serialize.java_params_patch import ComplexParamsMixin

class UDFTransformer(
    ComplexParamsMixin,
    JavaMLReadable,
    JavaMLWritable,
    JavaTransformer,
):
    inputCol: Param
    inputCols: Param
    outputCol: Param
    udf: Param

    def __init__(
        self,
        *,
        inputCol: Optional[str] = ...,
        inputCols: Optional[List[str]] = ...,
        outputCol: Optional[str] = ...,
        udf: Optional[UserDefinedFunction] = ...,
    ) -> None: ...
    def setInputCol(self, value: str) -> UDFTransformer: ...
    def getInputCol(self) -> str: ...
    def setInputCols(self, value: List[str]) -> UDFTransformer: ...
    def getInputCols(self) -> List[str]: ...
    def setOutputCol(self, value: str) -> UDFTransformer: ...
    def getOutputCol(self) -> str: ...
    def setUDF(self, udf: UserDefinedFunction) -> UDFTransformer: ...
    def getUDF(self) -> UserDefinedFunction: ...
    @classmethod
    def read(cls) -> Any: ...
    @staticmethod
    def getJavaPackage() -> str: ...
    @staticmethod
    def _from_java(java_stage: Any) -> UDFTransformer: ...
