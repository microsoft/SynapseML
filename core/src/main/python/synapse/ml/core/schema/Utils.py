# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= "3":
    basestring = str

from pyspark.ml.util import JavaMLReadable, JavaMLReader, MLReadable
from pyspark.ml.wrapper import JavaParams
from pyspark.ml.common import inherit_doc, _java2py
from pyspark import SparkContext
from py4j.java_gateway import JavaObject
from synapse.ml.core.serialize._safe_import import secure_import_class


def from_java(java_stage, stage_name):
    """
    Given a Java object, create and return a Python wrapper of it.
    Used for ML persistence.
    Meta-algorithms such as Pipeline should override this method as a classmethod.

    Args:
        java_stage (str):
        stage_name (str):

    Returns:
        object: The python wrapper
    """

    # Generate a default new instance from the stage_name class.
    py_type = secure_import_class(stage_name)
    if issubclass(py_type, JavaParams):
        # Load information from java_stage to the instance.
        py_stage = py_type()
        py_stage._java_obj = java_stage
        py_stage._resetUid(java_stage.uid())
        py_stage._transfer_params_from_java()
    elif hasattr(py_type, "_from_java"):
        py_stage = py_type._from_java(java_stage)
    else:
        raise NotImplementedError(
            "This Java stage cannot be loaded into Python currently: %r" % stage_name,
        )
    return py_stage


@inherit_doc
class JavaMMLReadable(MLReadable):
    """
    (Private) Mixin for instances that provide JavaMLReader.
    """

    @classmethod
    def read(cls):
        """Returns an MLReader instance for this class."""
        return JavaMMLReader(cls)


@inherit_doc
class ComplexParamsMixin(MLReadable):
    def hasDefault(self, param):
        param = self._resolveParam(param)
        if param.doc.startswith("ServiceParam:"):
            if param not in self._paramMap:
                value = self._java_service_value(param)
                if value is not None and value.isRight():
                    return False
            default = self._java_obj.getDefault(self._java_obj.getParam(param.name))
            return super().hasDefault(param) or (
                default.isDefined() and default.get().isLeft()
            )
        return super().hasDefault(param)

    def _service_value_to_python(self, value):
        # PySpark interprets raw bytes as a pickle rather than media content.
        if isinstance(value, (bytes, bytearray)):
            return value
        sc = SparkContext._active_spark_context
        if isinstance(value, JavaObject):
            map_class = sc._jvm.scala.collection.Map._java_lang_class
            if map_class.isAssignableFrom(value.getClass()):
                result = {}
                entries = value.iterator()
                while entries.hasNext():
                    entry = entries.next()
                    result[
                        self._service_value_to_python(entry._1())
                    ] = self._service_value_to_python(entry._2())
                return result
        return _java2py(sc, value)

    def _java_service_value(self, param):
        java_param = self._java_obj.getParam(param.name)
        if self._java_obj.isDefined(java_param):
            return self._java_obj.getOrDefault(java_param)
        return None

    def getOrDefault(self, param):
        param = self._resolveParam(param)
        if param.doc.startswith("ServiceParam:") and param not in self._paramMap:
            value = self._java_service_value(param)
            if value is not None:
                if value.isRight():
                    raise KeyError(f"{param.name} is column-bound, not a scalar value")
                return self._service_value_to_python(value.value())
        return super().getOrDefault(param)

    def extractParamMap(self, extra=None):
        """Include JVM scalars; column bindings remain on the JVM during copy/save."""
        result = super().extractParamMap(extra)
        for param in self.params:
            if (
                param.doc.startswith("ServiceParam:")
                and param not in self._paramMap
                and param not in (extra or {})
            ):
                value = self._java_service_value(param)
                if value is not None:
                    if value.isLeft():
                        result[param] = self._service_value_to_python(value.value())
                    else:
                        result.pop(param, None)
        return result

    def explainParam(self, param):
        param = self._resolveParam(param)
        if param.doc.startswith("ServiceParam:"):
            if param.name.lower() in (
                "subscriptionkey",
                "aadtoken",
                "customauthheader",
                "customheaders",
                "telemheaders",
            ):
                value = self._java_service_value(param)
                if (
                    param in self._paramMap
                    or (value is not None and value.isLeft())
                    or (value is None and param in self._defaultParamMap)
                ):
                    state = "scalar: [redacted]"
                elif value is not None:
                    state = f"column: {value.value()}"
                else:
                    state = "undefined"
                return f"{param.name}: {param.doc} ({state})"
            if param not in self._paramMap:
                return self._java_obj.explainParam(self._java_obj.getParam(param.name))
        return super().explainParam(param)

    def clear(self, param):
        param = self._resolveParam(param)
        if param.doc.startswith("ServiceParam:"):
            self._java_obj.clear(self._java_obj.getParam(param.name))
            self._paramMap.pop(param, None)
        else:
            super().clear(param)

    def _transfer_params_from_java(self):
        """
        Transforms the embedded com.microsoft.azure.synapse.ml.core.serialize.params from the companion Java object.
        """
        sc = SparkContext._active_spark_context
        for param in self.params:
            if self._java_obj.hasParam(param.name):
                java_param = self._java_obj.getParam(param.name)
                # SPARK-14931: Only check set com.microsoft.azure.synapse.ml.core.serialize.params back to avoid default com.microsoft.azure.synapse.ml.core.serialize.params mismatch.
                complex_param_class = (
                    sc._gateway.jvm.com.microsoft.azure.synapse.ml.core.serialize.ComplexParam._java_lang_class
                )
                is_complex_param = complex_param_class.isAssignableFrom(
                    java_param.getClass(),
                )
                service_param_class = (
                    sc._gateway.jvm.com.microsoft.azure.synapse.ml.param.ServiceParam._java_lang_class
                )
                is_service_param = service_param_class.isAssignableFrom(
                    java_param.getClass(),
                )
                if self._java_obj.isSet(java_param):
                    if is_complex_param:
                        value = self._java_obj.getOrDefault(java_param)
                    elif is_service_param:
                        self._paramMap.pop(param, None)
                        continue
                    else:
                        value = _java2py(sc, self._java_obj.getOrDefault(java_param))
                    self._set(**{param.name: value})

    def _transfer_params_to_java(self):
        """
        Transforms the embedded params to the companion Java object.
        """
        sc = SparkContext._active_spark_context
        pair_defaults = []
        for param in self.params:
            if param in self._paramMap:
                service_param_class = (
                    sc._gateway.jvm.com.microsoft.azure.synapse.ml.param.ServiceParam._java_lang_class
                )
                is_service_param = service_param_class.isAssignableFrom(
                    self._java_obj.getParam(param.name).getClass(),
                )
                if is_service_param:
                    getattr(
                        self,
                        "set{}".format(param.name[0].upper() + param.name[1:]),
                    )(self._paramMap[param])
                else:
                    pair = self._make_java_param_pair(param, self._paramMap[param])
                    self._java_obj.set(pair)
            if param in self._defaultParamMap:
                pair = self._make_java_param_pair(param, self._defaultParamMap[param])
                pair_defaults.append(pair)
        if len(pair_defaults) > 0:
            sc = SparkContext._active_spark_context
            pair_defaults_seq = sc._jvm.PythonUtils.toSeq(pair_defaults)
            self._java_obj.setDefault(pair_defaults_seq)


@inherit_doc
class JavaMMLReader(JavaMLReader):
    """
    (Private) Specialization of :py:class:`MLReader` for :py:class:`JavaParams` types
    """

    def __init__(self, clazz):
        super(JavaMMLReader, self).__init__(clazz)

    @classmethod
    def _java_loader_class(cls, clazz):
        """
        Returns the full class name of the Java ML instance.
        """
        return clazz.getJavaPackage()
