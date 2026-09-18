# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import json
import sys

from py4j.java_gateway import JavaObject

if sys.version >= "3":
    basestring = str

from pyspark.ml.util import JavaMLReadable, JavaMLReader, MLReadable
from pyspark.ml.wrapper import JavaParams
from pyspark.ml.common import inherit_doc, _java2py
from pyspark import SparkContext
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
    def _is_service_param(self, java_param):
        if not hasattr(self, "_service_param_java_class"):
            sc = SparkContext._active_spark_context
            self._service_param_java_class = (
                sc._gateway.jvm.com.microsoft.azure.synapse.ml.param.ServiceParam._java_lang_class
            )
        return self._service_param_java_class.isAssignableFrom(java_param.getClass())

    def _service_param_name_for_argument(self, argument):
        candidates = [argument]
        if argument.endswith("Col"):
            candidates.insert(0, argument[:-3])
        service_param_names = getattr(self, "_service_param_names", None)
        if service_param_names is not None:
            return next(
                (name for name in candidates if name in service_param_names), None
            )
        # Older generated wrappers do not include service parameter metadata.
        for candidate in candidates:
            if self._java_obj.hasParam(candidate):
                java_param = self._java_obj.getParam(candidate)
                if self._is_service_param(java_param):
                    return candidate
        return None

    def _validate_service_param_arguments(self, kwargs, skip_none=False):
        service_arguments = {}
        for argument, value in kwargs.items():
            service_param = self._service_param_name_for_argument(argument)
            if service_param is not None:
                if value is None:
                    if skip_none:
                        continue
                    raise TypeError("Service parameter '%s' cannot be None" % argument)
                previous = service_arguments.get(service_param)
                if previous is not None and previous != argument:
                    raise ValueError(
                        "Cannot set both '%s' and '%s' in the same call"
                        % (previous, argument),
                    )
                service_arguments[service_param] = argument

    def _service_param_value_to_java(self, value):
        jvm = SparkContext._active_spark_context._jvm
        if isinstance(value, list):
            return jvm.com.microsoft.azure.synapse.ml.param.ServiceParam.toSeq(value)
        if isinstance(value, dict):

            def convert(item):
                if isinstance(item, dict):
                    result = jvm.java.util.LinkedHashMap()
                    for key, nested in item.items():
                        result.put(key, convert(nested))
                    return result
                if isinstance(item, list):
                    result = jvm.java.util.ArrayList()
                    for nested in item:
                        result.add(convert(nested))
                    return result
                return item

            return jvm.com.microsoft.azure.synapse.ml.param.ServiceParam.toMap(
                convert(value)
            )
        return value

    def _service_param_scalar_to_python(self, name, value):
        sc = SparkContext._active_spark_context
        converted = _java2py(sc, value)
        if not isinstance(converted, JavaObject):
            return converted
        java_param = self._java_obj.getParam(name)
        encoded = java_param.jsonEncode(sc._jvm.scala.util.Left.apply(value))
        return json.loads(encoded)["left"]

    def _set_params_via_setters(self, kwargs, skip_none=False):
        self._validate_service_param_arguments(kwargs, skip_none=skip_none)
        for param, value in kwargs.items():
            if value is not None or not skip_none:
                setter = "set" + param[0].upper() + param[1:]
                getattr(self, setter)(value)
        return self

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
                is_service_param = self._is_service_param(java_param)
                if self._java_obj.isSet(java_param):
                    if is_complex_param:
                        value = self._java_obj.getOrDefault(java_param)
                    elif is_service_param:
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
            is_service_param = False
            if self.isSet(param):
                java_param = self._java_obj.getParam(param.name)
                is_service_param = self._is_service_param(java_param)
                if is_service_param:
                    setter = "set{}".format(param.name[0].upper() + param.name[1:])
                    getattr(self, setter)(self._paramMap[param])
                    self._paramMap.pop(param, None)
                else:
                    pair = self._make_java_param_pair(param, self._paramMap[param])
                    self._java_obj.set(pair)
            if self.hasDefault(param) and not is_service_param:
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
