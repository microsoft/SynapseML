# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= "3":
    basestring = str

from pyspark.ml.util import JavaMLReadable, JavaMLReader, MLReadable
from pyspark.ml.wrapper import JavaParams
from pyspark.ml.common import inherit_doc, _java2py
from pyspark import SparkContext


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

    def __get_class(clazz):
        """
        Loads a python object from its class

        Args:
            clazz (str): The name of the class

        Returns:
            object: The python object
        """
        parts = clazz.split(".")
        module = ".".join(parts[:-1])
        m = __import__(module)
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m

    # Generate a default new instance from the stage_name class.
    py_type = __get_class(stage_name)
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
                        jvObj = self._java_obj.getOrDefault(java_param)
                        if jvObj.isLeft():
                            value = _java2py(sc, jvObj.value())
                        else:
                            value = None
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
            if self.isSet(param):
                service_param_class = (
                    sc._gateway.jvm.com.microsoft.azure.synapse.ml.param.ServiceParam._java_lang_class
                )
                is_service_param = service_param_class.isAssignableFrom(
                    self._java_obj.getParam(param.name).getClass(),
                )
                if is_service_param:
                    getattr(
                        self._java_obj,
                        "set{}".format(param.name[0].upper() + param.name[1:]),
                    )(self._paramMap[param])
                else:
                    pair = self._make_java_param_pair(param, self._paramMap[param])
                    self._java_obj.set(pair)
            if self.hasDefault(param):
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
