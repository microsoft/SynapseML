from pyspark.ml.wrapper import JavaParams, JavaWrapper
from py4j.java_gateway import JavaObject
from pyspark import RDD, SparkContext
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark.sql import DataFrame, SQLContext
from pyspark.ml.common import _to_java_object_rdd, _java2py
import pyspark
from pyspark.ml import PipelineModel
from pyspark.sql.types import DataType


@staticmethod
def _mml_from_java(java_stage):
    """
    Given a Java object, create and return a Python wrapper of it.
    Used for ML persistence.

    Meta-algorithms such as Pipeline should override this method as a classmethod.
    """

    def __get_class(clazz):
        """
        Loads Python class from its name.
        """
        parts = clazz.split(".")
        module = ".".join(parts[:-1])
        m = __import__(module)
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m

    stage_name = java_stage.getClass().getName().replace("org.apache.spark", "pyspark")
    stage_name = stage_name.replace("com.microsoft.azure.synapse.ml", "synapse.ml")
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
        raise NotImplementedError("This Java stage cannot be loaded into Python currently: %r" % stage_name)
    return py_stage


JavaParams._from_java = _mml_from_java


def _mml_py2java(sc, obj):
    """ Convert Python object into Java """
    if isinstance(obj, JavaParams):
        obj._transfer_params_to_java()
        obj = obj._java_obj
    elif isinstance(obj, PipelineModel):
        obj = obj._to_java()
    elif isinstance(obj, RDD):
        obj = _to_java_object_rdd(obj)
    elif isinstance(obj, DataFrame):
        obj = obj._jdf
    elif isinstance(obj, SparkContext):
        obj = obj._jsc
    elif isinstance(obj, list):
        obj = [_mml_py2java(sc, x) for x in obj]
    elif isinstance(obj, JavaObject):
        pass
    elif isinstance(obj, (int, float, bool, bytes, str)):
        pass
    elif isinstance(obj, DataType):
        obj = sc._jvm.org.apache.spark.sql.types.DataType.fromJson(obj.json())
    else:
        data = bytearray(PickleSerializer().dumps(obj))
        obj = sc._jvm.org.apache.spark.ml.python.MLSerDe.loads(data)
    return obj


def _mml_make_java_param_pair(self, param, value):
    """
    Makes a Java param pair.
    """
    sc = SparkContext._active_spark_context
    param = self._resolveParam(param)
    java_param = self._java_obj.getParam(param.name)
    java_value = _mml_py2java(sc, value)
    return java_param.w(java_value)


def _mml_call_java(self, name, *args):
    m = getattr(self._java_obj, name)
    sc = SparkContext._active_spark_context
    java_args = [_mml_py2java(sc, arg) for arg in args]
    return _java2py(sc, m(*java_args))


JavaParams._make_java_param_pair = _mml_make_java_param_pair
JavaWrapper._call_java = _mml_call_java
