# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from abc import ABCMeta
import sys
from typing import Mapping, List
from py4j import java_gateway

if sys.version >= "3":
    basestring = str

from synapse.ml.onnx._ONNXModel import _ONNXModel
from pyspark.ml.common import inherit_doc
from py4j.java_gateway import JavaObject


class NodeInfo(object):
    def __init__(self, name: str, value_info: JavaObject):
        self.name = name
        self.value_info = ValueInfo.from_java(value_info)

    def __str__(self) -> str:
        return "NodeInfo(name=" + self.name + ",info=" + str(self.value_info) + ")"

    def __repr__(self) -> str:
        return self.__str__()


@inherit_doc
class ONNXModel(_ONNXModel):
    """

    Args:
        SparkSession (SparkSession): The SparkSession that will be used to find the model
        location (str): The location of the model, either on local or HDFS
    """

    def setModelLocation(self, location):
        self._java_obj = self._java_obj.setModelLocation(location)
        return self

    def setMiniBatchSize(self, n):
        self._java_obj = self._java_obj.setMiniBatchSize(n)
        return self

    def __parse_node_info(self, node_info: JavaObject) -> "NodeInfo":
        name = node_info.getName()
        value_info = node_info.getInfo()
        return NodeInfo(name, value_info)

    def getModelInputs(self):
        self._transfer_params_to_java()
        mi = self._java_obj.modelInputJava()
        return {name: self.__parse_node_info(info) for name, info in mi.items()}

    def getModelOutputs(self) -> Mapping[str, NodeInfo]:
        self._transfer_params_to_java()
        mo = self._java_obj.modelOutputJava()
        return {name: self.__parse_node_info(info) for name, info in mo.items()}


class ValueInfo(metaclass=ABCMeta):
    @classmethod
    def from_java(cls, java_value_info: JavaObject) -> "ValueInfo":
        className = java_value_info.getClass().getName()
        if className == "ai.onnxruntime.TensorInfo":
            return TensorInfo.from_java(java_value_info)
        elif className == "ai.onnxruntime.MapInfo":
            return MapInfo.from_java(java_value_info)
        else:
            return SequenceInfo.from_java(java_value_info)


class TensorInfo(ValueInfo):
    def __init__(self, shape: List[int], type: str):
        self.shape = shape
        self.type = type

    def __repr__(self):
        return str(self)

    def __str__(self):
        return "TensorInfo(shape={}, type={})".format(
            "[" + ",".join(map(str, self.shape)) + "]",
            self.type,
        )

    @classmethod
    def from_java(cls, java_tensor_info: JavaObject) -> "TensorInfo":
        shape = list(java_tensor_info.getShape())
        type = java_gateway.get_field(java_tensor_info, "type").toString()
        return cls(shape, type)


class MapInfo(ValueInfo):
    def __init__(self, key_type: str, value_type: str, size: int = -1):
        self.key_type = key_type
        self.value_type = value_type
        self.size = size

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        initial = (
            "MapInfo(size=UNKNOWN"
            if self.size == -1
            else "MapInfo(size=" + str(self.size)
        )
        return (
            initial
            + ",keyType="
            + self.key_type
            + ",valueType="
            + self.value_type
            + ")"
        )

    @classmethod
    def from_java(cls, java_map_info: JavaObject) -> "MapInfo":
        if java_map_info == None:
            return None
        else:
            key_type = java_gateway.get_field(java_map_info, "keyType").toString()
            value_type = java_gateway.get_field(java_map_info, "valueType").toString()
            size = java_gateway.get_field(java_map_info, "size")
            return cls(key_type, value_type, size)


class SequenceInfo(ValueInfo):
    def __init__(
        self,
        length: int,
        sequence_of_maps: bool,
        map_info: MapInfo,
        sequence_type: str,
    ):
        self.length = length
        self.sequence_of_maps = sequence_of_maps
        self.map_info = map_info
        self.sequence_type = sequence_type

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        initial = "SequenceInfo(length=" + (
            "UNKNOWN" if self.length == -1 else str(self.length)
        )
        if self.sequence_of_maps:
            initial += ",type=" + str(self.map_info) + ")"
        else:
            initial += ",type=" + str(self.sequence_type) + ")"
        return initial

    @classmethod
    def from_java(cls, java_sequence_info: JavaObject) -> "SequenceInfo":
        length = java_gateway.get_field(java_sequence_info, "length")
        sequence_of_maps = java_gateway.get_field(java_sequence_info, "sequenceOfMaps")
        map_info = MapInfo.from_java(
            java_gateway.get_field(java_sequence_info, "mapInfo"),
        )
        sequence_type = java_gateway.get_field(
            java_sequence_info,
            "sequenceType",
        ).toString()
        return cls(length, sequence_of_maps, map_info, sequence_type)
