// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.onnx

import ai.onnxruntime.OnnxValue

private[onnx] object ONNXValueConverter {

  /**
    * Eagerly copies a sequence to JVM values and closes every child handle returned by ONNX Runtime.
    * The caller remains responsible for closing the outer value through its owning OrtSession.Result.
    */
  def mapSequenceToArray(value: OnnxValue): Seq[Any] = {
    value.getValue match {
      case values: java.util.List[_] => unwrapList(values)
      case other =>
        val valueType = Option(other).map(_.getClass.getName).getOrElse("null")
        throw new IllegalArgumentException(s"Expected an ONNX sequence value, but found $valueType")
    }
  }

  private def unwrapValue(value: Any): Any = value match {
    case values: java.util.List[_] => unwrapList(values)
    case map: java.util.Map[_, _] => unwrapMap(map)
    case other => other
  }

  private def unwrapOnnxValue(value: OnnxValue): Any = unwrapValue(value.getValue)

  private def unwrapList(values: java.util.List[_]): Vector[Any] = {
    val elements = values.toArray
    val closeables = elements.collect { case value: OnnxValue => value }

    try {
      elements.iterator.map {
        case value: OnnxValue => unwrapOnnxValue(value)
        case other => unwrapValue(other)
      }.toVector
    } finally {
      closeables.foreach(_.close())
    }
  }

  private def unwrapMap(map: java.util.Map[_, _]): Map[Any, Any] = {
    val entries = map.entrySet().toArray.iterator.map {
      case entry: java.util.Map.Entry[_, _] => entry.getKey -> entry.getValue
    }.toVector
    val closeables = entries.iterator.flatMap {
      case (key, value) => Iterator(key, value).collect { case onnxValue: OnnxValue => onnxValue }
    }.toVector

    try {
      entries.iterator.map {
        case (key: OnnxValue, value: OnnxValue) =>
          unwrapOnnxValue(key) -> unwrapOnnxValue(value)
        case (key: OnnxValue, value) =>
          unwrapOnnxValue(key) -> unwrapValue(value)
        case (key, value: OnnxValue) =>
          unwrapValue(key) -> unwrapOnnxValue(value)
        case (key, value) =>
          unwrapValue(key) -> unwrapValue(value)
      }.toMap
    } finally {
      closeables.foreach(_.close())
    }
  }
}
