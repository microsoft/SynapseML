// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.swig

import com.microsoft.ml.lightgbm.{SWIGTYPE_p_double, SWIGTYPE_p_float, SWIGTYPE_p_int, doubleChunkedArray,
  floatChunkedArray, int32ChunkedArray, lightgbmlib}

object SwigUtils extends Serializable {
  /** Converts a Java float array to a native C++ array using SWIG.
    * @param array The Java float Array to convert.
    * @return The SWIG wrapper around the native array.
    */
  def floatArrayToNative(array: Array[Float]): SWIGTYPE_p_float = {
    val colArray = lightgbmlib.new_floatArray(array.length)
    array.zipWithIndex.foreach(ri =>
      lightgbmlib.floatArray_setitem(colArray, ri._2.toLong, ri._1.toFloat))
    colArray
  }
}

abstract class ChunkedArray[T]() {
  def getChunksCount: Long

  def getLastChunkAddCount: Long

  def getAddCount: Long

  def getItem(chunk: Long, inChunkIdx: Long, default: T): T

  def add(value: T): Unit
}

class FloatChunkedArray(floatChunkedArray: floatChunkedArray) extends ChunkedArray[Float] {

  def this(size: Long) = this(new floatChunkedArray(size))

  def getChunksCount: Long = floatChunkedArray.get_chunks_count()

  def getLastChunkAddCount: Long = floatChunkedArray.get_last_chunk_add_count()

  def getAddCount: Long = floatChunkedArray.get_add_count()

  def getItem(chunk: Long, inChunkIdx: Long, default: Float): Float =
    floatChunkedArray.getitem(chunk, inChunkIdx, default)

  def add(value: Float): Unit = floatChunkedArray.add(value)

  def delete(): Unit = floatChunkedArray.delete()

  def coalesceTo(floatSwigArray: FloatSwigArray): Unit = floatChunkedArray.coalesce_to(floatSwigArray.array)
}

class DoubleChunkedArray(doubleChunkedArray: doubleChunkedArray) extends ChunkedArray[Double] {
  def this(size: Long) = this(new doubleChunkedArray(size))

  def getChunksCount: Long = doubleChunkedArray.get_chunks_count()

  def getLastChunkAddCount: Long = doubleChunkedArray.get_last_chunk_add_count()

  def getAddCount: Long = doubleChunkedArray.get_add_count()

  def getItem(chunk: Long, inChunkIdx: Long, default: Double): Double =
    doubleChunkedArray.getitem(chunk, inChunkIdx, default)

  def add(value: Double): Unit = doubleChunkedArray.add(value)

  def delete(): Unit = doubleChunkedArray.delete()

  def coalesceTo(doubleSwigArray: DoubleSwigArray): Unit = doubleChunkedArray.coalesce_to(doubleSwigArray.array)
}

class IntChunkedArray(intChunkedArray: int32ChunkedArray) extends ChunkedArray[Int] {
  def this(size: Long) = this(new int32ChunkedArray(size))

  def getChunksCount: Long = intChunkedArray.get_chunks_count()

  def getLastChunkAddCount: Long = intChunkedArray.get_last_chunk_add_count()

  def getAddCount: Long = intChunkedArray.get_add_count()

  def getItem(chunk: Long, inChunkIdx: Long, default: Int): Int =
    intChunkedArray.getitem(chunk, inChunkIdx, default)

  def add(value: Int): Unit = intChunkedArray.add(value)

  def delete(): Unit = intChunkedArray.delete()

  def coalesceTo(intSwigArray: IntSwigArray): Unit = intChunkedArray.coalesce_to(intSwigArray.array)
}

abstract class BaseSwigArray[T]() {
  def setItem(index: Long, item: T): Unit
}

class FloatSwigArray(val array: SWIGTYPE_p_float) extends BaseSwigArray[Float] {
  def this(size: Long) = this(lightgbmlib.new_floatArray(size))

  def setItem(index: Long, item: Float): Unit = lightgbmlib.floatArray_setitem(array, index, item)

  def delete(): Unit = lightgbmlib.delete_floatArray(array)
}

class DoubleSwigArray(val array: SWIGTYPE_p_double) extends BaseSwigArray[Double] {
  def this(size: Long) = this(lightgbmlib.new_doubleArray(size))

  def setItem(index: Long, item: Double): Unit = lightgbmlib.doubleArray_setitem(array, index, item)

  def delete(): Unit = lightgbmlib.delete_doubleArray(array)
}

class IntSwigArray(val array: SWIGTYPE_p_int) extends BaseSwigArray[Int] {
  def this(size: Long) = this(lightgbmlib.new_intArray(size))

  def setItem(index: Long, item: Int): Unit = lightgbmlib.intArray_setitem(array, index, item)

  def delete(): Unit = lightgbmlib.delete_intArray(array)
}
