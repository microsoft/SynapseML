// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.swig

import com.microsoft.ml.lightgbm._

object SwigUtils extends Serializable {
  /** Converts a native double array to a Java array using SWIG.
    * @param nativeArray The native Array to convert.
    * @return The Java array.
    */
  def nativeDoubleArrayToArray(nativeArray: SWIGTYPE_p_double, count: Int): Array[Double] = {
    val array: Array[Double] = Array.fill[Double](count)(0)
    array.indices.foreach(i => array(i) = lightgbmlib.doubleArray_getitem(nativeArray, i))
    array
  }

  /** Converts a Java float array to a native C++ array using SWIG.
    * @param array The Java float Array to convert.
    * @return The SWIG wrapper around the native array.
    */
  def floatArrayToNative(array: Array[Float]): SWIGTYPE_p_float = {
    val colArray = lightgbmlib.new_floatArray(array.length)
    array.zipWithIndex.foreach { case (value, index) => lightgbmlib.floatArray_setitem(colArray, index.toLong, value)}
    colArray
  }

  /** Converts a Java Byte array to a native C++ array using SWIG.
    *
    * @param array The Java Byte Array to convert.
    * @return The SWIG wrapper around the native array.
    */
  def byteArrayToNative(array: Array[Byte]): SWIGTYPE_p_unsigned_char = {
    val colArray = lightgbmlib.new_byteArray(array.length)
    array.zipWithIndex.foreach { case (value, index) =>
      lightgbmlib.byteArray_setitem(colArray, index.toLong, value.toShort)}
    colArray
  }
}

abstract class ChunkedArray[T]() {
  def getChunksCount: Long

  def getLastChunkAddCount: Long

  def getChunkSize: Long

  def getAddCount: Long

  def getItem(chunk: Long, inChunkIdx: Long, default: T): T

  def add(value: T): Unit
}

object FloatChunkedArray {
  private def apply(size: Long): floatChunkedArray = {
    if (size <= 0) {
      throw new IllegalArgumentException("Chunked array size must be greater than zero")
    }
    new floatChunkedArray(size)
  }
}

class FloatChunkedArray(array: floatChunkedArray) extends ChunkedArray[Float] {
  def this(size: Long) = this(FloatChunkedArray(size))

  def getChunksCount: Long = array.get_chunks_count()

  def getLastChunkAddCount: Long = array.get_last_chunk_add_count()

  def getChunkSize: Long = array.get_chunk_size()

  def getAddCount: Long = array.get_add_count()

  def getItem(chunk: Long, inChunkIdx: Long, default: Float): Float =
    array.getitem(chunk, inChunkIdx, default)

  def add(value: Float): Unit = array.add(value)

  def delete(): Unit = array.delete()

  def coalesceTo(floatSwigArray: FloatSwigArray): Unit = array.coalesce_to(floatSwigArray.array)
}

object DoubleChunkedArray {
  private def apply(size: Long): doubleChunkedArray = {
    if (size <= 0) {
      throw new IllegalArgumentException("Chunked array size must be greater than zero")
    }
    new doubleChunkedArray(size)
  }
}

class DoubleChunkedArray(array: doubleChunkedArray) extends ChunkedArray[Double] {
  def this(size: Long) = this(DoubleChunkedArray(size))

  def getChunksCount: Long = array.get_chunks_count()

  def getLastChunkAddCount: Long = array.get_last_chunk_add_count()

  def getChunkSize: Long = array.get_chunk_size()

  def getAddCount: Long = array.get_add_count()

  def getItem(chunk: Long, inChunkIdx: Long, default: Double): Double =
    array.getitem(chunk, inChunkIdx, default)

  def add(value: Double): Unit = array.add(value)

  def delete(): Unit = array.delete()

  def coalesceTo(doubleSwigArray: DoubleSwigArray): Unit = array.coalesce_to(doubleSwigArray.array)
}

object IntChunkedArray {
  private def apply(size: Long): int32ChunkedArray = {
    if (size <= 0) {
      throw new IllegalArgumentException("Chunked array size must be greater than zero")
    }
    new int32ChunkedArray(size)
  }
}

class IntChunkedArray(array: int32ChunkedArray) extends ChunkedArray[Int] {
  def this(size: Long) = this(IntChunkedArray(size))

  def getChunksCount: Long = array.get_chunks_count()

  def getLastChunkAddCount: Long = array.get_last_chunk_add_count()

  def getChunkSize: Long = array.get_chunk_size()

  def getAddCount: Long = array.get_add_count()

  def getItem(chunk: Long, inChunkIdx: Long, default: Int): Int =
    array.getitem(chunk, inChunkIdx, default)

  def add(value: Int): Unit = array.add(value)

  def delete(): Unit = array.delete()

  def coalesceTo(intSwigArray: IntSwigArray): Unit = array.coalesce_to(intSwigArray.array)
}

abstract class BaseSwigArray[T]() {
  def getItem(index: Long): T
  def setItem(index: Long, item: T): Unit
}

// Wraps float*
class FloatSwigArray(val array: SWIGTYPE_p_float) extends BaseSwigArray[Float] {
  def this(size: Long) = this(lightgbmlib.new_floatArray(size))

  def getItem(index: Long): Float = lightgbmlib.floatArray_getitem(array, index)

  def setItem(index: Long, item: Float): Unit = lightgbmlib.floatArray_setitem(array, index, item)

  def delete(): Unit = lightgbmlib.delete_floatArray(array)
}

// Wraps double*
class DoubleSwigArray(val array: SWIGTYPE_p_double) extends BaseSwigArray[Double] {
  def this(size: Long) = this(lightgbmlib.new_doubleArray(size))

  def getItem(index: Long): Double = lightgbmlib.doubleArray_getitem(array, index)

  def setItem(index: Long, item: Double): Unit = lightgbmlib.doubleArray_setitem(array, index, item)

  def delete(): Unit = lightgbmlib.delete_doubleArray(array)
}

// Wraps int*
class IntSwigArray(val array: SWIGTYPE_p_int) extends BaseSwigArray[Int] {
  def this(size: Long) = this(lightgbmlib.new_intArray(size))

  def getItem(index: Long): Int = lightgbmlib.intArray_getitem(array, index)

  def setItem(index: Long, item: Int): Unit = lightgbmlib.intArray_setitem(array, index, item)

  def delete(): Unit = lightgbmlib.delete_intArray(array)
}

// Wraps long*
class LongSwigArray(val array: SWIGTYPE_p_long_long) extends BaseSwigArray[Long] {
  def this(size: Long) = this(lightgbmlib.new_longArray(size))

  def getItem(index: Long): Long = lightgbmlib.longArray_getitem(array, index)

  def setItem(index: Long, item: Long): Unit = lightgbmlib.longArray_setitem(array, index, item)

  def delete(): Unit = lightgbmlib.delete_longArray(array)
}

// Wraps void**
class VoidPointerSwigArray(val array: SWIGTYPE_p_p_void, val size: Int) extends BaseSwigArray[SWIGTYPE_p_void] {
  def this(size: Int) = this(lightgbmlib.new_voidPtrArray(size), size)

  def getItem(index: Long): SWIGTYPE_p_void = lightgbmlib.voidPtrArray_getitem(array, index)

  def setItem(index: Long, item: SWIGTYPE_p_void): Unit = {
    lightgbmlib.voidPtrArray_setitem(array, index, item) // set native pointer value
  }

  def delete(): Unit =  {
    lightgbmlib.delete_voidPtrArray(array)
  }
}

// Wraps double**, which is implemented in the wrapper as an array of DoubleSwigArray
class DoublePointerSwigArray(val array: SWIGTYPE_p_p_double, val size: Int) extends BaseSwigArray[DoubleSwigArray] {
  val columnVectors: Array[Option[DoubleSwigArray]] = Array.fill(size)(None)

  def this(size: Int) = this(lightgbmlib.new_doublePtrArray(size), size)

  def getItem(index: Long): DoubleSwigArray = columnVectors.apply(index.toInt).get

  def setItem(index: Long, item: DoubleSwigArray): Unit = {
    columnVectors(index.toInt).foreach(array => array.delete()) // free existing vector if exists
    columnVectors(index.toInt) = Option(item) // store vector wrapper locally
    lightgbmlib.doublePtrArray_setitem(array, index, item.array) // set native pointer value
  }

  def pushElement(col: Int, row: Int, value: Double): Unit = {
    columnVectors(col).foreach(v => v.setItem(row, value))
  }

  def delete(): Unit =  {
    // delete the individual vectors first
    columnVectors.foreach(v => v.map(array => array.delete()))
    lightgbmlib.delete_doublePtrArray(array)
  }
}

// Wraps int**, which is implemented in the wrapper as an array of IntSwigArray
class IntPointerSwigArray(val array: SWIGTYPE_p_p_int, val size: Int) extends BaseSwigArray[IntSwigArray] {
  val columnVectors: Array[Option[IntSwigArray]] = Array.fill(size)(None)

  def this(size: Int) = this(lightgbmlib.new_intPtrArray(size), size)

  def getItem(index: Long): IntSwigArray = columnVectors.apply(index.toInt).get

  def setItem(index: Long, item: IntSwigArray): Unit = {
    columnVectors(index.toInt).foreach(array => array.delete()) // free existing vector if exists
    columnVectors(index.toInt) = Option(item) // store vector wrapper locally
    lightgbmlib.intPtrArray_setitem(array, index, item.array) // set native pointer value
  }

  def pushElement(col: Int, row: Int, value: Int): Unit = {
    columnVectors(col).foreach(v => v.setItem(row, value))
  }

  def delete(): Unit =  {
    // delete the individual vectors first
    columnVectors.foreach(v => v.map(array => array.delete()))
    lightgbmlib.delete_intPtrArray(array)
  }
}
