// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.lightgbm.swig

import com.microsoft.ml.lightgbm.{SWIGTYPE_p_float, lightgbmlib}

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
