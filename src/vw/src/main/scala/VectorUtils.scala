// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

object VectorUtils {
  /**
    * Sort of indices and assocated values. Either sums or ignores values for collisions.
    * @param indices indices to be sorted.
    * @param values associated values to each index.
    * @param sumCollisions if true values of index collisions are summed, otherwise ignored.
    * @return
    */
  def sortAndDistinct(indices: Array[Int], values: Array[Double], sumCollisions: Boolean = true):
  (Array[Int], Array[Double]) = {
    if (indices.length == 0)
      (indices, values)
    else {
      // get a sorted list of indices
      val argsort = (0 until indices.length)
        .sortWith(indices(_) < indices(_))
        .toArray

      val indicesSorted = new Array[Int](indices.length)
      val valuesSorted = new Array[Double](indices.length)

      indicesSorted(0) = indices(argsort(0))
      var previousIndex = indicesSorted(0)
      valuesSorted(0) = values(argsort(0))

      // in-place de-duplicate
      var j = 1
      for (i <- 1 until indices.length) {
        val argIndex = argsort(i)
        val index = indices(argIndex)

        if (index != previousIndex) {
          indicesSorted(j) = index
          previousIndex = index
          valuesSorted(j) = values(argIndex)

          j += 1
        }
        else if (sumCollisions)
          valuesSorted(j - 1) += values(argIndex)
      }

      if (j == indices.length)
        (indicesSorted, valuesSorted)
      else {
        // just in case we found duplicates, lets compact the array
        val indicesCompacted = new Array[Int](j)
        val valuesCompacted = new Array[Double](j)

        Array.copy(indicesSorted, 0, indicesCompacted, 0, j)
        Array.copy(valuesSorted, 0, valuesCompacted, 0, j)

        (indicesCompacted, valuesCompacted)
      }
    }
  }
}
