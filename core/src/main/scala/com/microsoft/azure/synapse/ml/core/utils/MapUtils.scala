// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

object MapUtils {
  def newMapArray(args: Seq[Seq[Any]]): Array[Map[Any, Any]] = {
    val result = args.map(a => newMap(a))
    println(result)
    result.toArray
  }

  def newMap(args: Seq[Any]): Map[Any, Any] = {
    if (args.length % 2 != 0) {
      throw new IllegalArgumentException("Argument list must have an even number of elements.")
    }

    val map = scala.collection.mutable.Map[Any, Any]()
    addArgs(map, args)
  }

  def addArgs(map: scala.collection.mutable.Map[Any, Any], args: Seq[Any]): Map[Any, Any] = {
    if (args != null && args.length > 1) {
      map.put(args(0), args(1))
      addArgs(map, args.tail.tail)
    }

    map.toMap
  }
}
