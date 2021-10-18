// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import org.apache.spark.sql.Row

private[explainers] object RowUtils {
  implicit class RowCanGetAsDouble(row: Row) {
    def getAsDouble(col: String): Double = {
      val idx = row.fieldIndex(col)
      getAsDouble(idx)
    }

    def getAsDouble(fieldIndex: Int): Double = {
      row.get(fieldIndex) match {
        case v: Byte => v.toDouble
        case v: Short => v.toDouble
        case v: Int => v.toDouble
        case v: Long => v.toDouble
        case v: Float => v.toDouble
        case v: Double => v
        case v => throw new Exception(s"Cannot convert $v to Double.")
      }
    }
  }
}
