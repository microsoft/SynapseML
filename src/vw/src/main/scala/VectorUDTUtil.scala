// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml

import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.sql.types.DataType

object VectorUDTUtil {
  def getDataType: DataType = new VectorUDT
}
