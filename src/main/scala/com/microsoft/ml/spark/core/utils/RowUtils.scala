// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.core.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow

class RowUtils {

  //TODO Deprecate later
  def merge(rows: Row*): Row = {
    new GenericRow(rows.flatMap(_.toSeq).toArray)
  }
}
