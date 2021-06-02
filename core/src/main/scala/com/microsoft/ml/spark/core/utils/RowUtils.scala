// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.core.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow

// This class currently has no usage. Should we just remove it?
@deprecated("This is a copy of Row.merge function from Spark, which was marked deprecated.", "1.0.0-rc3")
class RowUtils {

  //TODO Deprecate later
  def merge(rows: Row*): Row = {
    Row.merge()
    new GenericRow(rows.flatMap(_.toSeq).toArray)
  }
}
