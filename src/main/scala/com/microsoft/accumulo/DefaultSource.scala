// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.accumulo

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}
import org.apache.spark.sql.types.StructType

class DefaultSource extends DataSourceV2 with ReadSupport {


  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {
    null
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    throw new UnsupportedOperationException("Must supply schema")
  }

}