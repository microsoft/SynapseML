// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.accumulo

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import java.util.{Arrays, List}

@SerialVersionUID(1L)
class AccumuloDataSourceReader(val schema: StructType, options: DataSourceOptions) extends DataSourceReader with Serializable {

  val props = options.asMap()
  val tableName = options.tableName.get

  override def readSchema: StructType = schema

  override def planInputPartitions: List[InputPartition[InternalRow]] = {
    // TODO: query accumulo server for partitions
    // https://github.com/apache/accumulo/blob/master/core/src/main/
    // java/org/apache/accumulo/core/client/mapred/AbstractInputFormat.java#L723

    Arrays.asList(new InputPartition[InternalRow]() {
      override def createPartitionReader: InputPartitionReader[InternalRow] = {
        // I assume this runs on the exectuors
        // new Nothing(tableName, props, schema)
        null
      }
    })
  }
}