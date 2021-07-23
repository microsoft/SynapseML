// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.injections

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.BlockManager

object BlockManagerUtils {
  /** Returns the block manager from the dataframe's spark context.
    *
    * @param spark The spark session to get the block manager from.
    * @return The block manager.
    */
  def getBlockManager(spark: SparkSession): BlockManager = {
    spark.sparkContext.env.blockManager
  }
}
