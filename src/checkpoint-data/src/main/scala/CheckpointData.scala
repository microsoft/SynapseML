// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import org.apache.spark.ml.param._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.storage._

trait CheckpointDataParams extends MMLParams {

  // Determines the storage level: MEMORY_ONLY or MEMORY_AND_DISK
  val diskIncluded: BooleanParam = BooleanParam(this, "diskIncluded", "Persist to disk as well as memory", false)
  final def getDiskIncluded: Boolean = $(diskIncluded)
  def setDiskIncluded(value: Boolean): this.type = set(diskIncluded, value)

  // Enables reverse operation to free up memory
  val removeCheckpoint: BooleanParam = BooleanParam(this, "removeCheckpoint", "Unpersist a cached dataset", false)
  final def getRemoveCheckpoint: Boolean = $(removeCheckpoint)
  def setRemoveCheckpoint(value: Boolean): this.type = set(removeCheckpoint, value)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    schema
  }

}

class CheckpointData(override val uid: String) extends Transformer with CheckpointDataParams {

  def this() = this(Identifiable.randomUID("CheckpointData"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    if ($(removeCheckpoint)) {
      CheckpointData.clearCache(dataset, false)
    } else {
      CheckpointData.cache(dataset, $(diskIncluded), false)
    }
  }

  def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  def copy(extra: ParamMap): CheckpointData = defaultCopy(extra)

}

object CheckpointData extends DefaultParamsReadable[CheckpointData]{

  def clearCache(ds: Dataset[_], blocking: Boolean): DataFrame = {
    ds.unpersist(blocking)
    ds.toDF
  }

  def cache(ds: Dataset[_], disk: Boolean, serialized: Boolean): DataFrame = {
    ds.persist(if (disk && serialized) StorageLevel.MEMORY_AND_DISK_SER
               else if (serialized)    StorageLevel.MEMORY_ONLY_SER
               else if (disk)          StorageLevel.MEMORY_AND_DISK
               else                    StorageLevel.MEMORY_ONLY)
    ds.toDF
  }

  def persistToHive(ds: Dataset[_], dbName: String, tableName: String): DataFrame = {
    ds.write.mode("overwrite").saveAsTable(dbName + "." + tableName)
    ds.toDF
  }

}
