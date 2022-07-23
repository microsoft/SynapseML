// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.utils.ClusterUtil
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row, functions => F}

import java.io.Serializable

trait VowpalWabbitSynchronizationSchedule extends Serializable {
  def shouldTriggerAllReduce(row: Row): Boolean
}

class VowpalWabbitSynchronizationScheduleDisabled extends VowpalWabbitSynchronizationSchedule {
  override def shouldTriggerAllReduce(row: Row): Boolean = false
}

object VowpalWabbitSynchronizationSchedule
{
  lazy val Disabled = new VowpalWabbitSynchronizationScheduleDisabled
}

class VowpalWabbitSynchronizationScheduleSplits (df: DataFrame,
                                                 numSplits: Integer)
  extends VowpalWabbitSynchronizationSchedule {

  assert(numSplits > 0, "Number of splits must be greater than zero")

  val rowsPerPartitions = ClusterUtil.getNumRowsPerPartition(df, F.lit(0))

  val stepSizePerPartition = rowsPerPartitions.map { c => Math.ceil(c / numSplits.toDouble).toLong }

  private def getStepSize() = stepSizePerPartition(TaskContext.getPartitionId())

  class ValueHolder(var value: Long)

  @transient
  lazy val nextSyncValue = new ValueHolder(getStepSize)

  @transient
  var rowCount = 0

  override def shouldTriggerAllReduce(row: Row): Boolean = {
    rowCount += 1

    if (rowCount < nextSyncValue.value)
      false
    else {
      nextSyncValue.value = rowCount + getStepSize()

      true
    }
  }
}
