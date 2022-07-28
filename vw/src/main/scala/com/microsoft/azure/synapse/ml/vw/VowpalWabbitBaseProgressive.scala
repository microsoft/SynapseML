// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import org.apache.spark.TaskContext
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.vowpalwabbit.spark.VowpalWabbitNative

import java.util.UUID

/**
  * VowpalWabbit is focused on online learning. In these settings it is common
  * to operate in progressive validation using a 1-step ahead approach.
  * By default when VW learns from data, it first computes the prediction without learning
  * and then updates the model from the new data.
  * This is especially useful in bandit scenarios as one wants to avoid cheating -
  * as in "knowing the future".
  */
trait VowpalWabbitBaseProgressive
  extends Transformer
    with VowpalWabbitBase {
  class TrainingPartitionIterator(inputRows: Iterator[Row],
                                  localInitialModel: Option[Array[Byte]],
                                  syncSchedule: VowpalWabbitSyncSchedule,
                                  vwArgs: String,
                                  numTasks: Int)
    extends Iterator[Row] {

      var closed = false

    // TODO: make sure we don't do double close
    override def finalize(): Unit = close

    private def close(): Unit = {
      if (!closed) {
        vw.close()
        closed = true
      }
    }

    // note this is executed on each worker
    private lazy val vw = {
      val contextArgs = if (numTasks == 1) "" else s"--node ${TaskContext.get.partitionId}"

      val args = buildCommandLineArguments(vwArgs, contextArgs)

      if (localInitialModel.isEmpty) new VowpalWabbitNative(args)
      else new VowpalWabbitNative(args, localInitialModel.get)
    }

    override def hasNext: Boolean = {
      val ret = inputRows.hasNext

      if (!ret) {
        vw.endPass()

        // cleanup
        close()
      }

      ret
    }

    override def next(): Row = {
      try {
        val inputRow = inputRows.next()

        // used to trigger inter-pass all reduce synchronizations
        if (syncSchedule.shouldTriggerAllReduce(inputRow))
          vw.endPass()

        // withColumn
        Row.fromSeq(inputRow.toSeq ++ trainFromRow(vw, inputRow))
      }
      catch {
        case e: Exception =>
          close

          throw new Exception(s"VW failed", e)
      }
    }
  }

  // implementors are task with the transfer of the data to VW
  def trainFromRow(vw: VowpalWabbitNative, row: Row): Seq[Any]

  def getAdditionalOutputSchema: StructType

  override def transform(dataset: Dataset[_]): DataFrame = {
    val df = prepareDataSet(dataset)
    val schema = transformSchema(df.schema)

    val numTasks = df.rdd.getNumPartitions
    val synchronizationSchedule = interPassSyncSchedule(df)

    // schedule multiple mapPartitions in
    val localInitialModel = if (isDefined(initialModel)) Some(getInitialModel) else None

    // TODO: this graph cannot be re-used
    val jobUniqueId = Math.abs(UUID.randomUUID.getLeastSignificantBits.toInt).toString

    // build the command line args and potentially add AllReduce related parameters
    val vwArgs = getCommandLineArgs

    if (numTasks > 1)
      VowpalWabbitClusterUtil.Instance.augmentVowpalWabbitArguments(vwArgs, numTasks, jobUniqueId)

    val args = vwArgs.result

    // TODO: barrier mode?
    // TODO: check w/ Stage ID (different stages)
    val encoder = RowEncoder(schema)

    df
      .mapPartitions(inputRows =>
        new TrainingPartitionIterator(
          inputRows,
          localInitialModel,
          synchronizationSchedule,
          args,
          numTasks))(encoder)
      .toDF
  }

  override def transformSchema(schema: StructType): StructType =
    StructType(schema.fields ++ getAdditionalOutputSchema.fields)
}
