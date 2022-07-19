package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.utils.ParamsStringBuilder
import org.apache.spark.TaskContext
import org.apache.spark.ml.Transformer
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StructField, StructType}
import org.vowpalwabbit.spark.VowpalWabbitNative

import java.util.UUID

trait VowpalWabbitBaseProgressive extends Transformer with VowpalWabbitBase {
  class TrainingPartitionIterator(inputRows: Iterator[Row],
                                  localInitialModel: Option[Array[Byte]],
                                  synchronizationSchedule: VowpalWabbitSynchronizationSchedule,
                                  vwArgs: String,
                                  numTasks: Int)
    extends Iterator[Row] {

    // TODO: make sure we don't do double close
//    private def close(): Unit = {
//    }

    // note this is executed on each worker
    lazy val vw = {
      val contextArgs = if (numTasks == 1) "" else s"--node ${TaskContext.get.partitionId}"

      val args = buildCommandLineArguments(vwArgs, contextArgs)

      if (localInitialModel.isEmpty) new VowpalWabbitNative(args)
      else new VowpalWabbitNative(args, localInitialModel.get)
    }

    override def hasNext: Boolean = {
      val ret = inputRows.hasNext

      if (!ret) {
        // blocks until sync in AllReduce use-case is done
        // make sure that all partitions call endPass the same amount of time
        // (and at least once at the end)
        0 to synchronizationSchedule.getFinishTriggerCount() foreach { _ =>
          println(s"AllReduce Finalize ${TaskContext.getPartitionId()}")
          vw.endPass()
        }
        // cleanup
        vw.close()
      }

      ret
    }

    override def next(): Row = {
      try {
        val inputRow = inputRows.next()

        0 until synchronizationSchedule.getAllReduceTriggerCount(inputRow) foreach { _ =>
//          if (TaskContext.getPartitionId() == 0) {
//            println(s"AllReduce ${TaskContext.getPartitionId()}}")
//          }
          vw.endPass()
        }

        // withColumn
        Row.fromSeq(inputRow.toSeq :+ trainFromRow(vw, inputRow))
      }
      catch {
        case e: Exception =>
          println(s"VW failed: ${e.getMessage}")
          vw.close()

          throw new Exception(s"VW failed", e)
      }
    }
  }

  def trainFromRow(vw: VowpalWabbitNative, row: Row): Seq[Any]

  def getAdditionalOutputSchema(): StructType

  override def transform(dataset: Dataset[_]): DataFrame = {
    val df = prepareDataSet(dataset)
    val schema = transformSchema(df.schema)

    val numTasks = df.rdd.getNumPartitions

    val synchronizationSchedule = getSynchronizationSchedule(df)

    // TODO: extend schema w/ predictions
    val encoder = RowEncoder(schema)

    // schedule multiple mapPartitions in
    val localInitialModel = if (isDefined(initialModel)) Some(getInitialModel) else None

    // TODO: this graph cannot be re-used
    val jobUniqueId = Math.abs(UUID.randomUUID.getLeastSignificantBits.toInt).toString

    val vwArgs = getCommandLineArgs()

    if (numTasks > 1)
      VowpalWabbitClusterUtil.Instance.augmentVowpalWabbitArguments(vwArgs, numTasks, jobUniqueId)

    val args = vwArgs.result

    // TODO: barrier mode?
    // TODO: check w/ Stage ID (different stages)
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
    StructType(schema.fields ++ getAdditionalOutputSchema().fields)
}
