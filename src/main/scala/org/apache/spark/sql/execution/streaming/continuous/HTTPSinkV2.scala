// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.sql.execution.streaming.continuous

import com.microsoft.ml.spark._
import com.microsoft.ml.spark.io.http.HTTPResponseData
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

import scala.collection.mutable

class HTTPSinkProviderV2 extends DataSourceV2
  with StreamWriteSupport
  with DataSourceRegister {

  override def createStreamWriter(queryId: String,
                                  schema: StructType,
                                  mode: OutputMode,
                                  options: DataSourceOptions): StreamWriter = {
    new HTTPWriter(schema, options)
  }

  def shortName(): String = "HTTPv2"
}

/** Common methods used to create writes for the the console sink */
class HTTPWriter(schema: StructType, options: DataSourceOptions)
  extends StreamWriter with Logging {

  protected val idCol: String = options.get("idCol").orElse("id")
  protected val replyCol: String = options.get("replyCol").orElse("reply")
  protected val name: String = options.get("name").get

  val idColIndex: Int = schema.fieldIndex(idCol)
  val replyColIndex: Int = schema.fieldIndex(replyCol)

  assert(SparkSession.getActiveSession.isDefined)

  def createWriterFactory(): DataWriterFactory[InternalRow] =
    HTTPWriterFactory(idColIndex, replyColIndex, name)

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    HTTPSourceStateHolder.cleanUp(name)
  }

}

private[streaming] case class HTTPWriterFactory(idColIndex: Int,
                                                replyColIndex: Int,
                                                name: String)
  extends DataWriterFactory[InternalRow] {
  def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new HTTPDataWriter(partitionId, idColIndex, replyColIndex, name, epochId)
  }
}

private[streaming] class HTTPDataWriter(partitionId: Int,
                                        val idColIndex: Int,
                                        val replyColIndex: Int,
                                        val name: String,
                                        epochId: Long)
  extends DataWriter[InternalRow] with Logging {
  logInfo(s"Creating writer on PID:$partitionId")
  HTTPSourceStateHolder.getServer(name).commit(epochId - 1, partitionId)

  private val ids: mutable.ListBuffer[(String, Int)] = new mutable.ListBuffer[(String, Int)]()

  private val fromRow = HTTPResponseData.makeFromInternalRowConverter

  override def write(row: InternalRow): Unit = {
    val id = row.getStruct(idColIndex, 2)
    val mid = id.getString(0)
    val rid = id.getString(1)
    val pid = id.getInt(2)
    val reply = fromRow(row.getStruct(replyColIndex, 4)) //scalastyle:ignore magic.number
    HTTPSourceStateHolder.getServer(name).replyTo(mid, rid, reply)
    ids.append((rid, pid))
  }

  override def commit(): HTTPCommitMessage = {
    val msg = HTTPCommitMessage(ids.toArray)
    ids.foreach { case (rid, _) =>
      HTTPSourceStateHolder.getServer(name).commit(rid)
    }
    ids.clear()
    msg
  }

  override def abort(): Unit = {
    if (TaskContext.get().getKillReason().contains("Stage cancelled")) {
      HTTPSourceStateHolder.cleanUp(name)
    }
  }
}

private[streaming] case class HTTPCommitMessage(ids: Array[(String, Int)]) extends WriterCommitMessage
