// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.sql.execution.streaming.continuous

import com.microsoft.azure.synapse.ml.io.http.HTTPResponseData
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.internal.connector.{SimpleTableProvider, SupportsStreamingUpdateAsAppend}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.sparkproject.dmg.pmml.False

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

case class HTTPRelation(override val sqlContext: SQLContext, data: DataFrame)
  extends BaseRelation {
  override def schema: StructType = data.schema
}


object HTTPSinkTable extends Table with SupportsWrite {

  override def name(): String = "console"

  override def schema(): StructType = StructType(Nil)

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.STREAMING_WRITE).asJava
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteBuilder with SupportsTruncate with SupportsStreamingUpdateAsAppend {
      private val inputSchema: StructType = info.schema()

      override def truncate(): WriteBuilder = this

      override def buildForStreaming(): StreamingWrite = {
        assert(inputSchema != null)
        new HTTPWriter(inputSchema, info.options)
      }
    }
  }
}


class HTTPSinkProviderV2 extends SimpleTableProvider
  with DataSourceRegister
  with CreatableRelationProvider {

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    HTTPSinkTable
  }

  def createRelation(sqlContext: SQLContext,
                     mode: SaveMode,
                     parameters: Map[String, String],
                     data: DataFrame): BaseRelation = {
    HTTPRelation(sqlContext, data)
  }

  def shortName(): String = "HTTPv2"
}

/** Common methods used to create writes for the the console sink */
class HTTPWriter(schema: StructType, options: CaseInsensitiveStringMap)
  extends StreamingWrite with Logging {

  protected val idCol: String = options.getOrDefault("idCol", "id")
  protected val replyCol: String = options.getOrDefault("replyCol", "reply")
  protected val name: String = options.get("name")

  val idColIndex: Int = schema.fieldIndex(idCol)
  val replyColIndex: Int = schema.fieldIndex(replyCol)

  assert(SparkSession.getActiveSession.isDefined)


  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    HTTPSourceStateHolder.cleanUp(name)
  }

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory =
    HTTPWriterFactory(idColIndex, replyColIndex, name)
}

private[streaming] case class HTTPWriterFactory(idColIndex: Int, replyColIndex: Int, name: String)
  extends StreamingDataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new HTTPDataWriter(partitionId, epochId, idColIndex, replyColIndex, name)
  }
}


private[streaming] class HTTPDataWriter(val partitionId: Int,
                                        val epochId: Long,
                                        val idColIndex: Int,
                                        val replyColIndex: Int,
                                        val name: String)
  extends DataWriter[InternalRow] with Logging {
  logDebug(s"Creating writer on parition:$partitionId epoch $epochId")

  val server = HTTPSourceStateHolder.getServer(name)
  if (server.isContinuous) {
    server.commit(epochId - 1, partitionId)
  }

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

  override def close(): Unit = {
    if (TaskContext.get().getKillReason().contains("Stage cancelled")) {
      HTTPSourceStateHolder.cleanUp(name)
    }
  }
}

private[streaming] case class HTTPCommitMessage(ids: Array[(String, Int)]) extends WriterCommitMessage
