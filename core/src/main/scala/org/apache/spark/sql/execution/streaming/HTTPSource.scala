// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.sql.execution.streaming

import com.microsoft.azure.synapse.ml.io.http.{HTTPRequestData, HTTPResponseData}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.streaming.continuous.HTTPSourceV2
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.net.{InetAddress, InetSocketAddress}
import javax.annotation.concurrent.GuardedBy
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object HTTPServerUtils {

  def respond(request: HttpExchange, data: HTTPResponseData): Unit = {
    data.respondToHTTPExchange(request)
  }
}

object HTTPSource {

  // Global datastructure that holds the callbacks (function taking request ID and data and sends response)
  // for the server (keys are server names)
  val ReplyCallbacks: mutable.Map[String, (String, HTTPResponseData) => Unit] = mutable.Map()

}

/** A source that reads text lines through a TCP socket, designed only for tutorials and debugging.
  * This source will *not* work in production applications due to multiple reasons, including no
  * support for fault recovery and keeping all of the text read in memory forever.
  */
class HTTPSource(name: String, host: String, port: Int, sqlContext: SQLContext)
    extends Source with Logging {

  class QueueHandler extends HttpHandler {

    override def handle(request: HttpExchange): Unit = {
      HTTPSource.this.synchronized {
        currentOffset = currentOffset + 1
        requests.append((currentOffset.offset, request))
      }
    }
  }

  /** All batches from `lastCommittedOffset + 1` to `currentOffset`, inclusive.
    * Stored in a ListBuffer to facilitate removing committed batches.
    */
  @GuardedBy("this")
  protected val requests: ListBuffer[(Long, HttpExchange)] = ListBuffer()

  @GuardedBy("this")
  protected var currentOffset: LongOffset = new LongOffset(-1)

  @GuardedBy("this")
  protected var lastOffsetCommitted: LongOffset = new LongOffset(-1)

  @GuardedBy("this")
  private val server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(host), port), 0)
  server.createContext(s"/$name", new QueueHandler)
  server.setExecutor(null) //scalastyle:ignore null
  server.start()
  HTTPSource.ReplyCallbacks.update(name, reply)

  /** Returns the schema of the data from this source */
  override def schema: StructType = HTTPSourceV2.Schema

  override def getOffset: Option[Offset] = synchronized {
    if (currentOffset.offset == -1) None else Some(currentOffset)
  }

  /** Returns the data that is between the offsets (`start`, `end`]. */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = synchronized {
    val startOrdinal = start.map(_.asInstanceOf[LongOffset]).getOrElse(LongOffset(-1)).offset.toInt + 1
    val endOrdinal = Option(end).map(_.asInstanceOf[LongOffset]).getOrElse(LongOffset(-1)).offset.toInt + 1

    val hrdToIr = HTTPRequestData.makeToInternalRowConverter

    // Internal buffer only holds the batches after lastOffsetCommitted
    val rawList = synchronized {
      val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
      requests.slice(sliceStart, sliceEnd).map{ case(id, request) =>
        val row = new GenericInternalRow(2)
        val idRow = new GenericInternalRow(3)
        idRow.update(0, null) //scalastyle:ignore null
        idRow.update(1, UTF8String.fromString(id.toString))
        idRow.update(2, null) //scalastyle:ignore null
        row.update(0, idRow)
        row.update(1, hrdToIr(HTTPRequestData.fromHTTPExchange(request)))
        row.asInstanceOf[InternalRow]
      }
    }
    val rawBatch = if (rawList.nonEmpty) {
      sqlContext.sparkContext.parallelize(rawList)
    } else {
      sqlContext.sparkContext.emptyRDD[InternalRow]
    }

    sqlContext.sparkSession
      .internalCreateDataFrame(rawBatch, schema, isStreaming = true)
  }

  def reply(id: String, reply: HTTPResponseData): Unit = {
    val request = requests((id.toInt - lastOffsetCommitted.offset).toInt - 1)
    HTTPServerUtils.respond(request._2, reply)
  }

  override def commit(end: Offset): Unit = synchronized {
    val newOffset = end.json().toInt
    val offsetDiff = (newOffset - lastOffsetCommitted.offset).toInt
    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }
    requests.trimStart(offsetDiff)
    lastOffsetCommitted = LongOffset(newOffset)
  }

  /** Stop this source. */
  override def stop(): Unit = synchronized {
    server.stop(0)
    HTTPSource.ReplyCallbacks.remove(name)
    ()
  }

  override def toString: String = s"HTTPSource[name: $name, host: $host, port: $port]"

}

class HTTPSourceProvider extends StreamSourceProvider with DataSourceRegister with Logging {

  /** Returns the name and schema of the source that can be used to continually read data. */
  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    logWarning("The socket source should not be used for production applications! " +
                 "It does not support recovery.")
    if (!parameters.contains("host")) {
      throw new AnalysisException("Set a host to read from with option(\"host\", ...).")
    }
    if (!parameters.contains("port")) {
      throw new AnalysisException("Set a port to read from with option(\"port\", ...).")
    }
    if (!parameters.contains("path")) {
      throw new AnalysisException("Set a name of the API which is used for routing")
    }
    ("HTTP", HTTPSourceV2.Schema)
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {
    val host = parameters("host")
    val port = parameters("port").toInt
    val name = parameters("path")
    val source = new HTTPSource(name, host, port, sqlContext)
    source
  }

  /** String that represents the format that this data source provider uses. */
  override def shortName(): String = "HTTP"

}

class HTTPSink(val options: Map[String, String]) extends Sink with Logging {

  if (!options.contains("name")) {
    throw new AnalysisException("Set a name of an API to reply to")
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {
    val replyCol = options.getOrElse("replyCol", "reply")
    val idCol = options.getOrElse("idCol", "id")
    val idColIndex = data.schema.fieldIndex(idCol)
    val replyColIndex = data.schema.fieldIndex(replyCol)

    val replyType = data.schema(replyCol).dataType
    val idType = data.schema(idCol).dataType
    assert(replyType == HTTPResponseData.schema, s"Reply col is $replyType, need HTTPResponseData Type")
    assert(idType == HTTPSourceV2.IdSchema, s"id col is $idType, need ${HTTPSourceV2.IdSchema}")

    val irToResponseData = HTTPResponseData.makeFromInternalRowConverter

    val replies = data.queryExecution.toRdd.map { ir =>
      //scalastyle:off magic.number
      (ir.getStruct(idColIndex, 3).getString(1), irToResponseData(ir.getStruct(replyColIndex, 4)))
      //scalastyle:on magic.number

      // 4 is the Number of fields of HTTPResponseData,
      // there does not seem to be a way to get this w/o reflection
    }.collect()

    val callback = HTTPSource.ReplyCallbacks(options("name"))
    replies.foreach(callback.tupled)
  }

}

class HTTPSinkProvider extends StreamSinkProvider with DataSourceRegister {

  def createSink(sqlContext: SQLContext,
                 parameters: Map[String, String],
                 partitionColumns: Seq[String],
                 outputMode: OutputMode): Sink = {
    new HTTPSink(parameters)
  }

  def shortName(): String = "HTTP"

}
