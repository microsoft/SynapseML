// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.sql.execution.streaming.continuous

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.LinkedBlockingQueue
import java.util.{Optional, UUID}

import com.microsoft.ml.spark._
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.HTTPServerUtils
import org.apache.spark.sql.sources.v2.reader.streaming._
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.{DataSourceRegister, v2}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.parallel.mutable.{ParHashMap, ParHashSet}

object HTTPSourceStateHolder {

  val factories: mutable.Map[(String, Int), HTTPContinuousDataReader] = mutable.Map()

  val serviceInformation: mutable.Map[String, ParHashSet[ServiceInfo]] = mutable.Map()

}

case class ServiceInfo(host: String,
                       port: Int,
                       name: String,
                       partitionId: Int)

case class ServiceAddedEvent(info: ServiceInfo) extends SparkListenerEvent

object ServiceListener extends SparkListener {

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case ServiceAddedEvent(info) =>
        val infoSet =  HTTPSourceStateHolder.serviceInformation
          .getOrElse(info.name, new ParHashSet[ServiceInfo]())
        infoSet += info
        HTTPSourceStateHolder.serviceInformation.update(info.name, infoSet)
      case _ =>
    }
  }
}

class HTTPSourceProviderV2 extends DataSourceRegister
  with DataSourceV2 with ContinuousReadSupport {

  override def createContinuousReader(
                                       schema: Optional[StructType],
                                       checkpointLocation: String,
                                       options: DataSourceOptions): ContinuousReader = {
    new HTTPContinuousReader(options)
  }

  override def shortName(): String = "HTTPv2"
}

object HTTPSourceProviderV2 {
  val VERSION = 1
}

case class HTTPOffset(partitionToValue: Map[Int, Long])
  extends v2.reader.streaming.Offset {
  implicit val defaultFormats: DefaultFormats = DefaultFormats
  override val json: String = Serialization.write(partitionToValue)
}

case class HTTPPartitionOffset(partition: Int, currentValue: Long) extends PartitionOffset

object HTTPSourceV2 {
  val NUM_PARTITIONS = "numPartitions"
  val HOST = "host"
  val PORT = "port"
  val NAME = "name"

  val SCHEMA: StructType = {
    new StructType()
      .add("id", new StructType()
        .add("requestId", StringType)
        .add("partitionId", IntegerType))
      .add(StructField("request", HTTPSchema.request))
  }

  private[sql] def createInitialOffset(numPartitions: Int) = {
    HTTPOffset(Range(0, numPartitions).map { i => (i, 0L) }.toMap)
  }
}

class HTTPContinuousReader(options: DataSourceOptions)
  extends ContinuousReader {
  implicit val defaultFormats: DefaultFormats = DefaultFormats
  SparkContext.getActive.get.addSparkListener(ServiceListener)

  val numPartitions: Int = options.get(HTTPSourceV2.NUM_PARTITIONS).orElse("5").toInt
  val host: String = options.get(HTTPSourceV2.HOST).orElse("localhost")
  val port: Int = options.getInt(HTTPSourceV2.PORT, 8888)
  val name: String = options.get(HTTPSourceV2.NAME).get

  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = {
    assert(offsets.length == numPartitions)
    val tuples = offsets.map {
      case HTTPPartitionOffset(i, currVal) => (i, currVal)
    }
    HTTPOffset(Map(tuples: _*))
  }

  override def deserializeOffset(json: String): Offset = {
    HTTPOffset(Serialization.read[Map[Int, Long]](json))
  }

  override def readSchema(): StructType = HTTPSourceV2.SCHEMA

  private var offset: Offset = _

  override def setStartOffset(offset: java.util.Optional[Offset]): Unit = {
    this.offset = offset.orElse(HTTPSourceV2.createInitialOffset(numPartitions))
  }

  override def getStartOffset: Offset = offset

  override def createDataReaderFactories(): java.util.List[DataReaderFactory[Row]] = {
    val partitionStartMap = offset match {
      case off: HTTPOffset => off.partitionToValue
      case off =>
        throw new IllegalArgumentException(
          s"invalid offset type ${off.getClass} for ContinuousHTTPSource")
    }
    if (partitionStartMap.keySet.size != numPartitions) {
      throw new IllegalArgumentException(
        s"The previous run contained ${partitionStartMap.keySet.size} partitions, but" +
          s" $numPartitions partitions are currently configured. The numPartitions option" +
          " cannot be changed.")
    }

    Range(0, numPartitions).map { i =>
      val start = partitionStartMap(i)
      HTTPContinuousDataReaderFactory(host, port, name, start, i).asInstanceOf[DataReaderFactory[Row]]
    }.asJava
  }

  override def commit(end: Offset): Unit = {
    println(end)
  }

  override def stop(): Unit = {}

}

case class HTTPReaderInfo(host: String,
                          port: Int,
                          name: String,
                          startValue: Long,
                          partitionIndex: Int)

case class HTTPContinuousDataReaderFactory(host: String,
                                           port: Int,
                                           name: String,
                                           startValue: Long,
                                           partitionIndex: Int)

  extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] =
    new HTTPContinuousDataReader(host, port, name, startValue, partitionIndex)
}

class HTTPContinuousDataReader(host: String,
                               port: Int,
                               name: String,
                               startValue: Long,
                               partitionIndex: Int)

  extends ContinuousDataReader[Row] {

  HTTPSourceStateHolder.factories.update((name, partitionIndex), this)

  class QueueHandler extends HttpHandler {

    override def handle(request: HttpExchange): Unit = {
      requests.put(request)
    }
  }

  def replyTo(id: String, data: HTTPResponseData): Unit = {
    val request = routingTable(id)
    HTTPServerUtils.respond(request, data)
    request.close()
    routingTable.remove(id)
    ()
  }

  def commit(rid: String): Unit = {
    routingTable.remove(rid)
    ()
    //TODO make it so that all requests that are stale are removed
  }

  private def tryCreateServer(host: String, startingPort: Int, triesLeft: Int): (HttpServer, Int) = {
    if (triesLeft == 0) {
      throw new java.net.BindException("Could not find open ports in the range," +
        " try increasing the number of ports to try")
    }
    try {
      val server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(host), startingPort), 100)
      (server, startingPort)
    } catch {
      case _: java.net.BindException =>
        tryCreateServer(host, startingPort + 1, triesLeft - 1)
    }
  }

  /** All batches from `lastCommittedOffset + 1` to `currentOffset`, inclusive.
    * Stored in a ListBuffer to facilitate removing committed batches.
    */
  protected val requests: LinkedBlockingQueue[HttpExchange] = new LinkedBlockingQueue()

  private val (server, foundPort) = tryCreateServer(host, port, 10)
  server.createContext(s"/$name", new QueueHandler)
  server.setExecutor(null)
  server.start()
  println(s"started server at $host:$foundPort")
  SparkContext.getActive.get.listenerBus.post(
    ServiceAddedEvent(ServiceInfo(host, foundPort, name, partitionIndex)))

  private val routingTable: ParHashMap[String, HttpExchange] = ParHashMap()

  private var currentValue = startValue
  private var currentRow: Row = _

  private val requestDataToRow = HTTPRequestData.makeToRowConverter

  override def next(): Boolean = {
    currentValue += 1
    val request = requests.take()
    val id = UUID.randomUUID().toString
    routingTable.put(id, request)
    currentRow = Row(Row(id, partitionIndex), requestDataToRow(HTTPRequestData.fromHTTPExchange(request)))
    true
  }

  override def get: Row = currentRow

  override def close(): Unit = {
    server.stop(0)
    HTTPSourceStateHolder.factories.remove((name, partitionIndex))
    ()
  }

  override def getOffset: PartitionOffset =
    HTTPPartitionOffset(partitionIndex, currentValue)
}

class HTTPSinkProviderV2 extends DataSourceV2
  with StreamWriteSupport
  with DataSourceRegister {

  override def createStreamWriter(
                                   queryId: String,
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
  def createWriterFactory(): DataWriterFactory[Row] = HTTPWriterFactory(idColIndex, replyColIndex, name)

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

}

case class HTTPWriterFactory(idColIndex: Int, replyColIndex: Int, name: String) extends DataWriterFactory[Row] {
  def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    new HTTPDataWriter(partitionId, idColIndex, replyColIndex, name)
  }
}

class HTTPDataWriter(partitionId: Int, val idColIndex: Int,
                     val replyColIndex: Int, val name: String)
  extends DataWriter[Row] with Logging {

  var ids: mutable.ListBuffer[(String, Int)] = new mutable.ListBuffer[(String, Int)]()

  val fromRow = HTTPResponseData.makeFromRowConverter

  override def write(row: Row): Unit = {
    val id = row.getStruct(idColIndex)
    val rid = id.getString(0)
    val pid = id.getInt(1)
    val reply = fromRow(row.getStruct(replyColIndex))
    HTTPSourceStateHolder.factories((name, pid)).replyTo(rid, reply)
    ids.append((rid, pid))
  }

  override def commit(): HTTPCommitMessage = {
    val msg = HTTPCommitMessage(ids.toArray)
    ids.foreach { case (rid, pid) =>
      HTTPSourceStateHolder.factories((name, pid)).commit(rid)
    }
    ids.clear()
    msg
  }

  override def abort(): Unit = {}
}

case class HTTPCommitMessage(ids: Array[(String, Int)]) extends WriterCommitMessage
