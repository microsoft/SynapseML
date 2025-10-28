// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.sql.execution.streaming

import com.microsoft.azure.synapse.ml.io.http.{HTTPRequestData, HTTPResponseData, SharedSingleton}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2}
import org.apache.spark.sql.execution.streaming.continuous.HTTPSourceV2
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

import java.net.{InetAddress, InetSocketAddress}
import java.util.UUID
import java.util.concurrent.Executors
import javax.annotation.concurrent.GuardedBy
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}

class MultiChannelMap[K, V](var nLists: Int) {

  @GuardedBy("this")
  private val map: mutable.HashMap[K, V] = new mutable.HashMap()

  @GuardedBy("this")
  private val lists: mutable.ListBuffer[mutable.ListBuffer[(K, V)]] =
    new mutable.ListBuffer() ++ (1 to nLists).map(_ => new mutable.ListBuffer[(K, V)]())

  @GuardedBy("this")
  private var getIndex = 0

  def updateNLists(n: Int): Unit = synchronized {
    getIndex = 0
    addIndex = 0
    val oldNLists = nLists
    nLists = n
    if (n > oldNLists) {
      lists.append(new mutable.ListBuffer[(K, V)]())
    } else if (n < nLists) { // TODO this is always false?
      val listsToDisperse = lists.slice(0, oldNLists - n)
      lists.remove(0, oldNLists - n)
      listsToDisperse.flatten.foreach(p => addToNextList(p._1, p._2))
    }
  }

  private def getAndIncrementGetIndex(): Int = synchronized {
    val oldIndex = getIndex
    getIndex = (getIndex + 1) % nLists
    oldIndex
  }

  def get(k: K): V = synchronized(map(k))

  private def getAll(i: Int): mutable.ListBuffer[(K, V)] =
    synchronized(lists(i))

  def nextList(): ListBuffer[(K, V)] = synchronized {
    getAll(getAndIncrementGetIndex())
  }

  private def add(i: Int, k: K, v: V): Unit = synchronized {
    map.update(k: K, v: V)
    lists(i).append((k, v))
  }

  @GuardedBy("this")
  private var addIndex = 0

  private def getAndIncrementAddIndex(): Int = synchronized {
    val oldIndex = addIndex
    addIndex = (addIndex + 1) % nLists
    oldIndex
  }

  def addToNextList(k: K, v: V): Unit = synchronized {
    add(getAndIncrementAddIndex(), k, v)
  }

  def size: Int = synchronized(map.size)

}

class JVMSharedServer(name: String, host: String,
                      port: Int, maxAttempts: Int,
                      handleResponseErrors: Boolean) extends Logging {

  type Body = HTTPRequestData
  type Exchange = HttpExchange
  type Request = (Body, Exchange)
  type ID = String

  @GuardedBy("this")
  private var requestsSeen = 0L
  @GuardedBy("this")
  private var requestsAccepted = 0L
  @GuardedBy("this")
  private var currentBatch = 0L
  @GuardedBy("this")
  private var earliestBatch = 0L
  @GuardedBy("this")
  private var nPartitions = 0

  val serverIdentity: String = UUID.randomUUID().toString

  @GuardedBy("this")
  private val batchesToRequests: mutable.HashMap[Long, MultiChannelMap[ID, Request]] =
    new mutable.HashMap[Long, MultiChannelMap[ID, Request]]()

  def updateNPartitions(n: Int): Unit = synchronized {
    nPartitions = n
    batchesToRequests.valuesIterator.foreach(_.updateNLists(n))
  }

  def updateCurrentBatch(currentOffset: Long): Unit = synchronized {
    currentBatch = currentOffset
  }

  def incrementCurrentBatch(): Unit = synchronized {
    currentBatch = currentBatch + 1
  }

  def getRequests(start: Long, end: Long): immutable.IndexedSeq[(ID, Body)] = synchronized {
    // Increment the current batch so new requests are routed to the next batch
    if (end == currentBatch) incrementCurrentBatch()
    val requests = ((start + 1) to end).flatMap { i =>
        val mcm = batchesToRequests.getOrElse(i, new MultiChannelMap[ID, Request](nPartitions))
        mcm.nextList().map(p => (p._1, p._2._1))
      }
    logDebug(s"getting ${requests.length} on $address batch $currentBatch")
    requests
  }

  def trimBatchesBefore(batch: Long): Unit = synchronized {
    val start = earliestBatch
    earliestBatch = batch
      (start to batch).foreach(batchesToRequests.remove)
    logDebug(s"trimming ${(start to batch).toList} on $address")
  }

  def respond(batch: Long, uuid: String, response: HTTPResponseData): Unit = synchronized {
    val request = batchesToRequests(batch).get(uuid)._2
    response.respondToHTTPExchange(request)
  }

  private class RequestHandler extends HttpHandler {

    override def handle(request: HttpExchange): Unit = synchronized {
      requestsSeen += 1
      val requestData = HTTPRequestData.fromHTTPExchange(request)
      val uuid = UUID.randomUUID().toString
      requestsAccepted += 1
      val cb = currentBatch.longValue()
      batchesToRequests.get(cb) match {
        case None =>
          val mcm = new MultiChannelMap[ID, Request](nPartitions)
          mcm.addToNextList(uuid, (requestData, request))
          batchesToRequests.update(cb, mcm)
        case Some(mcm) => mcm.addToNextList(uuid, (requestData, request))
      }
      logDebug(s"handling $requestData batch: $currentBatch ip: $address")
    }
  }

  @tailrec
  private def tryCreateServer(host: String, startingPort: Int, triesLeft: Int): (HttpServer, Int) = {
    if (triesLeft == 0) {
      throw new java.net.BindException("Could not find open ports in the range," +
        " try increasing the number of ports to try")
    }
    try {
      val server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(host), startingPort),
                            100)  //scalastyle:ignore magic.number
      (server, startingPort)
    } catch {
      case _: java.net.BindException =>
        tryCreateServer(host, startingPort + 1, triesLeft - 1)
    }
  }

  @GuardedBy("this")
  private val (server, serverPort) = tryCreateServer(host, port, maxAttempts)
  server.createContext(s"/$name", new RequestHandler)
  server.setExecutor(Executors.newFixedThreadPool(100))  //scalastyle:ignore magic.number
  server.start()

  val address: String = server.getAddress.getHostString + ":" + serverPort
  val machine: String = InetAddress.getLocalHost.toString

  def stop(): Unit = synchronized {
    server.stop(0)
    server.synchronized {
      server.notifyAll()
    }
  }

}

class DistributedHTTPSource(name: String,
                            host: String,
                            port: Int,
                            maxPortAttempts: Int,
                            maxPartitions: Option[Int],
                            handleResponseErrors: Boolean,
                            sqlContext: SQLContext)
    extends Source with Logging with Serializable {

  import sqlContext.implicits._

  private[spark] val server = SharedSingleton {
    new JVMSharedServer(name, host, port, maxPortAttempts, handleResponseErrors)
  }

  private[spark] val infoSchema = new StructType()
    .add("machine", StringType).add("ip", StringType).add("id", StringType)

  private[spark] val infoEnc = RowEncoder(infoSchema)

  // Access point to run code on nodes through mapPartitions
  // TODO do this by hooking deeper into spark,
  // TODO allow for dynamic allocation
  private[spark] val serverInfoDF: DataFrame = {
    val serverInfo = sqlContext.sparkContext
      .parallelize(Seq(Tuple1("placeholder")),
        maxPartitions.getOrElse(sqlContext.sparkContext.defaultParallelism))
      .toDF("empty")
      .mapPartitions { _ =>
        val s = server.get
        Iterator(Row(s.machine, s.address, s.serverIdentity))
      }(infoEnc)

    val serverToNumPartitions = serverInfo
      .select("id").groupBy("id").count().collect().map(r => (r.getString(0), r.getLong(1).toInt)).toMap
    val serverInfoConfigured = serverInfo.mapPartitions { it =>
      server.get.updateNPartitions(serverToNumPartitions(server.get.serverIdentity))
      it
    }(infoEnc).cache()
    serverInfoConfigured.collect() // materialize to trigger setup

    logInfo("Got or Created services: "
      + serverInfoConfigured.collect().map {r =>
        s"\n\t machine: ${r.getString(0)}, address: ${r.getString(1)}, guid: ${r.getString(2)}"
      }.toList.mkString(", "))
    serverInfoConfigured
  }

  private[spark] val serverInfoDFStreaming = {
    val serializer = infoEnc.createSerializer()
    val serverInfoConfRDD = serverInfoDF.rdd.map(serializer)
    sqlContext.sparkSession.internalCreateDataFrame(
      serverInfoConfRDD, schema, isStreaming = true)
  }

  @GuardedBy("this")
  protected var currentOffset: LongOffset = new LongOffset(-1)

  @GuardedBy("this")
  protected var lastOffsetCommitted: LongOffset = new LongOffset(-1)

  /** Returns the schema of the data from this source */
  override def schema: StructType = HTTPSourceV2.Schema

  // Note we assume that this function is only called once during the polling of a new batch
  override def getOffset: Option[Offset] = synchronized {
    currentOffset += 1
    Some(currentOffset)
  }

  /** Returns the data that is between the offsets (`start`, `end`]. */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = synchronized {
    val startOrdinal = start.map(_.asInstanceOf[LongOffset]).getOrElse(LongOffset(-1)).offset
    val endOrdinal = Option(end).map(_.asInstanceOf[LongOffset]).getOrElse(LongOffset(-1)).offset


    val toRow = HTTPRequestData.makeToRowConverter
    serverInfoDFStreaming.mapPartitions { _ =>
      val s = server.get
      s.updateCurrentBatch(currentOffset.offset)
      s.getRequests(startOrdinal, endOrdinal)
        .map{ case (id, request) =>
          Row.fromSeq(Seq(Row(null, id, null), toRow(request)))  //scalastyle:ignore null
        }.toIterator
    }(RowEncoder(HTTPSourceV2.Schema))
  }

  override def commit(end: OffsetV2): Unit = synchronized {
    val newOffset = Option(end).map(_.asInstanceOf[LongOffset]).getOrElse(
      sys.error(s"DistributedHTTPSource.commit() received an offset ($end) that did not " +
        s"originate with an instance of this class")
    )
    if (newOffset.offset < lastOffsetCommitted.offset) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }
    serverInfoDF.foreachPartition((f: Iterator[Row]) =>
      server.get.trimBatchesBefore(newOffset.offset))
    lastOffsetCommitted = newOffset
  }

  /** Stop this source. */
  override def stop(): Unit = synchronized {
    serverInfoDF.foreachPartition((f: Iterator[Row]) =>
      server.get.stop())
    ()
  }

  override def toString: String = s"DistributedHTTPSource[name: $name, host: $host, port: $port]"

}

class DistributedHTTPSourceProvider extends StreamSourceProvider with DataSourceRegister with Logging {

  /** Returns the name and schema of the source that can be used to continually read data. */
  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    if (!parameters.contains("host")) {
      throw new AnalysisException("Set a host to read from with option(\"host\", ...).")
    }
    if (!parameters.contains("port")) {
      throw new AnalysisException("Set a port to read from with option(\"port\", ...).")
    }
    if (!parameters.contains("path")) {
      throw new AnalysisException("Set a name of the API which is used for routing")
    }
    ("DistributedHTTP", HTTPSourceV2.Schema)
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {
    val host = parameters("host")
    val port = parameters("port").toInt
    val name = parameters("path")
    val maxPartitions = parameters.get("maxPartitions").map(_.toInt)
    val maxAttempts = parameters.getOrElse("maxPortAttempts", "10").toInt
    val handleResponseErrors = parameters.getOrElse("handleResponseErrors", "false").toBoolean
    val source = new DistributedHTTPSource(
      name, host, port, maxAttempts, maxPartitions, handleResponseErrors, sqlContext)
    DistributedHTTPSink.ActiveSinks(parameters("path")).linkWithSource(source)

    parameters.get("deployLoadBalancer") match {
      case Some(s) if s.toBoolean =>
        //AzureLoadBalancer.deployFromParameters(parameters)
        throw new NotImplementedError("Support for automatic deployment coming soon")
      case _ =>
    }
    source
  }

   /** String that represents the format that this data source provider uses. */
  override def shortName(): String = "DistributedHTTP"

}

class DistributedHTTPSink(val options: Map[String, String])
    extends Sink with Logging with Serializable {

  if (!options.contains("name")) {
    throw new AnalysisException("Set a name of an API to reply to")
  }
  override def name: String = options("name")

  DistributedHTTPSink.ActiveSinks.update(name, this)

  private var source: DistributedHTTPSource = _

  def getServerAddresses: List[String] = {
    source.serverInfoDF.collect().toList.map(_.getString(0)).distinct
  }

  private[spark] def linkWithSource(s: DistributedHTTPSource): Unit = {
    source = s
  }

  private def server: SharedSingleton[JVMSharedServer] = source.server

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
    data.queryExecution.toRdd.map { ir =>
      //scalastyle:off magic.number
      (ir.getStruct(idColIndex, 3).getString(1), irToResponseData(ir.getStruct(replyColIndex, 4)))
      //scalastyle:on magic.number
    }.foreach { case (id, value) =>
      server.get.respond(batchId, id, value)
    }
  }

}

class DistributedHTTPSinkProvider extends StreamSinkProvider with DataSourceRegister {

  def createSink(sqlContext: SQLContext,
                 parameters: Map[String, String],
                 partitionColumns: Seq[String],
                 outputMode: OutputMode): Sink = {
    new DistributedHTTPSink(parameters)
  }

  def shortName(): String = "DistributedHTTP"

}

object DistributedHTTPSink {

  val ActiveSinks: mutable.Map[String, DistributedHTTPSink] = mutable.Map()

}
