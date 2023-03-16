// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import java.util.concurrent.{BlockingQueue, CountDownLatch, LinkedBlockingQueue}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class DynamicBufferedBatcher[T](val it: Iterator[T],
                                maxBufferSize: Int = Integer.MAX_VALUE)
  extends Iterator[List[T]] {

  val queue: BlockingQueue[T] = new LinkedBlockingQueue[T](maxBufferSize)
  var hasStarted = false
  val finishedLatch = new CountDownLatch(1)

  private val thread: Thread = new Thread {
    override def run(): Unit = {
      while (it.synchronized(it.hasNext)) {  //scalastyle:ignore while
        val datum = it.synchronized(it.next())
        queue.put(datum)
      }
      finishedLatch.countDown()
    }
  }

  override def hasNext: Boolean = {
    if (!hasStarted) {
      it.hasNext
    } else {
      it.synchronized(it.hasNext) ||
        !queue.isEmpty || {
        finishedLatch.await()
        !queue.isEmpty
      }
      // Final clause needed to ensure the fetching thread
      // can finish before the iterator is exhausted.
      // This blocking should be kept in the final clause
      // To optimize performance
    }
  }

  def start(): Unit = {
    hasStarted = true
    thread.start()
  }

  def close(): Unit = {
    thread.interrupt()
  }

  override def next(): List[T] = {
    if (!hasStarted) start()
    assert(hasNext)
    val results = new java.util.ArrayList[T]()
    queue.drainTo(results)
    if (results.isEmpty) List(queue.take()) else results.asScala.toList
  }

}

class FixedBufferedBatcher[T](val it: Iterator[T],
                              batchSize: Int,
                              maxBufferSize: Int = Integer.MAX_VALUE)
  extends Iterator[List[T]] {

  val queue: BlockingQueue[List[T]] = new LinkedBlockingQueue[List[T]](maxBufferSize)
  var hasStarted = false
  val finishedLatch = new CountDownLatch(1)

  private val thread: Thread = new Thread {
    override def run(): Unit = {
      while (it.synchronized(it.hasNext)) {  //scalastyle:ignore while
        val data = it.synchronized(it.take(batchSize).toList)
        queue.put(data)
      }
      finishedLatch.countDown()
    }
  }

  override def hasNext: Boolean = {
    if (!hasStarted) {
      it.hasNext
    } else {
      it.synchronized(it.hasNext) ||
        !queue.isEmpty || {
        finishedLatch.await()
        !queue.isEmpty
      }
      // Final clause needed to ensure the fetching thread
      // can finish before the iterator is exhausted.
      // This blocking should be kept in the final clause
      // To optimize performance
    }
  }

  def start(): Unit = {
    hasStarted = true
    thread.start()
  }

  def close(): Unit = {
    thread.interrupt()
  }

  override def next(): List[T] = {
    if (!hasStarted) start()
    assert(hasNext)
    queue.take()
  }

}

class FixedBatcher[T](val it: Iterator[T],
                      batchSize: Int)
  extends Iterator[List[T]] {

  override def hasNext: Boolean = {
    it.hasNext
  }

  override def next(): List[T] = {
    it.take(batchSize).toList
  }

}

class TimeIntervalBatcher[T](val it: Iterator[T],
                             millis: Int,
                             maxBufferSize: Int)
  extends Iterator[List[T]] {

  override def hasNext: Boolean = {
    it.hasNext
  }

  override def next(): List[T] = {
    val start = System.currentTimeMillis()
    val buffer: ListBuffer[T] = mutable.ListBuffer()
    buffer += it.next()
    while (  //scalastyle:ignore while
      (System.currentTimeMillis()-start < millis) &&
        buffer.lengthCompare(maxBufferSize) < 0 &&
        it.hasNext
    ){buffer += it.next()}
    buffer.toList
  }

}
