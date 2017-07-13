// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import java.io.ByteArrayOutputStream
import java.util.zip.ZipInputStream
import org.apache.commons.io.IOUtils
import org.apache.spark.input.PortableDataStream
import scala.util.Random

object StreamUtilities {

  import scala.util.{Try, Success, Failure}
  def using[T <: AutoCloseable, U](disposable: Seq[T])(task: Seq[T] => U): Try[U] = {
    try {
      Success(task(disposable))
    } catch {
      case e: Exception => Failure(e)
    } finally {
      disposable.foreach(d => d.close())
    }
  }

  def using[T <: AutoCloseable, U](disposable: T)(task: T => U): Try[U] = {
    try {
      Success(task(disposable))
    } catch {
      case e: Exception => Failure(e)
    } finally {
      disposable.close()
    }
  }

  /** Iterate through the entries of a streamed .zip file, selecting only sampleRatio of them
    *
    * @param portableStream  Stream of zip file
    * @param zipfile         File name is only used to construct the names of the entries
    * @param sampleRatio     What fraction of files is returned from zip
    */
  class ZipIterator(portableStream: PortableDataStream, zipfile: String, sampleRatio: Double = 1)
    extends Iterator[(String, Array[Byte])] {

    val stream = portableStream.open
    private val zipstream = new ZipInputStream(stream)

    val random = {
      val rd = new Random()
      rd.setSeed(0)
      rd
    }

    private def getNext: Option[(String, Array[Byte])] = {
      var entry = zipstream.getNextEntry
      while (entry != null) {
        if (!entry.isDirectory && random.nextDouble < sampleRatio) {

          val filename = zipfile + java.io.File.separator + entry.getName()

          //extracting all bytes of a given entry
          val byteStream = new ByteArrayOutputStream
          IOUtils.copy(zipstream, byteStream)
          val bytes = byteStream.toByteArray

          assert(bytes.length == entry.getSize,
            "incorrect number of bytes is read from zipstream: " + bytes.length + " instead of " + entry.getSize)

          return Some((filename, bytes))
        }
        entry = zipstream.getNextEntry
      }

      stream.close()
      None
    }

    private var nextValue = getNext

    def hasNext: Boolean = !nextValue.isEmpty

    def next: (String, Array[Byte]) = {
      val result = nextValue.get
      nextValue = getNext
      result
    }
  }

}
