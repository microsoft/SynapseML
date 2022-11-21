// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.env

import org.apache.commons.io.IOUtils

import java.io.{ByteArrayOutputStream, InputStream}
import java.util.zip.ZipInputStream
import scala.io.Source
import scala.util.Random

object StreamUtilities {

  import scala.util.{Failure, Success, Try}
  def usingMany[T <: AutoCloseable, U](disposable: Seq[T])(task: Seq[T] => U): Try[U] = {
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

  def usingSource[T <: Source, U](disposable: T)(task: T => U): Try[U] = {
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
    * @param stream       Stream of zip file
    * @param zipfile      File name is only used to construct the names of the entries
    * @param sampleRatio  What fraction of files is returned from zip
    */
  class ZipIterator(stream: InputStream, zipfile: String, random: Random, sampleRatio: Double = 1)
    extends Iterator[(String, Array[Byte])] {

    private val zipStream = new ZipInputStream(stream)

    private def getNext: Option[(String, Array[Byte])] = {
      var entry = zipStream.getNextEntry
      while (entry != null) {  //scalastyle:ignore while
        if (!entry.isDirectory && random.nextDouble < sampleRatio) {

          val filename = zipfile + java.io.File.separator + entry.getName

          //extracting all bytes of a given entry
          val byteStream = new ByteArrayOutputStream
          IOUtils.copy(zipStream, byteStream)
          val bytes = byteStream.toByteArray

          assert(bytes.length == entry.getSize,
            "incorrect number of bytes is read from zipstream: " + bytes.length + " instead of " + entry.getSize)

          return Some((filename, bytes))  //scalastyle:ignore return
        }
        entry = zipStream.getNextEntry
      }

      stream.close()
      None
    }

    private var nextValue = getNext

    def hasNext: Boolean = nextValue.isDefined

    def next: (String, Array[Byte]) = {
      val result = nextValue.get
      nextValue = getNext
      result
    }
  }

}
