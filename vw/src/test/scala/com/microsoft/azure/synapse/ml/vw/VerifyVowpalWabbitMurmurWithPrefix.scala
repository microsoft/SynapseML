// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.vowpalwabbit.spark.VowpalWabbitMurmur

import java.nio.charset.StandardCharsets

class VerifyVowpalWabbitMurmurWithPrefix extends TestBase {

  case class Sample1(val str: String, val seq: Seq[String])

  test("Verify VowpalWabbitMurmurWithPrefix-based hash produces same results") {
    val prefix = "Markus"

    val fastStringHash = new VowpalWabbitMurmurWithPrefix(prefix)

    var time1: Long = 0
    var time2: Long = 0
    var time3: Long = 0

    for (j <- 0 until 1024) {
      val sb = new StringBuilder

      for (i <- 0 until 128) {
        sb.append(i)

        val str = sb.toString

        // prefix caching + manual UTF-8 conversion with byte array re-usage
        var start = System.nanoTime()
        val h1 = fastStringHash.hash(str, 0, str.length, 0)
        time1 += System.nanoTime() - start

        // allocation of new array for Java char to UTF-8 bytes
        start = System.nanoTime()
        val h2 = VowpalWabbitMurmur.hash(prefix + str, 0)
        time2 += System.nanoTime() - start

        //
        start = System.nanoTime()
        val bytes = (prefix + str).getBytes(StandardCharsets.UTF_8)
        val h3 = VowpalWabbitMurmur.hashNative(bytes, 0, bytes.length, 0)
        time3 += System.nanoTime() - start

        assert(h1 == h2)
        assert(h1 == h3)
      }
    }

    println(s"FastStringHashing:   $time1")
    println(s"Java String to UTF8: $time2")
    println(s"Java to C++:         $time3")
  }

  test("Verify VowpalWabbitMurmurWithPrefix verify max-size exceed") {
    val fastStringHash = new VowpalWabbitMurmurWithPrefix("a", 2)

    val longStr = (1 to 32).mkString("_")

    assert(fastStringHash.hash(longStr, 0) == VowpalWabbitMurmur.hash("a" + longStr, 0))
  }

  def verifyHashesAreTheSame(): Unit = {
    Seq("\u0900def", "\ud800\udc00def").foreach { unicodeString =>
      test(s"Verify VowpalWabbitMurmurWithPrefix verify unicode $unicodeString") {
        val fastStringHash = new VowpalWabbitMurmurWithPrefix("abc")

        assert(fastStringHash.hash(unicodeString, 0) == VowpalWabbitMurmur.hash("abc" + unicodeString, 0))
      }
    }
  }

  test("VowpalWabbitMurmurWithPrefix invalid unicode string") {
    assertThrows[Exception] {
      new VowpalWabbitMurmurWithPrefix("abc").hash("\ud800def", 0)
    }
  }
}
