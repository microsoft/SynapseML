// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.nn

import breeze.linalg.DenseVector
import com.microsoft.azure.synapse.ml.core.test.base.TestBase

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import com.microsoft.azure.synapse.ml.core.utils.SafeObjectInputStream

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream}

class VerifySchemas extends TestBase {

  test("Ball stores center and radius correctly") {
    val center = DenseVector(1.0, 2.0, 3.0)
    val ball = Ball(center, 5.0)
    assert(ball.mu === center)
    assert(ball.radius === 5.0)
  }

  test("BestMatch stores index and distance correctly") {
    val bm = BestMatch(42, 3.14)
    assert(bm.index === 42)
    assert(bm.distance === 3.14)
  }

  test("LeafNode toString contains data size") {
    val ball = Ball(DenseVector(0.0), 1.0)
    val leaf = LeafNode(Seq(1, 2, 3), ball)
    assert(leaf.toString.contains("data size of 3"))
  }

  test("InnerNode toString contains ball info") {
    val ball = Ball(DenseVector(1.0, 2.0), 4.0)
    val leftChild = LeafNode(Seq(0), Ball(DenseVector(1.0), 1.0))
    val rightChild = LeafNode(Seq(1), Ball(DenseVector(2.0), 1.0))
    val inner = InnerNode(ball, leftChild, rightChild)
    assert(inner.toString.contains("InnerNode"))
    assert(inner.toString.contains("Ball"))
  }

  test("LeafNode stores point indices") {
    val indices = Seq(10, 20, 30)
    val leaf = LeafNode(indices, Ball(DenseVector(0.0), 1.0))
    assert(leaf.pointIdx === indices)
  }

  test("Ball with zero radius") {
    val ball = Ball(DenseVector(5.0, 6.0), 0.0)
    assert(ball.radius === 0.0)
  }

  test("BestMatch is serializable") {
    val bm = BestMatch(7, 2.5)
    val baos = new ByteArrayOutputStream()
    using(new ObjectOutputStream(baos)) { oos =>
      oos.writeObject(bm)
    }

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val deserialized = using(new SafeObjectInputStream(bais, SafeObjectInputStream.DefaultNNAllowedPrefixes)) { ois =>
      ois.readObject().asInstanceOf[BestMatch]
    }.get

    assert(deserialized.index === 7)
    assert(deserialized.distance === 2.5)
  }

  test("SafeObjectInputStream rejects unauthorized classes") {
    val malicious = new java.util.concurrent.atomic.AtomicReference[String]("payload")
    val baos = new ByteArrayOutputStream()
    using(new ObjectOutputStream(baos)) { oos =>
      oos.writeObject(malicious)
    }

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val restrictedPrefixes = Set("com.microsoft.azure.synapse.ml.nn.")
    val result = using(new SafeObjectInputStream(bais, restrictedPrefixes)) { ois =>
      ois.readObject()
    }
    assert(result.isFailure)
    assert(result.failed.get.isInstanceOf[java.io.InvalidClassException])
  }

  test("SafeObjectInputStream allows primitive arrays") {
    val data = Array(1.0, 2.0, 3.0)
    val baos = new ByteArrayOutputStream()
    using(new ObjectOutputStream(baos)) { oos =>
      oos.writeObject(data)
    }

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val deserialized = using(new SafeObjectInputStream(bais, Set("java.lang."))) { ois =>
      ois.readObject().asInstanceOf[Array[Double]]
    }.get

    assert(deserialized.sameElements(data))
  }

  test("SafeObjectInputStream allows object arrays with permitted component type") {
    val data = Array("hello", "world")
    val baos = new ByteArrayOutputStream()
    using(new ObjectOutputStream(baos)) { oos =>
      oos.writeObject(data)
    }

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val deserialized = using(new SafeObjectInputStream(bais, Set("java.lang."))) { ois =>
      ois.readObject().asInstanceOf[Array[String]]
    }.get

    assert(deserialized.sameElements(data))
  }

  test("SafeObjectInputStream rejects object arrays with disallowed component type") {
    val data = Array(new java.util.concurrent.atomic.AtomicInteger(1))
    val baos = new ByteArrayOutputStream()
    using(new ObjectOutputStream(baos)) { oos =>
      oos.writeObject(data)
    }

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val result = using(new SafeObjectInputStream(bais, Set("java.lang."))) { ois =>
      ois.readObject()
    }
    assert(result.isFailure)
    assert(result.failed.get.isInstanceOf[java.io.InvalidClassException])
  }

  test("SafeObjectInputStream rejects dynamic proxies with disallowed interfaces") {
    val proxy = java.lang.reflect.Proxy.newProxyInstance(
      getClass.getClassLoader,
      Array(classOf[Runnable]),
      (_: Any, _: java.lang.reflect.Method, _: Array[AnyRef]) => None.orNull
    )
    val baos = new ByteArrayOutputStream()
    using(new ObjectOutputStream(baos)) { oos =>
      oos.writeObject(proxy)
    }

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val result = using(new SafeObjectInputStream(bais, Set("com.microsoft.azure.synapse.ml.nn."))) { ois =>
      ois.readObject()
    }
    assert(result.isFailure)
    assert(result.failed.get.isInstanceOf[java.io.InvalidClassException])
  }

}
