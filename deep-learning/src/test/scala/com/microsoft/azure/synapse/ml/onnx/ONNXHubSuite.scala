// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.onnx

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.hadoop.fs.Path

import java.io.FileNotFoundException

class ONNXHubSuite extends TestBase {

  val name = "MNIST"
  val repo = "onnx/models:main"
  val opset = 7
  lazy val hub = new ONNXHub()

  override def beforeAll(): Unit = {
    spark
    super.beforeAll()
  }

  test("list models") {
    val models1 = hub.listModels(model = Some("mnist"), tags = Some(Seq("vision")))
    val models2 = hub.listModels(tags = Some(Seq("vision")))
    val models3 = hub.listModels()

    assert(models1.length > 1)
    assert(models2.length > models1.length)
    assert(models3.length > models2.length)
  }

  test("forceReload and cache") {
    val bytes = hub.load(name, forceReload = true)
    val bytes1 = hub.load(name)
    assert(bytes.length > 2000)
    assert(bytes === bytes1)
  }

  test("custom cache") {
    val hub2 = new ONNXHub(new Path(hub.getDir, "custom"))
    val bytes = hub2.load(name, forceReload = true)
    assert(bytes.length > 2000)
  }

  test("download with opset") {
    val bytes = hub.load(name, opset = Some(8))
    assert(bytes.length > 2000)
    assertThrows[IllegalArgumentException](hub.load(name, opset = Some(-1)))
  }

  test("Manifest not found") {
    assertThrows[FileNotFoundException](hub.load(name, "onnx/models:unknown", silent = true))
  }

  test("verified repo") {
    assert(!hub.verifyRepoRef("mhamilton723/models"))
    assert(!hub.verifyRepoRef("onnx/models:unknown"))
    assert(hub.verifyRepoRef(repo))
  }

  test("get model info") {
    hub.getModelInfo("mnist", opset = Some(8))
    hub.getModelInfo("mnist")
    assertThrows[IllegalArgumentException](hub.getModelInfo("mnist", opset = Some(-1)))
  }
}
