// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.io.binary

import com.microsoft.azure.synapse.ml.core.schema.BinaryFileSchema
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.hadoop.fs.Path

import java.io.File
import java.nio.file.Files

class VerifyBinaryFileFormat extends TestBase {

  test("BinaryFileFormat shortName returns 'binary'") {
    val format = new BinaryFileFormat()
    assert(format.shortName() === "binary")
  }

  test("BinaryFileFormat toString returns 'Binary'") {
    val format = new BinaryFileFormat()
    assert(format.toString === "Binary")
  }

  test("BinaryFileFormat isSplitable returns false") {
    val format = new BinaryFileFormat()
    val result = format.isSplitable(spark, Map.empty, new Path("/test"))
    assert(!result)
  }

  test("BinaryFileFormat inferSchema returns BinaryFileSchema") {
    val format = new BinaryFileFormat()
    val schema = format.inferSchema(spark, Map.empty, Seq.empty)
    assert(schema.isDefined)
    assert(schema.get === BinaryFileSchema.Schema)
  }

  test("BinaryFileFormat equals returns true for same type") {
    val format1 = new BinaryFileFormat()
    val format2 = new BinaryFileFormat()
    assert(format1.equals(format2))
  }

  test("BinaryFileFormat equals returns false for different type") {
    val format = new BinaryFileFormat()
    assert(!format.equals("not a format"))
  }

  test("BinaryFileFormat hashCode is consistent") {
    val format1 = new BinaryFileFormat()
    val format2 = new BinaryFileFormat()
    assert(format1.hashCode() === format2.hashCode())
  }

  test("ConfUtils.getHConf returns SerializableConfiguration") {
    import spark.implicits._
    val df = Seq(1, 2, 3).toDF("num")
    val hConf = ConfUtils.getHConf(df)
    assert(hConf != null)
  }

  test("BinaryFileFormat can read binary files") {
    // Create a temp directory with binary files
    val tempDir = Files.createTempDirectory("binary-test").toFile
    tempDir.deleteOnExit()

    val testFile = new File(tempDir, "test.bin")
    Files.write(testFile.toPath, Array[Byte](1, 2, 3, 4, 5))
    testFile.deleteOnExit()

    val df = spark.read.format("binary").load(tempDir.getAbsolutePath)
    assert(df.count() >= 1)
    assert(df.schema === BinaryFileSchema.Schema)
  }

  test("BinaryFileFormat reads file content correctly") {
    val tempDir = Files.createTempDirectory("binary-content-test").toFile
    tempDir.deleteOnExit()

    val testContent = "Hello, Binary!".getBytes
    val testFile = new File(tempDir, "content.bin")
    Files.write(testFile.toPath, testContent)
    testFile.deleteOnExit()

    val df = spark.read.format("binary").load(tempDir.getAbsolutePath)
    val row = df.collect().head
    val struct = row.getStruct(0)
    val bytes = struct.getAs[Array[Byte]]("bytes")
    assert(bytes.sameElements(testContent))
  }

  test("BinaryFileFormat respects subsample option") {
    val tempDir = Files.createTempDirectory("binary-subsample-test").toFile
    tempDir.deleteOnExit()

    // Create multiple files
    for (i <- 1 to 10) {
      val testFile = new File(tempDir, s"file$i.bin")
      Files.write(testFile.toPath, Array[Byte](i.toByte))
      testFile.deleteOnExit()
    }

    // Read with subsample=0.0 should return fewer or no rows
    val df = spark.read.format("binary")
      .option("subsample", "0.0")
      .load(tempDir.getAbsolutePath)
    assert(df.count() === 0)
  }

  test("BinaryFileFormat reads multiple files") {
    val tempDir = Files.createTempDirectory("binary-multi-test").toFile
    tempDir.deleteOnExit()

    for (i <- 1 to 3) {
      val testFile = new File(tempDir, s"multi$i.bin")
      Files.write(testFile.toPath, s"content$i".getBytes)
      testFile.deleteOnExit()
    }

    val df = spark.read.format("binary").load(tempDir.getAbsolutePath)
    assert(df.count() === 3)
  }
}
