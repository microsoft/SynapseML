// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.serialize

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.utils.DeserializationClassFilter
import com.microsoft.azure.synapse.ml.param.ByteArrayParam
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, ObjectSerializer, Serializer, Transformer}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  File,
  ObjectInputStream,
  StreamCorruptedException
}
import java.util.concurrent.atomic.AtomicBoolean

private object DeserializationTripwire {
  val Triggered = new AtomicBoolean(false)
}

@SerialVersionUID(1L)
private class DeserializationTripwire extends Serializable {
  private def readObject(input: ObjectInputStream): Unit = {
    DeserializationTripwire.Triggered.set(true)
    input.defaultReadObject()
  }
}

private class CloseTrackingInputStream(bytes: Array[Byte]) extends ByteArrayInputStream(bytes) {
  var closed = false

  override def close(): Unit = {
    closed = true
    super.close()
  }
}

private class UnsafePayloadParam(parent: Params, name: String, doc: String)
  extends ComplexParam[DeserializationTripwire](
    parent,
    name,
    doc,
    (_: DeserializationTripwire) => true
  )

class TestEstimatorBase(val uid: String) extends Transformer {
  def this() = this(Identifiable.randomUID("TestEstimatorBase"))

  def transform(dataset: Dataset[_]): DataFrame = dataset.toDF()

  def copy(extra: ParamMap): this.type = defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = schema

}

trait HasByteArrayParam extends Params {
  val byteArray = new ByteArrayParam(this, "byteArray", "bar")

  def getByteArray: Array[Byte] = $(byteArray)

  def setByteArray(value: Array[Byte]): this.type = set(byteArray, value)
}

trait HasStringParam extends Params {
  val stringParam = new Param[String](this, "stringParam", "bar")

  def getStringParam: String = $(stringParam)

  def setStringParam(value: String): this.type = set(stringParam, value)
}

private trait HasUnsafePayloadParam extends Params {
  val unsafePayload = new UnsafePayloadParam(this, "unsafePayload", "test-only unsafe payload")

  def getUnsafePayload: DeserializationTripwire = $(unsafePayload)

  def setUnsafePayload(value: DeserializationTripwire): this.type = set(unsafePayload, value)
}

private class UnsafePayloadParams extends Params with HasUnsafePayloadParam {
  override val uid: String = "unsafe-payload-params"

  override def copy(extra: ParamMap): Params = this
}

class ComplexParamTest(override val uid: String) extends TestEstimatorBase(uid)
  with HasByteArrayParam with ComplexParamsWritable {
  def this() = this(Identifiable.randomUID("ComplexParamTest"))
}

object ComplexParamTest extends ComplexParamsReadable[ComplexParamTest]

class StandardParamTest(override val uid: String) extends TestEstimatorBase(uid)
  with HasStringParam with ComplexParamsWritable {
  def this() = this(Identifiable.randomUID("StandardParamTest"))
}

object StandardParamTest extends ComplexParamsReadable[StandardParamTest]

class MixedParamTest(override val uid: String) extends TestEstimatorBase(uid)
  with HasStringParam with HasByteArrayParam with ComplexParamsWritable {
  def this() = this(Identifiable.randomUID("MixedParamTest"))
}

object MixedParamTest extends ComplexParamsReadable[MixedParamTest]

class ValidateComplexParamSerializer extends TestBase {
  val saveFile = new File(tmpDir.toFile, "m1.model").toString
  val saveFile2 = new File(tmpDir.toFile, "m2.model").toString
  private val legacyDeserializationConfig =
    Serializer.LegacyObjectDeserializationConfig

  private def restoreConfig(key: String, previous: Option[String]): Unit = {
    previous.fold(spark.conf.unset(key))(spark.conf.set(key, _))
  }

  private def withoutLegacyDeserialization[T](action: => T): T = {
    val config = legacyDeserializationConfig
    val previous = spark.conf.getOption(config)
    spark.conf.unset(config)
    try {
      action
    } finally {
      restoreConfig(config, previous)
    }
  }

  test("Complex Param serialization should work on all complex, all normal, or mixed") {
    spark

    val bytes = "foo".toCharArray.map(_.toByte)
    val s = "foo"

    val cpt1 = new ComplexParamTest("foo").setByteArray(bytes)
    cpt1.write.overwrite().save(saveFile)
    val cpt2 = ComplexParamTest.load(saveFile)
    assert(cpt1.getByteArray === cpt2.getByteArray)

    val spt1 = new StandardParamTest("foo").setStringParam(s)
    spt1.write.overwrite().save(saveFile)
    val spt2 = StandardParamTest.load(saveFile)
    assert(spt1.getStringParam === spt2.getStringParam)

    val mpt1 = new MixedParamTest("foo").setByteArray(bytes).setStringParam(s)
    mpt1.write.overwrite().save(saveFile)
    val mpt2 = MixedParamTest.load(saveFile)
    assert(mpt1.getByteArray === mpt2.getByteArray)
    assert(mpt1.getStringParam === mpt2.getStringParam)
  }

  test("Complex Param serialization should yield portable models") {
    spark
    val bytes = "foo".toCharArray.map(_.toByte)
    val s = "foo"

    val mpt1 = new MixedParamTest("foo").setByteArray(bytes).setStringParam(s)
    mpt1.write.overwrite().save(saveFile)

    FileUtils.moveDirectory(new File(saveFile), new File(saveFile2))

    val mpt2 = MixedParamTest.load(saveFile2)
    assert(mpt1.getByteArray === mpt2.getByteArray)
    assert(mpt1.getStringParam === mpt2.getStringParam)
  }

  test("Complex Param serialization should read metadata written by the legacy SparkContext path") {
    spark
    val bytes = "foo".toCharArray.map(_.toByte)

    val mpt1 = new MixedParamTest("foo").setByteArray(bytes).setStringParam("foo")
    mpt1.write.overwrite().save(saveFile)

    // Rewrite the metadata the way SynapseML wrote it before the reader moved off
    // SparkContext.textFile, so this asserts that models saved by earlier versions still
    // load rather than just round-tripping the current writer against the current reader.
    val metadataDir = new File(saveFile, "metadata")
    val metadataJson = spark.read.text(metadataDir.toString).first().getString(0)
    FileUtils.deleteDirectory(metadataDir)
    spark.sparkContext.parallelize(Seq(metadataJson), 1).saveAsTextFile(metadataDir.toString)

    val mpt2 = MixedParamTest.load(saveFile)
    assert(mpt1.getByteArray === mpt2.getByteArray)
    assert(mpt1.getStringParam === mpt2.getStringParam)
  }

  test("Objects written the way earlier versions wrote them still load through the session path") {
    spark
    val obj = "round-trip payload".toCharArray.map(_.toByte)
    val legacyPath = new Path(new File(tmpDir.toFile, "legacy-object").toString)

    // Reproduce the previous write path byte for byte: the FileSystem resolved from the
    // SparkContext Hadoop configuration rather than from the session, writing through the same
    // Serializer.write. Only the configuration lookup moved, so this pins that the on-disk format
    // is unchanged and that objects written by earlier SynapseML versions still load.
    using(legacyPath.getFileSystem(spark.sparkContext.hadoopConfiguration).create(legacyPath, true)) { os =>
      Serializer.write(obj, os)
    }.get

    assert(new ObjectSerializer[Array[Byte]](spark).read(legacyPath) === obj)
    assert(Serializer.readFromHDFS[Array[Byte]](spark, legacyPath) === obj)
  }

  test("Serializer rejects unconstrained objects before deserialization callbacks run") {
    val output = new ByteArrayOutputStream()
    Serializer.write(new DeserializationTripwire, output)
    DeserializationTripwire.Triggered.set(false)

    assertThrows[SecurityException] {
      Serializer.read[DeserializationTripwire](new ByteArrayInputStream(output.toByteArray))
    }
    assert(!DeserializationTripwire.Triggered.get())
  }

  test("Serializer closes malformed filtered streams") {
    val input = new CloseTrackingInputStream(Array[Byte](0, 1, 2, 3))

    assertThrows[StreamCorruptedException] {
      Serializer.read[String](input, DeserializationClassFilter(
        allowedClasses = Set(classOf[String].getName)
      ))
    }
    assert(input.closed)
  }

  test("ComplexParam filters reject crafted payloads before deserialization callbacks run") {
    spark
    new MixedParamTest("filtered").setByteArray(Array[Byte](1, 2, 3)).setStringParam("safe")
      .write.overwrite().save(saveFile)
    val payloadPath = new Path(new File(saveFile, "complexParams/byteArray").toString)
    Serializer.writeToHDFS(spark, new DeserializationTripwire, payloadPath, overwrite = true)
    DeserializationTripwire.Triggered.set(false)

    withoutLegacyDeserialization {
      assertThrows[SecurityException] {
        MixedParamTest.load(saveFile)
      }
      assert(!DeserializationTripwire.Triggered.get())
    }
  }

  test("Unconstrained ComplexParams require explicit trusted legacy opt-in") {
    spark
    val holder = new UnsafePayloadParams
    val payloadPath = new Path(new File(tmpDir.toFile, "unsafe-payload").toString)
    holder.unsafePayload.save(new DeserializationTripwire, spark, payloadPath, overwrite = true)
    val config = legacyDeserializationConfig
    val previous = spark.conf.getOption(config)
    spark.conf.unset(config)
    DeserializationTripwire.Triggered.set(false)

    try {
      val error = intercept[SecurityException] {
        holder.unsafePayload.load(spark, payloadPath)
      }
      assert(error.getMessage.contains(config))
      assert(!DeserializationTripwire.Triggered.get())

      spark.conf.set(config, "true")
      val loaded = holder.unsafePayload.load(spark, payloadPath)
      assert(Option(loaded).nonEmpty)
      assert(DeserializationTripwire.Triggered.get())
    } finally {
      restoreConfig(config, previous)
    }
  }

  override def afterAll(): Unit = {
    new File(saveFile).delete()
    new File(saveFile2).delete()
    super.afterAll()
  }
}
