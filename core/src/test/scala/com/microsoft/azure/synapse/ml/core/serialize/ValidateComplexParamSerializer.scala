// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.serialize

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import com.microsoft.azure.synapse.ml.core.serialize.ComplexParamTestFixtures.withActiveSession
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.utils.{
  DeserializationClassFilter,
  DeserializationClassRejectedException
}
import com.microsoft.azure.synapse.ml.param.ByteArrayParam
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.param.{DoubleArrayParam, DoubleParam, FloatParam, Param, ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{
  ComplexParamsReadable,
  ComplexParamsTestHelper,
  ComplexParamsWritable,
  ExplicitComplexParamsWritable,
  ObjectSerializer,
  PipelineStage,
  Serializer
}
import org.apache.spark.sql.SparkSession
import org.json4s.{JNothing, JString}
import org.json4s.jackson.JsonMethods.parse

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  File,
  InvalidClassException,
  IOException,
  ObjectInputStream,
  StreamCorruptedException
}
import java.nio.charset.StandardCharsets
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

@SerialVersionUID(1L)
private class VersionedPayload extends Serializable

private class UnsafePayloadParam(parent: Params, name: String, doc: String)
  extends ComplexParam[DeserializationTripwire](
    parent,
    name,
    doc,
    (_: DeserializationTripwire) => true
  )

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

  def setStringDefault(value: String): this.type = setDefault(stringParam -> value)
}

object StandardParamTest extends ComplexParamsReadable[StandardParamTest]

class TestableImplicitDefaultParamTest(override val uid: String) extends StandardParamTest(uid) {
  def this() = this(Identifiable.randomUID("TestableImplicitDefaultParamTest"))

  setStringDefault("constructor-default")

  override def paramsWithImplicitDefaults: Set[Param[_]] = super.paramsWithImplicitDefaults + stringParam
}

object TestableImplicitDefaultParamTest extends ComplexParamsReadable[TestableImplicitDefaultParamTest]

class TestableFloatingPointParamTest(override val uid: String)
  extends TestEstimatorBase(uid) with ComplexParamsWritable {
  def this() = this(Identifiable.randomUID("TestableFloatingPointParamTest"))

  val doubleValue = new DoubleParam(this, "doubleValue", "double value")
  val floatValue = new FloatParam(this, "floatValue", "float value")
  val doubleValues = new DoubleArrayParam(this, "doubleValues", "double array")
  setDefault(doubleValue -> -0.0d, floatValue -> -0.0f, doubleValues -> Array(-0.0d, 0.0d))
}

object TestableFloatingPointParamTest extends ComplexParamsReadable[TestableFloatingPointParamTest]

class MixedParamTest(override val uid: String) extends TestEstimatorBase(uid)
  with HasStringParam with HasByteArrayParam with ComplexParamsWritable {
  def this() = this(Identifiable.randomUID("MixedParamTest"))
}

object MixedParamTest extends ComplexParamsReadable[MixedParamTest]

class TestableExplicitSessionParamTest(override val uid: String) extends TestableSessionParamTest(uid)
  with ExplicitComplexParamsWritable {
  override protected def validateForWrite(): Unit = ()
}

class TestableDefaultParamTest(override val uid: String) extends MixedParamTest(uid) {
  setDefault(stringParam -> "default", byteArray -> Array[Byte](1, 2))
}

object TestableDefaultParamTest extends ComplexParamsReadable[TestableDefaultParamTest]

class TestableExplicitParamTest(override val uid: String) extends TestableDefaultParamTest(uid) {
  override protected def saveDefaultParamsAsExplicit: Boolean = false
}

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

  Seq(false, true).foreach { explicit =>
    test(s"complex writers use their assigned Spark session with explicit=$explicit") {
      val stage = if (explicit) new TestableExplicitSessionParamTest("explicit-session")
        else new TestableSessionParamTest("legacy-session")
      val session = spark.newSession()
      session.conf.set(TestableSessionParamTest.ConfigKey, stage.uid)
      withActiveSession(Some(spark)) {
        val bytes = Array[Byte](1, 2, 3)
        val path = tmpDir.resolve(s"session-$explicit.model").toString
        stage.set(stage.byteArray, bytes).write.session(session).save(path)
        val restored = TestableSessionParamTest.read.session(session).load(path)
        assert(restored.getOrDefault(restored.byteArray).sameElements(bytes))
      }
    }
  }

  for {
    array <- Seq(false, true)
    hasActiveSession <- Seq(false, true)
  } {
    test(s"nested complex writers use their assigned Spark session with array=$array, active=$hasActiveSession") {
      val child = new TestableSessionParamTest(s"nested-session-$array")
      val assigned = spark.newSession()
      assigned.conf.set(TestableSessionParamTest.ConfigKey, child.uid)
      val previousDefault = SparkSession.getDefaultSession
      val expectedActive = if (hasActiveSession) Some(spark) else None
      withActiveSession(expectedActive) {
        val bytes = Array[Byte](1, 2, 3)
        val stage = new TestableNestedParamTest(s"parent-$array")
          .setNested(child.set(child.byteArray, bytes), array)
        val path = tmpDir.resolve(s"nested-session-$array-$hasActiveSession.model").toString
        stage.write.session(assigned).save(path)
        assert(SparkSession.getActiveSession == expectedActive)
        assert(SparkSession.getDefaultSession == previousDefault)
        val restored = TestableNestedParamTest.read.session(assigned).load(path)
          .getNested(array).asInstanceOf[TestableSessionParamTest]
        assert(restored.getOrDefault(restored.byteArray).sameElements(bytes))
        assert(SparkSession.getActiveSession == expectedActive)
        assert(SparkSession.getDefaultSession == previousDefault)

        val nestedPath = new Path(tmpDir.resolve(s"nested-overwrite-$array-$hasActiveSession.model").toUri)
        def saveNested(overwrite: Boolean): Unit = {
          if (array) stage.stages.save(Array[PipelineStage](child), assigned, nestedPath, overwrite)
          else stage.transformer.save(child, assigned, nestedPath, overwrite)
        }
        Seq(false, true).foreach { overwrite =>
          val payload = if (overwrite) Array[Byte](4, 5, 6) else bytes
          child.set(child.byteArray, payload)
          saveNested(overwrite)
          assert(SparkSession.getActiveSession == expectedActive)
          assert(SparkSession.getDefaultSession == previousDefault)
          val nested = (if (array) stage.stages.load(assigned, nestedPath).head
            else stage.transformer.load(assigned, nestedPath)).asInstanceOf[TestableSessionParamTest]
          assert(nested.getOrDefault(nested.byteArray).sameElements(payload))
          assert(SparkSession.getActiveSession == expectedActive)
          assert(SparkSession.getDefaultSession == previousDefault)
        }

        child.set(child.byteArray, Array[Byte](7, 8, 9))
        val existingPathError = intercept[IOException](saveNested(overwrite = false))
        assert(existingPathError.getMessage.contains("already exists"))
        assert(SparkSession.getActiveSession == expectedActive)
        assert(SparkSession.getDefaultSession == previousDefault)
        val preserved = (if (array) stage.stages.load(assigned, nestedPath).head
          else stage.transformer.load(assigned, nestedPath)).asInstanceOf[TestableSessionParamTest]
        assert(preserved.getOrDefault(preserved.byteArray).sameElements(Array[Byte](4, 5, 6)))
        assert(SparkSession.getActiveSession == expectedActive)
        assert(SparkSession.getDefaultSession == previousDefault)

        assigned.conf.set(TestableSessionParamTest.ConfigKey, "wrong-session")
        val overwriteError = intercept[IllegalArgumentException] {
          saveNested(overwrite = true)
        }
        assert(overwriteError.getMessage.contains("Incorrect writer session"))
        assert(SparkSession.getActiveSession == expectedActive)
        assert(SparkSession.getDefaultSession == previousDefault)
        val error = intercept[IllegalArgumentException] {
          stage.write.session(assigned).save(tmpDir.resolve(s"nested-failure-$array-$hasActiveSession.model").toString)
        }
        assert(error.getMessage.contains("Incorrect writer session"))
        assert(SparkSession.getActiveSession == expectedActive)
        assert(SparkSession.getDefaultSession == previousDefault)
      }
    }
  }

  for {
    array <- Seq(false, true)
    hasActiveSession <- Seq(false, true)
  } {
    test(s"pipeline params with an unassigned session restore sessions with array=$array, active=$hasActiveSession") {
      val child = new TestableSessionParamTest(s"unassigned-session-$array")
      val selected = spark.newSession()
      selected.conf.set(TestableSessionParamTest.ConfigKey, child.uid)
      val previousDefault = SparkSession.getDefaultSession
      val expectedActive = if (hasActiveSession) Some(selected) else None
      val expectedDefault = if (hasActiveSession) previousDefault else Some(selected)
      try {
        expectedDefault.fold(SparkSession.clearDefaultSession())(SparkSession.setDefaultSession)
        withActiveSession(expectedActive) {
          val bytes = Array[Byte](1, 2, 3)
          child.set(child.byteArray, bytes)
          val parent = new TestableNestedParamTest(s"unassigned-parent-$array")
          val path = new Path(tmpDir.resolve(s"unassigned-session-$array-$hasActiveSession.model").toUri)
          val unassigned = Option.empty[SparkSession].orNull
          def saveNested(overwrite: Boolean): Unit = {
            if (array) parent.stages.save(Array[PipelineStage](child), unassigned, path, overwrite)
            else parent.transformer.save(child, unassigned, path, overwrite)
          }
          saveNested(overwrite = false)
          assert(SparkSession.getActiveSession == expectedActive)
          assert(SparkSession.getDefaultSession == expectedDefault)
          val restored = (if (array) parent.stages.load(unassigned, path).head
            else parent.transformer.load(unassigned, path)).asInstanceOf[TestableSessionParamTest]
          assert(restored.getOrDefault(restored.byteArray).sameElements(bytes))
          assert(SparkSession.getActiveSession == expectedActive)
          assert(SparkSession.getDefaultSession == expectedDefault)
          selected.conf.set(TestableSessionParamTest.ConfigKey, "wrong-session")
          val error = intercept[IllegalArgumentException] {
            saveNested(overwrite = true)
          }
          assert(error.getMessage.contains("Incorrect writer session"))
          assert(SparkSession.getActiveSession == expectedActive)
          assert(SparkSession.getDefaultSession == expectedDefault)
        }
      } finally {
        previousDefault.fold(SparkSession.clearDefaultSession())(SparkSession.setDefaultSession)
      }
    }
  }

  Seq("active", "default").foreach { selected =>
    test(s"legacy complex parameter helpers use the $selected Spark session") {
      val active = spark.newSession()
      val default = spark.newSession()
      active.conf.set(TestableSessionParamTest.ConfigKey, "active")
      default.conf.set(TestableSessionParamTest.ConfigKey, "default")
      val previousActive = SparkSession.getActiveSession
      val previousDefault = SparkSession.getDefaultSession
      try {
        SparkSession.setDefaultSession(default)
        if (selected == "active") {
          SparkSession.setActiveSession(active)
        } else {
          SparkSession.clearActiveSession()
        }
        val bytes = Array[Byte](1, 2, 3)
        val stage = new TestableSessionParamTest(selected)
        stage.set(stage.byteArray, bytes)
        val restored = new TestableSessionParamTest(selected)
        val path = tmpDir.resolve(s"legacy-$selected.model").toString
        ComplexParamsTestHelper.saveAndLoad(stage, restored, path, spark)
        assert(restored.getOrDefault(restored.byteArray).sameElements(bytes))
      } finally {
        previousActive.fold(SparkSession.clearActiveSession())(SparkSession.setActiveSession)
        previousDefault.fold(SparkSession.clearDefaultSession())(SparkSession.setDefaultSession)
      }
    }
  }

  test("Persisted ordinary defaults retain legacy explicit values") {
    spark
    Seq(None, Some("fallback"), Some("custom")).foreach { explicit =>
      val original = new StandardParamTest().setStringDefault("fallback")
      explicit.foreach(original.setStringParam)
      original.write.overwrite().save(saveFile)
      val restored = StandardParamTest.load(saveFile)
      assert(restored.getStringParam == explicit.getOrElse("fallback"))
      assert(restored.isSet(restored.stringParam))
      assert(restored.getDefault(restored.stringParam).contains("fallback"))
      restored.clear(restored.stringParam)
      assert(restored.getStringParam == "fallback")
    }
  }

  test("Unset constructor-recreated defaults are omitted from both metadata maps") {
    spark
    val path = tmpDir.resolve("constructor-recreated.model").toString
    val resavePath = tmpDir.resolve("constructor-recreated-resaved.model").toString
    Seq(None, Some("saved-default"), Some("custom")).foreach { explicit =>
      val original = new TestableImplicitDefaultParamTest().setStringDefault("saved-default")
      explicit.foreach(original.setStringParam)
      original.write.overwrite().save(path)
      val metadata = parse(spark.read.text(new File(path, "metadata").toString).first().getString(0))
      assert((metadata \ "paramMap" \ "stringParam") == explicit.map(JString(_)).getOrElse(JNothing))
      assert((metadata \ "defaultParamMap" \ "stringParam") ==
        explicit.map(_ => JString("saved-default")).getOrElse(JNothing))
      val restored = TestableImplicitDefaultParamTest.load(path)
      assert(restored.isSet(restored.stringParam) == explicit.isDefined)
      assert(restored.getStringParam == explicit.getOrElse("constructor-default"))
      assert(restored.getDefault(restored.stringParam).contains(
        if (explicit.isDefined) "saved-default" else "constructor-default"))
      restored.write.overwrite().save(resavePath)
      val resaved = TestableImplicitDefaultParamTest.load(resavePath)
      assert(resaved.isSet(resaved.stringParam) == explicit.isDefined)
      assert(resaved.getStringParam == restored.getStringParam)
    }
  }

  private def assertSignedZeros(stage: TestableFloatingPointParamTest): Unit = {
    val negativeDoubleBits = java.lang.Double.doubleToRawLongBits(-0.0d)
    val negativeFloatBits = java.lang.Float.floatToRawIntBits(-0.0f)
    assert(java.lang.Double.doubleToRawLongBits(stage.getOrDefault(stage.doubleValue)) == negativeDoubleBits)
    assert(java.lang.Float.floatToRawIntBits(stage.getOrDefault(stage.floatValue)) == negativeFloatBits)
    assert(stage.getOrDefault(stage.doubleValues).map(java.lang.Double.doubleToRawLongBits)
      .sameElements(Array(negativeDoubleBits, 0L)))
    assert(java.lang.Double.doubleToRawLongBits(stage.getDefault(stage.doubleValue).get) == negativeDoubleBits)
    assert(java.lang.Float.floatToRawIntBits(stage.getDefault(stage.floatValue).get) == negativeFloatBits)
    assert(stage.getDefault(stage.doubleValues).get.map(java.lang.Double.doubleToRawLongBits)
      .sameElements(Array(negativeDoubleBits, 0L)))
  }

  test("Floating-point parameters retain signed-zero through save load and resave") {
    spark
    Seq(false, true).foreach { explicit =>
      val stage = new TestableFloatingPointParamTest()
      if (explicit) {
        stage.set(stage.doubleValue, -0.0d).set(stage.floatValue, -0.0f)
          .set(stage.doubleValues, Array(-0.0d, 0.0d))
      }
      val path = tmpDir.resolve(s"signed-zero-$explicit.model").toString
      stage.write.save(path)
      val restored = TestableFloatingPointParamTest.load(path)
      assertSignedZeros(restored)
      val resavePath = tmpDir.resolve(s"signed-zero-resaved-$explicit.model").toString
      restored.write.save(resavePath)
      assertSignedZeros(TestableFloatingPointParamTest.load(resavePath))
    }
  }

  test("Previously saved signed-zero metadata retains numeric parameter signs") {
    val path = tmpDir.resolve("legacy-signed-zero.model").toString
    val values = """{"doubleValue":-0.0,"floatValue":-0.0,"doubleValues":[-0.0,0.0]}"""
    val metadata = s"""{"class":"${classOf[TestableFloatingPointParamTest].getName}","uid":"legacy-signed-zero",""" +
      s""""timestamp":0,"sparkVersion":"${spark.version}","paramMap":$values,"defaultParamMap":$values}"""
    spark.createDataFrame(Seq(Tuple1(metadata))).toDF("value").write.text(new File(path, "metadata").toString)
    assertSignedZeros(TestableFloatingPointParamTest.load(path))
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

  test("explicit parameter persistence preserves defaults and complex values without changing legacy writers") {
    for (source <- Seq(new TestableDefaultParamTest("legacy"), new TestableExplicitParamTest("explicit"))) {
      source.write.session(spark).overwrite().save(saveFile)
      val loaded = TestableDefaultParamTest.load(saveFile)
      assert(loaded.getStringParam == source.getStringParam)
      assert(loaded.getByteArray.sameElements(source.getByteArray))
      assert(loaded.getDefault(loaded.stringParam) == source.getDefault(source.stringParam))
      assert(loaded.isSet(loaded.stringParam) == !source.isInstanceOf[TestableExplicitParamTest])
      source.setStringParam(source.getStringParam)
      source.write.session(spark).overwrite().save(saveFile)
      val selected = TestableDefaultParamTest.load(saveFile)
      assert(selected.isSet(selected.stringParam))
      selected.clear(selected.stringParam)
      assert(selected.getStringParam == source.getStringParam)
    }
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

  test("Serializer preserves ordinary Java compatibility failures") {
    spark
    val output = new ByteArrayOutputStream()
    Serializer.write(new VersionedPayload, output)
    val serialized = output.toByteArray
    val className = classOf[VersionedPayload].getName
    val classNameBytes = className.getBytes(StandardCharsets.UTF_8)
    val classNameIndex = serialized.indexOfSlice(classNameBytes)
    assert(classNameIndex >= 0)
    serialized(classNameIndex + classNameBytes.length + java.lang.Long.BYTES - 1) = 2

    val path = new Path(new File(tmpDir.toFile, "incompatible-object").toString)
    using(path.getFileSystem(spark.sparkContext.hadoopConfiguration).create(path, true)) { stream =>
      stream.write(serialized)
    }.get

    withoutLegacyDeserialization {
      val error = intercept[InvalidClassException] {
        Serializer.readFromHDFS[VersionedPayload](
          spark,
          path,
          DeserializationClassFilter(allowedClasses = Set(className))
        )
      }
      assert(error.classname === className)
      assert(!error.isInstanceOf[DeserializationClassRejectedException])
    }
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
