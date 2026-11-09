// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.serialize

import com.microsoft.azure.synapse.ml.param.{ByteArrayParam, TransformerParam}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{ComplexParamsReadable, ComplexParamsWritable, PipelineStage, Transformer}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ComplexParamTestFixtures {
  def withActiveSession[T](active: Option[SparkSession])(action: => T): T = {
    val previous = SparkSession.getActiveSession
    active.fold(SparkSession.clearActiveSession())(SparkSession.setActiveSession)
    try action
    finally previous.fold(SparkSession.clearActiveSession())(SparkSession.setActiveSession)
  }
}

class TestEstimatorBase(val uid: String) extends Transformer {
  def this() = this(Identifiable.randomUID("TestEstimatorBase"))

  def transform(dataset: Dataset[_]): DataFrame = dataset.toDF()

  def copy(extra: ParamMap): this.type = defaultCopy(extra)

  def transformSchema(schema: StructType): StructType = schema
}

class TestableSessionParamTest(override val uid: String) extends TestEstimatorBase(uid) with ComplexParamsWritable {
  def this() = this(Identifiable.randomUID("TestableSessionParamTest"))

  val byteArray = new ByteArrayParam(this, "byteArray", "session-dependent test payload") {
    override def save(value: Array[Byte], session: SparkSession, path: Path, overwrite: Boolean): Unit = {
      require(session.conf.getOption(TestableSessionParamTest.ConfigKey).contains(uid), "Incorrect writer session")
      super.save(value, session, path, overwrite)
    }

    override def load(session: SparkSession, path: Path): Array[Byte] = {
      require(session.conf.getOption(TestableSessionParamTest.ConfigKey).contains(uid), "Incorrect reader session")
      super.load(session, path)
    }
  }
}

object TestableSessionParamTest extends ComplexParamsReadable[TestableSessionParamTest] {
  val ConfigKey: String = "synapseml.test.persistence.session"
}

class TestableNestedParamTest(override val uid: String) extends TestEstimatorBase(uid) with ComplexParamsWritable {
  def this() = this(Identifiable.randomUID("TestableNestedParamTest"))

  val transformer = new TransformerParam(this, "transformer", "nested transformer")
  val stages = new ComplexParam[Array[PipelineStage]](
    this, "stages", "nested pipeline stages", (_: Array[PipelineStage]) => true) {}

  def setNested(value: Transformer, array: Boolean): this.type =
    if (array) set(stages, Array[PipelineStage](value)) else set(transformer, value)

  def getNested(array: Boolean): PipelineStage = if (array) $(stages).head else $(transformer)
}

object TestableNestedParamTest extends ComplexParamsReadable[TestableNestedParamTest]
