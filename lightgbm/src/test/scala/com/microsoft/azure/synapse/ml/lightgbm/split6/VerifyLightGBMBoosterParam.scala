// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split6

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm.booster.LightGBMBooster
import com.microsoft.azure.synapse.ml.lightgbm.params.LightGBMBoosterParam
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.Serializer
import org.apache.spark.ml.param.{ParamMap, Params}

import java.io.{File, ObjectInputStream}
import java.util.concurrent.atomic.AtomicBoolean

private object BoosterDeserializationTripwire {
  val Triggered = new AtomicBoolean(false)
}

@SerialVersionUID(1L)
private class BoosterDeserializationTripwire extends Serializable {
  private def readObject(input: ObjectInputStream): Unit = {
    BoosterDeserializationTripwire.Triggered.set(true)
    input.defaultReadObject()
  }
}

class VerifyLightGBMBoosterParam extends TestBase {

  private val legacyDeserializationConfig =
    Serializer.LegacyObjectDeserializationConfig

  private class TestParamsHolder extends Params {
    override val uid: String = "lightgbm-booster-holder"
    val booster = new LightGBMBoosterParam(this, "booster", "A LightGBM booster param")

    override def copy(extra: ParamMap): Params = this
  }

  test("LightGBMBoosterParam loads its constrained legacy object graph") {
    val holder = new TestParamsHolder
    val path = new Path(new File(tmpDir.toFile, "booster").toString)
    val expected = new LightGBMBooster("model-data")

    holder.booster.save(expected, spark, path, overwrite = true)
    val loaded = holder.booster.load(spark, path)

    assert(loaded.getNativeModel() === expected.getNativeModel())
  }

  test("LightGBMBoosterParam rejects classes outside its policy before callbacks run") {
    val holder = new TestParamsHolder
    val path = new Path(new File(tmpDir.toFile, "crafted-booster").toString)
    Serializer.writeToHDFS(
      spark,
      new BoosterDeserializationTripwire,
      path,
      overwrite = true
    )
    BoosterDeserializationTripwire.Triggered.set(false)
    val previous = spark.conf.getOption(legacyDeserializationConfig)
    spark.conf.unset(legacyDeserializationConfig)

    try {
      assertThrows[SecurityException] {
        holder.booster.load(spark, path)
      }
      assert(!BoosterDeserializationTripwire.Triggered.get())
    } finally {
      previous.fold(spark.conf.unset(legacyDeserializationConfig))(
        spark.conf.set(legacyDeserializationConfig, _)
      )
    }
  }
}
