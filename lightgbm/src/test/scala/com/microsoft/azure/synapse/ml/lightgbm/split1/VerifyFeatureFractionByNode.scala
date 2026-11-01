// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.lightgbm._
import com.microsoft.azure.synapse.ml.lightgbm.params.LightGBMParams
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.DataFrame

class VerifyFeatureFractionByNode extends TestBase {
  private val fraction = 0.4
  private val overrideFraction = 0.7
  private val nativeKey = "feature_fraction_bynode"
  private val aliases = Seq("colsample_bynode", "sub_feature_bynode")
  private val names = Seq("classifier", "regressor", "ranker")
  private val modes = Seq("bulk", "streaming")
  private var port = 29000

  private def learner(name: String): LightGBMParams = {
    port += 1
    val stage: LightGBMParams = name match {
      case "classifier" => new LightGBMClassifier()
      case "regressor" => new LightGBMRegressor()
      case "ranker" => new LightGBMRanker().setGroupCol("group")
    }
    stage.setNumIterations(3).setNumLeaves(3).setMinDataInLeaf(1).setMinDataPerBin(1)
      .setNumTasks(1).setNumThreads(1).setDefaultListenPort(port).setSeed(42).setFeatureFractionSeed(42)
  }

  private def config(stage: LightGBMParams): String = {
    val schema = new AttributeGroup("features", 4).toStructField()
    stage match {
      case classifier: LightGBMClassifier => classifier.getTrainParams(1, schema, 1).toString
      case regressor: LightGBMRegressor => regressor.getTrainParams(1, schema, 1).toString
      case ranker: LightGBMRanker => ranker.getTrainParams(1, schema, 1).toString
    }
  }

  private def values(config: String, key: String = nativeKey): Seq[String] = {
    config.split("\\s+").filter(_.startsWith(key + "=")).map(_.split("=", 2)(1)).toSeq
  }

  private def restore(name: String, stage: LightGBMParams, suffix: String): LightGBMParams = {
    val path = tmpDir.resolve(s"$name-$suffix").toString
    stage.write.session(spark).save(path)
    name match {
      case "classifier" => LightGBMClassifier.read.session(spark).load(path)
      case "regressor" => LightGBMRegressor.read.session(spark).load(path)
      case "ranker" => LightGBMRanker.read.session(spark).load(path)
    }
  }

  names.foreach { name =>
    test(s"$name defaults and clears featureFractionByNode without emitting it") {
      val stage = learner(name)
      assert(stage.getFeatureFractionByNode == 1.0)
      assert(!stage.isSet(stage.featureFractionByNode))
      assert(values(config(stage)).isEmpty)
      val copied = stage.copy(ParamMap.empty).asInstanceOf[LightGBMParams]
      assert(copied.getFeatureFractionByNode == 1.0)
      assert(!copied.isSet(copied.featureFractionByNode))
      assert(values(config(copied)).isEmpty)
      val loaded = restore(name, stage, "unset")
      assert(loaded.getFeatureFractionByNode == loaded.getFeatureFraction)
      assert(loaded.getFeatureFractionByNode == 1.0)
      assert(loaded.isSet(loaded.featureFractionByNode))
      assert(loaded.isSet(loaded.featureFraction))
      assert(values(config(loaded)) == Seq("1.0"))
      assert(values(config(loaded), "feature_fraction") == Seq("1.0"))
      Seq(stage, loaded).foreach { value =>
        value.setFeatureFractionByNode(fraction)
        value.clear(value.featureFractionByNode)
        assert(!value.isSet(value.featureFractionByNode))
        assert(value.getFeatureFractionByNode == 1.0)
        assert(values(config(value)).isEmpty)
      }
    }

    test(s"$name emits explicit featureFractionByNode and preserves canonical overrides") {
      val stage = learner(name).setFeatureFractionByNode(fraction)
      assert(values(config(stage)) == Seq(fraction.toString), "EXPLICIT_NODE_FRACTION_MISSING")
      Seq(stage.copy(ParamMap.empty).asInstanceOf[LightGBMParams], restore(name, stage, "set"))
        .foreach { value =>
          assert(value.getFeatureFractionByNode == fraction)
          assert(value.isSet(value.featureFractionByNode))
          assert(values(config(value)) == Seq(fraction.toString))
        }
      stage.setPassThroughArgs(s"$nativeKey=$overrideFraction")
      assert(values(config(stage)) == Seq(overrideFraction.toString))
    }

    test(s"$name preserves alias-only passThroughArgs without injecting a canonical default") {
      aliases.foreach { alias =>
        val stage = learner(name).setPassThroughArgs(s"$alias=$overrideFraction")
        assert(config(stage).contains(s"$alias=$overrideFraction"))
        assert(values(config(stage)).isEmpty)
      }
    }

    test(s"$name recognizes complete native alias names when overriding typed node sampling") {
      aliases.foreach { alias =>
        val stage = learner(name).setFeatureFractionByNode(fraction)
        Seq(s"$alias=$overrideFraction", s"\t'$alias'='$overrideFraction'\n").foreach { args =>
          stage.setPassThroughArgs(args)
          assert(values(config(stage)).isEmpty)
        }
        stage.setPassThroughArgs(s"not_$alias=$overrideFraction")
        assert(values(config(stage)) == Seq(fraction.toString))
      }
    }
  }

  private lazy val trainingData: DataFrame = {
    val rows = (0 until 48).map { index =>
      val label = (index % 2).toDouble
      (label, index / 6, Vectors.dense(label, (index % 3).toDouble, (index % 5).toDouble, index.toDouble))
    }
    spark.createDataFrame(rows).toDF("label", "group", "features").coalesce(1)
  }

  private def fit(stage: LightGBMParams): Transformer with LightGBMModelMethods = {
    val model = stage match {
      case classifier: LightGBMClassifier => classifier.fit(trainingData)
      case regressor: LightGBMRegressor => regressor.fit(trainingData)
      case ranker: LightGBMRanker => ranker.fit(trainingData)
    }
    model.asInstanceOf[Transformer with LightGBMModelMethods]
  }

  private def effectiveFraction(model: LightGBMModelMethods, key: String = nativeKey): Double = {
    val pattern = ("(?m)^\\[" + key + ": ([^\\]]+)\\]$").r
    val matched = pattern.findFirstMatchIn(model.getNativeModel())
    assert(matched.isDefined, s"Native model must record the effective $key")
    matched.get.group(1).toDouble
  }

  for (name <- names; mode <- modes) {
    aliases.foreach { alias =>
      test(s"$name $mode preserves node sampling across estimator persistence for $alias") {
        val stage = learner(name).setDataTransferMode(mode)
          .setPassThroughArgs(s"$alias=$overrideFraction")
        assert(values(config(stage)).isEmpty)
        val model = fit(stage)
        try {
          assert(math.abs(effectiveFraction(model) - overrideFraction) < 0.000001)
          val expected = model.transform(trainingData).collect().toSeq
          val loaded = restore(name, stage, s"loaded-$mode-$alias")
          assert(loaded.isSet(loaded.featureFractionByNode))
          val restoredModel = fit(loaded)
          try {
            assert(math.abs(effectiveFraction(restoredModel) - overrideFraction) < 0.000001)
            assert(values(config(loaded)).isEmpty)
            assert(restoredModel.transform(trainingData).collect().toSeq == expected)
          } finally {
            restoredModel.getLightGBMBooster.freeNativeMemory()
          }
        } finally {
          model.getLightGBMBooster.freeNativeMemory()
        }
      }
    }
    val cases = Seq("typed", "canonical", "canonical-with-aliases") ++ aliases ++ aliases.map(alias => s"typed-$alias")
    cases.foreach { setting =>
      test(s"$name $mode trains and persists native featureFractionByNode via $setting") {
        val stage = learner(name).setDataTransferMode(mode)
        val expected = if (setting == "typed") fraction else overrideFraction
        if (setting == "typed") {
          stage.setFeatureFractionByNode(fraction)
        } else if (setting == "canonical") {
          stage.setFeatureFractionByNode(fraction).setPassThroughArgs(s"$nativeKey=$overrideFraction")
        } else if (setting == "canonical-with-aliases") {
          stage.setFeatureFractionByNode(fraction)
            .setPassThroughArgs(s"${aliases.head}=$fraction $nativeKey=$overrideFraction ${aliases.last}=$fraction")
        } else if (setting.startsWith("typed-")) {
          val alias = setting.stripPrefix("typed-")
          stage.setFeatureFractionByNode(fraction).setPassThroughArgs(s"$alias=$overrideFraction")
          assert(values(config(stage)).isEmpty)
          assert(config(stage).contains(s"$alias=$overrideFraction"))
        } else {
          stage.setPassThroughArgs(s"$setting=$overrideFraction")
        }
        val model = fit(stage)
        try {
          val observed = effectiveFraction(model)
          assert(!observed.isNaN && observed > 0 && observed <= 1)
          assert(math.abs(observed - expected) < 0.000001)
          assert(model.transform(trainingData).count() == trainingData.count())
          val path = tmpDir.resolve(s"model-$name-$mode-$setting").toString
          model.asInstanceOf[MLWritable].write.save(path)
          val loaded = name match {
            case "classifier" => LightGBMClassificationModel.load(path)
            case "regressor" => LightGBMRegressionModel.load(path)
            case "ranker" => LightGBMRankerModel.load(path)
          }
          try {
            assert(math.abs(effectiveFraction(loaded) - observed) < 0.000001)
            assert(loaded.transform(trainingData).collect().toSeq == model.transform(trainingData).collect().toSeq)
          } finally {
            loaded.getLightGBMBooster.freeNativeMemory()
          }
        } finally {
          model.getLightGBMBooster.freeNativeMemory()
        }
      }
    }
  }
}
