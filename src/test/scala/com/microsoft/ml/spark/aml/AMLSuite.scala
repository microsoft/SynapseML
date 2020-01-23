// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark.aml

import com.microsoft.ml.spark.cognitive.{SpeechKey, SpeechToTextSDK, TestApp}
import com.microsoft.ml.spark.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.util.MLReadable

class AMLSuite extends TransformerFuzzing[SpeechToTextSDK]
  with SpeechKey {

  override def testSerialization(): Unit = {
    tryWithRetries(Array(0, 100, 100, 100, 100))(super.testSerialization)
  }

  val app = new TestApp

  test("getToken") {
    val auth = app.getAuth
    val token = auth.getAccessToken
    println(s"Token: $token")
    assert(token.length > 0, "Access token not found")
  }

  test("get existing experiment") {
    app.getOrCreateExperiment("tensor_experiment")
  }

  /*
    TODO: Comment back in when support for removing experiments
    through API is supported (otherwise created a new experiment
    whenever tests are run)
   */
  /*
  test("create new experiment") {
    def randomString(length: Int): String = {
      val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
      val sb = new StringBuilder
      for (i <- 1 to length) {
        val randomNum = util.Random.nextInt(chars.length)
        sb.append(chars(randomNum))
      }
      sb.toString
    }

    val randomExperimentName = s"fake_experiment_${randomString(6)}"
    println(s"Random Name: $randomExperimentName")
    val experiment = app.getOrCreateExperiment(randomExperimentName)
  }
  */

  test("launch run") {
    val experimentName = "new_experiment"
    val runFilePath = System.getProperty("user.dir") + "/src/test/resources/testRun"
    app.getOrCreateExperiment(experimentName)
    app.launchRun(experimentName, runFilePath)
  }

  test("get models") {
    app.showModels
  }

  override def testObjects(): Seq[TestObject[SpeechToTextSDK]] =
    Seq()

  override def reader: MLReadable[_] = SpeechToTextSDK
}
