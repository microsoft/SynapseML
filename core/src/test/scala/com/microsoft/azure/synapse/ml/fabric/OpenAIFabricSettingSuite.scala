// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import org.scalatest.funsuite.AnyFunSuite
import spray.json.{JsNull, JsString}

class OpenAIFabricSettingSuite extends AnyFunSuite {

  private val settings = new OpenAIFabricSetting {}

  test("missing models retain the failure and include conditional subprocessor guidance") {
    val error = intercept[RuntimeException] {
      settings.assertModelStatus("example-deployment", JsString("ModelNotFound"))
    }
    assert(error.getClass == classOf[RuntimeException])
    assert(error.getMessage.startsWith("Default OpenAI model example-deployment not found, " +
      "please check your deployment name."))
    assert(error.getMessage.contains("If the model requires OpenAI as a Microsoft Subprocessor"))
    assert(error.getMessage.contains(
      "Users can use Copilot, AI Agents and other AI experiences powered by OpenAI as a Microsoft Subprocessor"))
    assert(error.getMessage.contains("https://learn.microsoft.com/en-us/fabric/admin/service-admin-portal-copilot"))
    assert(error.getMessage.contains(
      "https://learn.microsoft.com/en-us/fabric/data-science/ai-services/ai-services-overview"))
  }

  test("other statuses retain their existing success and failure gates") {
    settings.assertModelStatus("example-deployment", JsString("Allowed"))
    Seq("Disallowed", "DisallowedForCrossGeo", "InvalidResult", "unknown").foreach { status =>
      val error = intercept[RuntimeException] {
        settings.assertModelStatus("example-deployment", JsString(status))
      }
      assert(!error.getMessage.contains("Subprocessor"))
    }
    intercept[RuntimeException] {
      settings.assertModelStatus("example-deployment", JsNull)
    }
  }
}
