// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import org.scalatest.funsuite.AnyFunSuite
import spray.json._

class FabricTestConstantsSuite extends AnyFunSuite {

  test("Use the Power BI workspace endpoint for workspace discovery") {
    assert(
      FabricTestConstants.PowerBiGroupsEndpoint ==
        "https://api.powerbi.com/v1.0/myorg/groups")
  }

  test("Resolve the integration workspace ID by name") {
    val response =
      """
        |{
        |  "value": [
        |    {"id": "other-id", "name": "Other Workspace"},
        |    {"id": "expected-id", "name": "Integration Workspace"}
        |  ]
        |}
        |""".stripMargin.parseJson

    assert(
      FabricTestConstants.workspaceIdFromResponse(response, "Integration Workspace") ==
        "expected-id")
  }

  test("Reject a workspace response without the integration workspace") {
    val response =
      """
        |{
        |  "value": [
        |    {"id": "other-id", "name": "Other Workspace"}
        |  ]
        |}
        |""".stripMargin.parseJson

    val error = intercept[IllegalArgumentException] {
      FabricTestConstants.workspaceIdFromResponse(response, "Integration Workspace")
    }

    assert(error.getMessage.contains("Integration Workspace"))
    assert(error.getMessage.contains("Other Workspace"))
  }
}
