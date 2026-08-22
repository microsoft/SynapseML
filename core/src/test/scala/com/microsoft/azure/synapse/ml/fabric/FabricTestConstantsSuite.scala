// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import org.scalatest.funsuite.AnyFunSuite
import spray.json._

class FabricTestConstantsSuite extends AnyFunSuite {

  test("Use the Fabric API scope for workspace discovery") {
    val config = FabricTestConstants.workspaceAccessTokenConfiguration(
      tenant = "example.onmicrosoft.com",
      username = "integration-user")

    assert(config.ClientId == FabricTestConstants.INTEGRATION_APP_ID)
    assert(config.RedirectUri == FabricTestConstants.INTEGRATION_REDIRECT_URI)
    assert(config.Resource == "https://api.fabric.microsoft.com/.default")
    assert(config.Tenant == "example.onmicrosoft.com")
    assert(config.Username == "integration-user")
  }

  test("Resolve the integration workspace ID by display name") {
    val response =
      """
        |{
        |  "value": [
        |    {"id": "other-id", "displayName": "Other Workspace"},
        |    {"id": "expected-id", "displayName": "Integration Workspace"}
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
        |    {"id": "other-id", "displayName": "Other Workspace"}
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
