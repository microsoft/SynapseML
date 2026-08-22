// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import org.scalatest.funsuite.AnyFunSuite

class FabricAuthenticationSuite extends AnyFunSuite {
  private val workspaceId = "01234567-89ab-cdef-0123-456789abcdef"

  test("Default to credential authentication") {
    assert(
      FabricTokenProvider.resolveAuthenticationMode(None) ==
        FabricTokenProvider.CredentialAuthMode)
  }

  test("Normalize Azure CLI authentication mode") {
    assert(
      FabricTokenProvider.resolveAuthenticationMode(Some(" Azure-CLI ")) ==
        FabricTokenProvider.AzureCliAuthMode)
  }

  test("Reject an unsupported authentication mode") {
    val error = intercept[IllegalArgumentException] {
      FabricTokenProvider.resolveAuthenticationMode(Some("token"))
    }
    assert(error.getMessage.contains("INTEGRATION_AUTH_MODE"))
  }

  test("Build an Azure CLI command without credentials") {
    val command =
      FabricTokenProvider.azureCliTokenCommand(FabricTokenProvider.DefaultPowerBiScope)

    assert(command.head == "az" || command.head == "az.cmd")
    assert(command.contains("get-access-token"))
    assert(command.sliding(2).exists(_.toSeq == Seq(
      "--scope", FabricTokenProvider.DefaultPowerBiScope)))
    assert(command.sliding(2).exists(_.toSeq == Seq("--query", "accessToken")))
  }

  test("Use an explicit workspace ID without discovery") {
    var discoveryAttempted = false
    val resolved = FabricTestConstants.resolveIntegrationWorkspaceId(
      Some(s" $workspaceId "), {
        discoveryAttempted = true
        "fedcba98-7654-3210-fedc-ba9876543210"
      })

    assert(resolved == workspaceId)
    assert(!discoveryAttempted)
  }

  test("Discover a workspace ID when no explicit ID is configured") {
    assert(
      FabricTestConstants.resolveIntegrationWorkspaceId(None, workspaceId) ==
        workspaceId)
  }

  test("Reject an empty explicit workspace ID") {
    val error = intercept[IllegalArgumentException] {
      FabricTestConstants.resolveIntegrationWorkspaceId(Some("  "), workspaceId)
    }
    assert(error.getMessage.contains("INTEGRATION_WORKSPACE_ID"))
  }

  test("Require an explicit workspace ID for Azure CLI authentication") {
    var discoveryAttempted = false
    val error = intercept[IllegalArgumentException] {
      FabricTestConstants.resolveIntegrationWorkspaceId(
        None, {
          discoveryAttempted = true
          workspaceId
        },
        requireExplicitWorkspaceId = true)
    }

    assert(error.getMessage.contains("INTEGRATION_AUTH_MODE"))
    assert(!discoveryAttempted)
  }

  test("Reject a malformed workspace ID") {
    val error = intercept[IllegalArgumentException] {
      FabricTestConstants.resolveIntegrationWorkspaceId(Some("workspace-name"), workspaceId)
    }
    assert(error.getMessage.contains("not a GUID"))
  }
}
