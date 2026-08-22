// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.fabric

import spray.json._

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.sys.process.Process
import scala.util.control.NonFatal

private[ml] object FabricAzureCliTestConfiguration {
  private[fabric] val AzureCliAuthMode: String = "azure-cli"
  private[fabric] val CredentialAuthMode: String = "credential"
  private[fabric] val DefaultPowerBiScope: String =
    "https://analysis.windows.net/powerbi/api/.default"
  private[fabric] val DefaultFabricScope: String =
    "https://api.fabric.microsoft.com/.default"
  private val WorkspaceIdPattern =
    "(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$".r
  private var AzureCliTokenMap: Map[String, String] = Map.empty

  private[ml] def authenticationMode: String = {
    resolveAuthenticationMode(sys.env.get("INTEGRATION_AUTH_MODE"))
  }

  private[ml] def integrationWorkspaceId(discoveredWorkspaceId: => String): String = {
    integrationWorkspaceId(sys.env, discoveredWorkspaceId)
  }

  private[fabric] def integrationWorkspaceId(
      environment: Map[String, String],
      discoveredWorkspaceId: => String): String = {
    resolveIntegrationWorkspaceId(
      environment.get("INTEGRATION_WORKSPACE_ID"),
      discoveredWorkspaceId,
      resolveAuthenticationMode(environment.get("INTEGRATION_AUTH_MODE")) == AzureCliAuthMode)
  }

  private[fabric] def getAccessToken(clientId: String,
                                     redirectUri: String,
                                     scope: String): String = {
    authenticationMode match {
      case AzureCliAuthMode => getAzureCliAccessToken(scope)
      case CredentialAuthMode =>
        require(
          scope == DefaultPowerBiScope,
          s"Credential authentication only supports the Power BI scope, not '$scope'")
        FabricTokenProvider.getAccessToken(clientId, redirectUri)
    }
  }

  private[fabric] def resolveAuthenticationMode(configuredMode: Option[String]): String = {
    configuredMode match {
      case None => CredentialAuthMode
      case Some(mode) =>
        mode.trim.toLowerCase match {
          case AzureCliAuthMode => AzureCliAuthMode
          case CredentialAuthMode => CredentialAuthMode
          case _ =>
            throw new IllegalArgumentException(
              s"INTEGRATION_AUTH_MODE must be '$CredentialAuthMode' or '$AzureCliAuthMode'")
        }
    }
  }

  private[fabric] def resolveIntegrationWorkspaceId(
      explicitWorkspaceId: Option[String],
      discoveredWorkspaceId: => String,
      requireExplicitWorkspaceId: Boolean = false): String = {
    val workspaceId = explicitWorkspaceId match {
      case Some(value) if value.trim.nonEmpty => value.trim
      case Some(_) =>
        throw new IllegalArgumentException(
          "INTEGRATION_WORKSPACE_ID must not be empty when it is set")
      case None if requireExplicitWorkspaceId =>
        throw new IllegalArgumentException(
          "INTEGRATION_WORKSPACE_ID must be set when INTEGRATION_AUTH_MODE is 'azure-cli'")
      case None => discoveredWorkspaceId
    }

    workspaceId match {
      case WorkspaceIdPattern() => workspaceId
      case _ =>
        throw new IllegalArgumentException(
          s"Fabric integration workspace ID is not a GUID: '$workspaceId'")
    }
  }

  private[fabric] def azureCliTokenCommand(scope: String): Seq[String] = {
    val executable = if (sys.props("os.name").toLowerCase.contains("windows")) "az.cmd" else "az"
    Seq(
      executable,
      "account",
      "get-access-token",
      "--scope",
      scope,
      "--query",
      "accessToken",
      "--output",
      "tsv",
      "--only-show-errors"
    )
  }

  private def getAzureCliAccessToken(scope: String): String = synchronized {
    AzureCliTokenMap.get(scope).filterNot(JwtUtils.isTokenExpired) match {
      case Some(token) => token
      case None =>
        val token = fetchTokenByAzureCli(scope)
        AzureCliTokenMap += scope -> token
        token
    }
  }

  private def fetchTokenByAzureCli(scope: String): String = {
    println(s"Fetching token from the active Azure CLI session for scope: $scope")
    try {
      val token = Process(azureCliTokenCommand(scope)).!!.trim
      if (token.isEmpty) {
        throw new IllegalStateException("Azure CLI returned an empty access token")
      }
      token
    } catch {
      case NonFatal(error) =>
        throw new RuntimeException(
          "Could not acquire a Fabric test token from the active Azure CLI session", error)
    }
  }

  private object JwtUtils {
    private case class Token(exp: Long)
    private object JsonProtocol extends DefaultJsonProtocol {
      implicit val TokenFormat: RootJsonFormat[Token] = jsonFormat1(Token)
    }

    def isTokenExpired(token: String): Boolean = {
      import JsonProtocol._
      try {
        val parts = token.split("\\.")
        require(parts.length == 3, "Invalid JWT token format")
        val payload = new String(
          Base64.getUrlDecoder.decode(parts(1)),
          StandardCharsets.UTF_8)
        val expiration = payload.parseJson.convertTo[Token].exp
        expiration < System.currentTimeMillis() / 1000 + 300
      } catch {
        case NonFatal(error) =>
          println(s"Failed to process Azure CLI token: ${error.getMessage}")
          true
      }
    }
  }
}
