// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.ml.spark

import com.microsoft.azure.AzureEnvironment
import com.microsoft.azure.credentials.{ApplicationTokenCredentials, AzureCliCredentials}
import com.microsoft.azure.management.Azure
import com.microsoft.azure.management.Azure.Authenticated
import com.microsoft.azure.management.network._
import com.microsoft.azure.management.resources.ResourceGroup
import com.microsoft.azure.management.resources.fluentcore.arm.collection.SupportsListingByResourceGroup
import com.microsoft.azure.management.resources.fluentcore.arm.models.HasName

import scala.collection.JavaConverters._

class AzureLoadBalancer(
                         auth: Authenticated,
                         subscriptionName: String,
                         rgName: String
                       ) {

  private val azure: Azure = {
    val subscription = auth.subscriptions().list().iterator().asScala
      .filter(_.displayName() == subscriptionName).next()
    auth.withSubscription(subscription.subscriptionId())
  }

  private implicit val rg: ResourceGroup = azure.resourceGroups().getByName(rgName)

  private type RP[T <: HasName] = SupportsListingByResourceGroup[T]

  private def getOrCreateResource[T <: HasName](resourceCategory: RP[T],
                                                name: String)
                                               (create: () => T)(implicit rg: ResourceGroup): T = {
    val matching = resourceCategory.listByResourceGroup(rg.name())
      .listIterator().asScala.filter(_.name() == name).toList
    if (matching.length == 1) {
      println(s"Found resource $name")
      matching.head
    }
    else {
      create()
    }
  }

  private def getResource[T <: HasName](resourceCategory: RP[T],
                                        name: String)
                                       (implicit rg: ResourceGroup): Option[T] = {
    resourceCategory.listByResourceGroup(rg.name())
      .listIterator().asScala.filter(_.name() == name).toList.headOption
  }

  class NoResourceError(message: String) extends Exception(message)

  def createIfNotExists(vnName: String,
                        dnsName: String,
                        backendPort: Int,
                        frontendPort: Int,
                        resourceSuffix: String,
                        lbNameOption: Option[String] = None): Unit = {

    val ipName = s"IP-$resourceSuffix"
    val probeName = s"probe1-$resourceSuffix"
    val backendName = s"backendPool1-$resourceSuffix"
    val lbName = lbNameOption.getOrElse(s"LB-$resourceSuffix")
    val ruleName = s"rule-$resourceSuffix"

    try {
      val lb: LoadBalancer = getResource(azure.loadBalancers(), lbName).getOrElse(
        throw new NoResourceError("No Load Balancer Found"))
      assert(lb.backends().containsKey(backendName))

      val rule = lb.loadBalancingRules().asScala(ruleName)
      assert(rule.frontendPort() == frontendPort)
      assert(rule.backendPort() == backendPort)

      assert(getResource(azure.publicIPAddresses(), ipName).isDefined)
      println(s"Using Existing Load Balancer: $lbName")
    } catch {
      case _: NoResourceError =>
        val ip: PublicIPAddress = getOrCreateResource(azure.publicIPAddresses, ipName) { () =>
          azure.publicIPAddresses()
            .define(ipName)
            .withRegion(rg.region())
            .withExistingResourceGroup(rg)
            .withLeafDomainLabel(dnsName)
            .create()
        }

        val vn: Network = azure.networks().listByResourceGroup(rg.name()).listIterator().asScala
          .filter(_.name() == vnName).next()

        val nics: List[NetworkInterface] = azure.networkInterfaces()
          .listByResourceGroup(rg.name())
          .listIterator().asScala
          .filter { nic =>
            nic.primaryIPConfiguration().getNetwork.name() == vn.name() && nic.name().startsWith("nic-workernode")
          }.toList
        //TODO ensure there are no other workernode nics

        val lb = getOrCreateResource(azure.loadBalancers(), lbName) { () =>
          azure.loadBalancers()
            .define(lbName)
            .withRegion(rg.region())
            .withExistingResourceGroup(rg)
            .defineLoadBalancingRule(ruleName)
            .withProtocol(TransportProtocol.TCP)
            .fromExistingPublicIPAddress(ip)
            .fromFrontendPort(frontendPort)
            .toBackend(backendName)
            .toBackendPort(backendPort)
            .withProbe(probeName)
            .attach()
            .defineHttpProbe(probeName)
            .withRequestPath("/")
            .withPort(backendPort)
            .attach()
            .create()
        }

        if (!lb.backends().containsKey(backendName)) {
          lb.update()
            .defineBackend(backendName)
            .attach()
            .defineLoadBalancingRule(ruleName)
            .withProtocol(TransportProtocol.TCP)
            .fromExistingPublicIPAddress(ip)
            .fromFrontendPort(frontendPort)
            .toBackend(backendName)
            .toBackendPort(backendPort)
            .withProbe(probeName)
            .attach()
            .apply()
        }
        linkNics(nics, lb, backendName)
    }
  }

  private def linkNics(nics: List[NetworkInterface], lb: LoadBalancer, backendName: String): Unit = {
    nics.foreach { nic =>
      val nicAlreadyInBackend = nic.primaryIPConfiguration()
        .listAssociatedLoadBalancerBackends().asScala.exists(_.name() == backendName)
      if (!nicAlreadyInBackend) {
        nic.update()
          .withExistingLoadBalancerBackend(lb, backendName)
          .apply()
      } else {
        println(s"nic ${nic.name()} already configured")
      }
    }
  }

}

object AzureLoadBalancer {
  def deployFromParameters(parameters: Map[String, String]): Unit = {
      parameters.get("clusterType") match {
        case Some("hdi") =>
          val auth = parameters.get("authType") match {
            case Some("cli") => Azure.authenticate(AzureCliCredentials.create)
            case Some("servicePrincipal") =>
              val credentials = new ApplicationTokenCredentials(
                parameters("authClient"), parameters("authTenent"), parameters("authKey"), AzureEnvironment.AZURE)
              Azure.authenticate(credentials)
            case at => throw new IllegalArgumentException(s"Invalid setting of authType: $at")
          }
          new AzureLoadBalancer(auth,
            parameters("subscription"),
            parameters("resourceGroup")
          ).createIfNotExists(
            parameters("vnName"),
            parameters("dnsName"),
            parameters("port").toInt,
            parameters.getOrElse("frontEndPort", parameters("port")).toInt,
            s"Serving-${parameters("name")}")
        case ct => throw new IllegalArgumentException(s"Invalid setting of clusterType: $ct")
      }
  }

}
