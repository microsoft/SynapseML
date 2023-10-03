// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.logging.fabric.FabricConstants

class UsageUtilsTests extends TestBase with FabricConstants {
  import com.microsoft.azure.synapse.ml.logging.fabric.HostEndpointUtils._

  val target = "c528701c8f9442c0b65a1660171c306c.pbidedicated.windows-int.net/webapi/Capacities/" +
    "c528701c-8f94-42c0-b65a-1660171c306c/workloads/ML/MLAdmin/Automatic/" +
    "workspaceid/89b9b330-6eac-4ee1-b225-590dfd68e4be/"
  val capacityId = "c528701c-8f94-42c0-b65a-1660171c306c"
  val wlHost = "c528701c8f9442c0b65a1660171c306c.pbidedicated.windows-int.net"
  val workspaceId = "89b9b330-6eac-4ee1-b225-590dfd68e4be"
  test("ML Workload Endpoint Check"){
    val url = getMLWorkloadEndpoint(this.wlHost, this.capacityId, workloadEndpointAdmin, this.workspaceId)
    assert(url == target)
  }
}
