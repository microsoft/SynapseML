// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.logging.Usage.FabricConstants.{WorkloadEndpointAdmin}
import com.microsoft.azure.synapse.ml.logging.Usage.HostEndpointUtils._

class UsageUtilsTests extends TestBase {

  val target = "f32fae846ed04406944c01e26087aa9b.pbidedicated.windows-int.net/webapi/Capacities/" +
    "f32fae84-6ed0-4406-944c-01e26087aa9b/workloads/ML/MLAdmin/Automatic/" +
    "workspaceid/c1aaa432-2b6e-4325-acca-1aac063d9a6e/"
  val capacityId = "f32fae84-6ed0-4406-944c-01e26087aa9b"
  val wlHost = "f32fae846ed04406944c01e26087aa9b.pbidedicated.windows-int.net"
  val workspaceId = "c1aaa432-2b6e-4325-acca-1aac063d9a6e"
  test("ML Workload Endpoint Check"){
    val url = getMLWorkloadEndpoint(this.wlHost, this.capacityId, WorkloadEndpointAdmin, this.workspaceId)
    assert(url == target)
  }
}
