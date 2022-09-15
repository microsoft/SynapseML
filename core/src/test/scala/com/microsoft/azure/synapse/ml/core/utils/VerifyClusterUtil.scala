// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.slf4j.LoggerFactory

class VerifyClusterUtil extends TestBase {
  test("Verify ClusterUtil can get default number of executor cores based on master") {
    val log = LoggerFactory.getLogger("VerifyClusterUtil")

    // https://spark.apache.org/docs/latest/configuration.html
    assert(ClusterUtil.getDefaultNumExecutorCores(spark, log, Option("yarn")) == 1)
    assert(ClusterUtil.getDefaultNumExecutorCores(spark, log, Option("spark://localhost:7077")) ==
      ClusterUtil.getJVMCPUs(spark))
  }
}
