package com.microsoft.ml.spark.core.utils

import com.microsoft.ml.spark.core.test.base.{SparkSessionFactory, TestBase}
import org.slf4j.LoggerFactory

class VerifyClusterUtil extends TestBase {
  test("Verify ClusterUtil can get default number of executor cores based on master") {
    val spark = SparkSessionFactory.getSession("verifyClusterUtil-Session")
    val log = LoggerFactory.getLogger("VerifyClusterUtil")

    // https://spark.apache.org/docs/latest/configuration.html
    assert(ClusterUtil.getDefaultNumExecutorCores(spark, log, Option("yarn")) == 1)
    assert(ClusterUtil.getDefaultNumExecutorCores(spark, log, Option("spark://localhost:7077")) ==
      ClusterUtil.getJVMCPUs(spark))
  }
}
