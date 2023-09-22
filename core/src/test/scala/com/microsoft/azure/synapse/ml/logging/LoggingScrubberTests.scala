// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.logging.common.SASScrubber

class LoggingScrubberTests extends TestBase {
  test("SASScrubber Valid Input Test.") {
    val message = "sending {\"alignPolicy\":{},\"endTime\":\"2023-04-17T13:00:00Z\",\"slidingWindow\":300," +
      "\"source\":\"https://usw2itesgprodsa01g8kn8.blob.core.windows.net/raw/intermediate%2F" +
      "FitMultivariateAnomaly_b79685045fba.zip?sv=2020-10-02&se=2023-04-19T22%3A45%3A17Z&sr=b&sp=r&" +
      "sig=k2N3nSvLtiDH5xfYAwklSfyiuJ42aG8T8hLCNWdtNXk%3D\",\"startTime\":\"2020-07-01T00:00:00Z\"}"
    val scrubbedMessage = "sending {\"alignPolicy\":{},\"endTime\":\"2023-04-17T13:00:00Z\",\"slidingWindow\":300," +
      "\"source\":\"https://usw2itesgprodsa01g8kn8.blob.core.windows.net/raw/intermediate%2F" +
      "FitMultivariateAnomaly_b79685045fba.zip?sv=2020-10-02&se=2023-04-19T22%3A45%3A17Z&sr=b&sp=r&" +
      "sig=####\",\"startTime\":\"2020-07-01T00:00:00Z\"}"
    val result = SASScrubber.scrub(message)
    assert(result == scrubbedMessage)
  }
  test("SASScrubber Invalid Input Test.") {
    val message = "sending {\"alignPolicy\":{},\"endTime\":\"2023-04-17T13:00:00Z\",\"slidingWindow\":300," +
      "\"source\":\"https://usw2itesgprodsa01g8kn8.blob.core.windows.net/raw/intermediate%2F" +
      "FitMultivariateAnomaly_b79685045fba.zip?sv=2020-10-02&se=2023-04-19T22%3A45%3A17Z&sr=b&sp=r&" +
      "sig=k2N3nSvLt@DH5xfYAwkl###iuJ42aG8T8hLCNWdtNXk%3D\",\"startTime\":\"2020-07-01T00:00:00Z\"}"
    val scrubbedMessage = "sending {\"alignPolicy\":{},\"endTime\":\"2023-04-17T13:00:00Z\",\"slidingWindow\":300," +
      "\"source\":\"https://usw2itesgprodsa01g8kn8.blob.core.windows.net/raw/intermediate%2F" +
      "FitMultivariateAnomaly_b79685045fba.zip?sv=2020-10-02&se=2023-04-19T22%3A45%3A17Z&sr=b&sp=r&" +
      "sig=####\",\"startTime\":\"2020-07-01T00:00:00Z\"}"
    val result = SASScrubber.scrub(message)
    assert(result != scrubbedMessage)
  }
}
