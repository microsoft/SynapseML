// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.services.testutils

import com.microsoft.azure.synapse.ml.core.env.StreamUtilities.using
import com.microsoft.azure.synapse.ml.io.http.RESTHelpers
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

trait ImageDownloadUtils {

  def downloadBytes(url: String): Array[Byte] = {
    val request = new HttpGet(url)
    using(RESTHelpers.Client.execute(request)) { response =>
      IOUtils.toByteArray(response.getEntity.getContent)
    }.get
  }

  val downloadBytesUdf: UserDefinedFunction = udf(downloadBytes _)
}
