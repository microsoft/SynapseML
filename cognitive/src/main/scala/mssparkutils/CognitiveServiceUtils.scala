// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package mssparkutils

object CognitiveServiceUtils {
  def getEndpointAndKey(lsName: String): (String, String) = {
    ("https://wenqxanodet.cognitiveservices.azure.com/", lsName)
  }
}
