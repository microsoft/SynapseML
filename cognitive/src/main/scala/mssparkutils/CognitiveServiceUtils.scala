// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package mssparkutils

object CognitiveServiceUtils {

  def getEndpoint(lsName: String): String = {
    "https://wenqxanodet.cognitiveservices.azure.com/"
  }

  def getKey(lsName: String): String = {
    lsName
  }
}
