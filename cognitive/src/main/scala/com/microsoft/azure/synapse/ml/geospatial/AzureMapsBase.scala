// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.geospatial

import com.microsoft.azure.synapse.ml.cognitive._

abstract class AzureMapsBase(override val uid: String)
  extends CognitiveServicesBase(uid) with HasSubscriptionKey with HasInternalJsonOutputParser {
  setDefault(errorCol -> (this.uid + "_error"))
}

abstract class AzureMapsBaseNoHandler(override val uid: String)
  extends CognitiveServicesBaseNoHandler(uid) with HasSubscriptionKey with HasInternalJsonOutputParser {
  setDefault(errorCol -> (this.uid + "_error"))
}
