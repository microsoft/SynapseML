# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from synapse.ml.services.vision._AnalyzeImageV4 import _AnalyzeImageV4
from pyspark.ml.common import inherit_doc
from pyspark.ml.param import TypeConverters


@inherit_doc
class AnalyzeImageV4(_AnalyzeImageV4):
    """Image Analysis 4.0 GA with a separate schema from legacy AnalyzeImage."""

    def setSmartCropsAspectRatios(self, value):
        """Set numeric ratios in [0.75, 1.8], accepting integers such as 1."""
        return super().setSmartCropsAspectRatios(TypeConverters.toListFloat(value))

    def _set_service_url(self, method, value):
        self._java_obj = getattr(self._java_obj, method)(value)
        # A prior setUrl also lives in Python's ParamMap. Keep it in sync so
        # transform/copy/save cannot overwrite the newly selected Java endpoint.
        return self._set(url=self._java_obj.getUrl())

    def setEndpoint(self, value):
        """Set the resource endpoint, with or without a trailing slash."""
        return self._set_service_url("setEndpoint", value)

    def setLocation(self, value):
        """Set the Azure region and replace any earlier URL override."""
        return self._set_service_url("setLocation", value)

    def setCustomServiceName(self, value):
        """Set the resource name and replace any earlier URL override."""
        return self._set_service_url("setCustomServiceName", value)

    def setLinkedService(self, value):
        """Use a linked service's endpoint and authentication."""
        self._set_service_url("setLinkedService", value)
        # Keep the resolved credential on the JVM; discard any older Python value.
        self._paramMap.pop(self.subscriptionKey, None)
        return self
