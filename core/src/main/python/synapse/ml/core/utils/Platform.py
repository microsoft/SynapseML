# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.
import os
from notebookutils.mssparkutils.credentials import getSecret


PLATFORM_SYNAPSE = "synapse"
PLATFORM_BINDER = "binder"
PLATFORM_DATABRICKS = "databricks"
PLATFORM_UNKNOWN = "unknown"


def CurrentPlatform():
    if os.environ.get("AZURE_SERVICE", None) == "Microsoft.ProjectArcadia":
        return PLATFORM_SYNAPSE
    elif "dbfs" in os.listdir("/"):
        return PLATFORM_DATABRICKS
    elif os.environ.get("BINDER_LAUNCH_HOST", None) is not None:
        return PLATFORM_BINDER
    else:
        return PLATFORM_UNKNOWN


def GetNotebookSecret(key=""):
    if CurrentPlatform() is PLATFORM_SYNAPSE:
        return getSecret(getSecret("mmlspark-build-keys", key))
    else:
        if os.environ.get("key", None) is None:
            print("Please add your environment/service specific keys")
        return ""
