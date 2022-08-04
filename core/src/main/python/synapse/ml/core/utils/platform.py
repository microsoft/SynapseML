# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.
import os


def current_platform():
    if os.environ.get("AZURE_SERVICE", None) == "Microsoft.ProjectArcadia":
        return "synapse"
    elif "dbfs" in os.listdir("/"):
        return "databricks"
    elif os.environ.get("BINDER_LAUNCH_HOST", None) is not None:
        return "binder"
    else:
        return "unknown"
