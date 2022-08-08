# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.
import os


PLATFORM_SYNAPSE = "synapse"
PLATFORM_BINDER = "binder"
PLATFORM_DATABRICKS = "databricks"
PLATFORM_UNKNOWN = "unknown"
SECRET_STORE = "mmlspark-build-keys"
SYNAPSE_PROJECT_NAME = "Microsoft.ProjectArcadia"


def current_platform():
    if os.environ.get("AZURE_SERVICE", None) == SYNAPSE_PROJECT_NAME:
        return PLATFORM_SYNAPSE
    elif "dbfs" in os.listdir("/"):
        return PLATFORM_DATABRICKS
    elif os.environ.get("BINDER_LAUNCH_HOST", None) is not None:
        return PLATFORM_BINDER
    else:
        return PLATFORM_UNKNOWN


def running_on_synapse():
    return current_platform() is PLATFORM_SYNAPSE


def running_on_binder():
    return current_platform() is PLATFORM_BINDER


def running_on_databricks():
    return current_platform() is PLATFORM_DATABRICKS


def get_platform_specific_secret(searchKey):
    if running_on_synapse():
        from notebookutils.mssparkutils.credentials import getSecret

        return getSecret(SECRET_STORE, searchKey)
    elif running_on_databricks():
        from pyspark.sql import SparkSession
        from pyspark.dbutils import DBUtils

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        return dbutils.secrets.get(scope=SECRET_STORE, key=searchKey)
    else:
        raise RuntimeError(
            "#### Please add your environment/service specific key(s) before running the notebook ####"
        ) from None
        return ""
