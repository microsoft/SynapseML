# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.
import os


PLATFORM_SYNAPSE = "synapse"
PLATFORM_BINDER = "binder"
PLATFORM_DATABRICKS = "databricks"
PLATFORM_UNKNOWN = "unknown"
SECRET_STORE = "mmlspark-build-keys"
SYNAPSE_PROJECT_NAME = "Microsoft.ProjectArcadia"


def CurrentPlatform():
    if os.environ.get("AZURE_SERVICE", None) == SYNAPSE_PROJECT_NAME:
        return PLATFORM_SYNAPSE
    elif "dbfs" in os.listdir("/"):
        return PLATFORM_DATABRICKS
    elif os.environ.get("BINDER_LAUNCH_HOST", None) is not None:
        return PLATFORM_BINDER
    else:
        return PLATFORM_UNKNOWN


def RunningOnSynapse():
    if CurrentPlatform() is PLATFORM_SYNAPSE:
        return True
    return False


def RunningOnBinder():
    if CurrentPlatform() is PLATFORM_BINDER:
        return True
    return False


def RunningOnDatabricks():
    if CurrentPlatform() is PLATFORM_DATABRICKS:
        return True
    return False


def GetPlatformSpecificSecret(searchKey):
    if RunningOnSynapse():
        from notebookutils.mssparkutils.credentials import getSecret

        return getSecret(SECRET_STORE, searchKey)
    elif RunningOnDatabricks():
        from pyspark.sql import SparkSession
        from pyspark.dbutils import DBUtils

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)
        return dbutils.secrets.get(scope=SECRET_STORE, key=searchKey)
    else:
        print(
            "#### Please add your environment/service specific key(s) before running the notebook ####"
        )
        return ""
