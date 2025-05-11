# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.
import os
from pyspark.sql import DataFrame

PLATFORM_SYNAPSE_INTERNAL = "synapse_internal"
PLATFORM_SYNAPSE = "synapse"
PLATFORM_FABRIC_PYTHON = "fabric_python_env"
PLATFORM_BINDER = "binder"
PLATFORM_DATABRICKS = "databricks"
PLATFORM_UNKNOWN = "unknown"
SECRET_STORE = "mmlspark-build-keys"
SYNAPSE_PROJECT_NAME = "Microsoft.ProjectArcadia"
FABRIC_URL = "fabric.microsoft.com"


def current_platform():
    if os.environ.get("AZURE_SERVICE", None) == SYNAPSE_PROJECT_NAME:
        from pyspark.sql import SparkSession

        sc = SparkSession.builder.getOrCreate().sparkContext
        cluster_type = sc.getConf().get("spark.cluster.type")
        if cluster_type == "synapse":
            return PLATFORM_SYNAPSE
        else:
            return PLATFORM_SYNAPSE_INTERNAL
    elif "dbfs" in os.listdir("/"):
        return PLATFORM_DATABRICKS
    elif os.environ.get("BINDER_LAUNCH_HOST", None) is not None:
        return PLATFORM_BINDER
    elif FABRIC_URL in os.environ.get("MSNOTEBOOKUTILS_SPARK_TRIDENT_PBIHOST", ""):
        return PLATFORM_FABRIC_PYTHON
    else:
        return PLATFORM_UNKNOWN


def running_on_synapse_internal():
    return current_platform() is PLATFORM_SYNAPSE_INTERNAL


def running_on_synapse():
    return current_platform() is PLATFORM_SYNAPSE


def running_on_binder():
    return current_platform() is PLATFORM_BINDER


def running_on_databricks():
    return current_platform() is PLATFORM_DATABRICKS


def running_on_fabric_python():
    return current_platform() is PLATFORM_FABRIC_PYTHON


def find_secret(secret_name, keyvault):
    try:
        if running_on_synapse():
            from notebookutils.mssparkutils.credentials import getSecret

            return getSecret(keyvault, secret_name)
        elif running_on_synapse_internal():
            from notebookutils.mssparkutils.credentials import getSecret

            keyVaultURL = f"https://{keyvault}.vault.azure.net/"
            return getSecret(keyVaultURL, secret_name)
        elif running_on_databricks():
            from pyspark.sql import SparkSession
            from pyspark.dbutils import DBUtils

            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)
            return dbutils.secrets.get(scope=keyvault, key=secret_name)
        else:
            raise RuntimeError("get secret is not supported on this platform.")
    except:
        raise RuntimeError(
            f"Could not find {secret_name} in keyvault {keyvault}. "
            f"If you are trying to use the mmlspark-buil-keys keyvault, you cant! "
            f"You need to make your own keyvault with secrets or replace this call with a string. "
            f"Make sure your notebook has access to a "
            f"keyvault named {keyvault} which contains a secret named {secret_name}. "
            f"On synapse, use a linked service keyvault, "
            f"on databricks use their secrets management sdk, "
            f"on fabric make sure your azure identity can access your azure keyvault. "
            f"If you want to avoid making a keyvault, replace this call to find secret with your secret as a string "
            f"like my_secret = 'jdiej38dnal.....'. Note that this has "
            f"security implications for publishing and sharing notebooks! "
            f"Please see the documentation for more information. "
            f"https://microsoft.github.io/SynapseML/docs/Get%20Started/Set%20up%20Cognitive%20Services/"
        )


def materializing_display(data):
    if running_on_synapse() or running_on_synapse_internal():
        from notebookutils.visualization import display

        if isinstance(data, DataFrame):
            data.collect()
        display(data)
    else:
        print(data)
