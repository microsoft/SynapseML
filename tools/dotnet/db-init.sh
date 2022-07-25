#!/bin/bash

##############################################################################
# Description:
# This script installs the worker binaries and your app dependencies onto
# your Databricks Spark cluster.
#
# Usage:
# Change the variables below appropriately.
#
##############################################################################
################################# CHANGE THESE ###############################

# DOTNET_SPARK_RELEASE to point to the appropriate version you downloaded from the
# https://github.com/dotnet/spark Releases section. For instance, for v2.1.1, you
# would set it to the following URI:
# https://github.com/dotnet/spark/releases/download/v2.1.1/Microsoft.Spark.Worker.netcoreapp3.1.linux-x64-2.1.1.tar.gz
DOTNET_SPARK_RELEASE=https://github.com/dotnet/spark/releases/download/v2.1.1/Microsoft.Spark.Worker.netcoreapp3.1.linux-x64-2.1.1.tar.gz

# No need to change this unless you choose to use a different location
DBFS_INSTALLATION_ROOT=/dbfs/spark-dotnet
DOTNET_SPARK_WORKER_INSTALLATION_PATH=/usr/local/bin

###############################################################################

set +e
/bin/bash $DBFS_INSTALLATION_ROOT/install-worker.sh github $DOTNET_SPARK_RELEASE $DOTNET_SPARK_WORKER_INSTALLATION_PATH



##############################################################################
# Uncomment below to deploy application dependencies to workers if submitting
# jobs using the "Set Jar" task (https://docs.databricks.com/user-guide/jobs.html#jar-jobs)
# Change the variables below appropriately
##############################################################################
################################# CHANGE THESE ###############################

#APP_DEPENDENCIES=/dbfs/apps/dependencies
#WORKER_PATH=`readlink $DOTNET_SPARK_WORKER_INSTALLATION_PATH/Microsoft.Spark.Worker`
#if [ -f $WORKER_PATH ] && [ -d $APP_DEPENDENCIES ]; then
#    sudo cp -fR $APP_DEPENDENCIES/. `dirname $WORKER_PATH`
#fi
