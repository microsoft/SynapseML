#!/bin/bash

##############################################################################
# Description:
# This is a helper script to install the worker binaries on your Apache Spark cluster
#
# Usage:
# ./install-worker.sh <release-provider> <path-to-worker-release> <local-worker-installation-path>
#
# Sample usage:
# ./install-worker.sh
#    github
#    https://github.com/dotnet/spark/releases/download/v2.1.1/Microsoft.Spark.Worker.netcoreapp3.1.linux-x64-2.1.1.tar.gz
#    /usr/local/bin
#
# or if you have your Worker release on filesystem like ABFS, here's how the path would
# look like:
# ./install-worker.sh
#    azure
#    abfs://<blobcontainer>@<gen2storageaccount>.dfs.core.windows.net/<path>/Microsoft.Spark.Worker.netcoreapp3.1.linux-x64-2.1.1.tar.gz
#    /usr/local/bin
#
##############################################################################

set +e

# Uncomment if you want full tracing (for debugging purposes)
#set -o xtrace

# Cloud Provider
CLOUD_PROVIDER=$1

# Path where packaged worker file (tgz) exists.
SRC_WORKER_PATH_OR_URI=$2

# The path on the executor nodes where Microsoft.Spark.Worker executable is installed.
WORKER_INSTALLATION_PATH=$3

# The path where all the dependent libraies are installed so that it doesn't
# pollute the $WORKER_INSTALLATION_PATH.
SPARKDOTNET_ROOT=$WORKER_INSTALLATION_PATH/spark-dotnet

# Temporary worker file.
TEMP_WORKER_FILENAME=/tmp/temp_worker.tgz

# Extract version
IFS='-' read -ra BASE_FILENAME <<< "$(basename $SRC_WORKER_PATH_OR_URI .tar.gz)"
VERSION=${BASE_FILENAME[2]}

IFS='.' read -ra VERSION_CHECK <<< "$VERSION"
[[ ${#VERSION_CHECK[@]} == 3 ]] || { echo >&2 "Version check does not satisfy. Raise an issue here: https://github.com/dotnet/spark"; exit 1; }

# Path of the final destination for the worker binaries
# (the one we just downloaded and extracted)
DEST_WORKER_PATH=$SPARKDOTNET_ROOT/Microsoft.Spark.Worker-$VERSION
DEST_WORKER_BINARY=$DEST_WORKER_PATH/Microsoft.Spark.Worker

# Clean up any existing files.
sudo rm -f $WORKER_INSTALLATION_PATH/Microsoft.Spark.Worker
sudo rm -rf $SPARKDOTNET_ROOT

# Copy the worker file to a local temporary file.
if [ $"${CLOUD_PROVIDER,,}" = "github" ]; then
  wget $SRC_WORKER_PATH_OR_URI -O $TEMP_WORKER_FILENAME
elif [ "${CLOUD_PROVIDER,,}" = "azure" ]; then
  hdfs dfs -get $SRC_WORKER_PATH_OR_URI $TEMP_WORKER_FILENAME
elif [ "${CLOUD_PROVIDER,,}" = "aws" ]; then
  aws s3 cp $SRC_WORKER_PATH_OR_URI $TEMP_WORKER_FILENAME
else
  cp -f $SRC_WORKER_PATH_OR_URI $TEMP_WORKER_FILENAME
fi

# Untar the file.
sudo mkdir -p $SPARKDOTNET_ROOT
sudo tar xzf $TEMP_WORKER_FILENAME -C $SPARKDOTNET_ROOT

# Make the file executable since dotnet doesn't set this correctly.
sudo chmod 755 $DEST_WORKER_BINARY

# Create a symlink.
sudo ln -sf $DEST_WORKER_BINARY $WORKER_INSTALLATION_PATH/Microsoft.Spark.Worker

# Remove the temporary worker file.
sudo rm $TEMP_WORKER_FILENAME
