#!/bin/bash
# Add Microsoft package signing key and repository
wget https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
rm packages-microsoft-prod.deb

# Install .NET SDK
sudo apt-get update
sudo apt-get install -y apt-transport-https dotnet-sdk-5.0

# Update Nuget Config to include SynapseML Feed
dotnet nuget add source https://mmlspark.blob.core.windows.net/synapsemlnuget/index.json -n SynapseMLFeed

# Install .NET for Apache Spark
wget https://github.com/dotnet/spark/releases/download/v2.1.1/Microsoft.Spark.Worker.netcoreapp3.1.linux-x64-2.1.1.tar.gz
tar -xvf Microsoft.Spark.Worker.netcoreapp3.1.linux-x64-2.1.1.tar.gz -C ~/bin/
export DOTNET_WORKER_DIR=~/bin/Microsoft.Spark.Worker-2.1.1
echo "##vso[task.setvariable variable=DOTNET_WORKER_DIR]$DOTNET_WORKER_DIR"

# Install Sleet
dotnet tool install -g sleet

# Install Apache Spark-3.3
curl https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz -o spark-3.3.1-bin-hadoop3.tgz
mkdir ~/bin
tar -xzvf spark-3.3.1-bin-hadoop3.tgz -C ~/bin
export SPARK_HOME=~/bin/spark-3.3.1-bin-hadoop3/
export PATH=$SPARK_HOME/bin:$PATH
echo "##vso[task.setvariable variable=SPARK_HOME]$SPARK_HOME"
echo "##vso[task.setvariable variable=PATH]$SPARK_HOME/bin:$PATH"
