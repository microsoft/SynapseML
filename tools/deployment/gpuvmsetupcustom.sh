#!/bin/bash
## Install custom built cntk with parquet reader on a GPU VM (UbuntuServer 16.04-LTS)

set -eux

# -e: immediately exit if any command has a non-zero exit status
# -u: immediately exit when script tries to use undeclared variables
# -x: trace what gets executed (useful for debugging)

BASE_URI="https://tongtest.blob.core.windows.net/cntk"
INSTALL_PATH="/usr/local"

NVIDIA_DRIVER_DOWNLOAD_SITE="http://us.download.nvidia.com/XFree86/Linux-x86_64/384.66"
NVIDIA_DRIVER="NVIDIA-Linux-x86_64-384.66.run"
NVIDIA_DRIVER_URI="$NVIDIA_DRIVER_DOWNLOAD_SITE/$NVIDIA_DRIVER"

CNTK="cntkparquetreadergpubuild.tgz"
CNTK_BINARY_URI="$BASE_URI/$CNTK"
CNTK_INSTALL="$INSTALL_PATH/cntk"
CNTK_PATH="$CNTK_INSTALL/releasegpu"

MPI_DOWNLOAD_SITE="https://www.open-mpi.org/software/ompi/v1.10/downloads"
MPI_VERSION="openmpi-1.10.3"
MPI="$MPI_VERSION.tar.gz"
MPI_URI="$MPI_DOWNLOAD_SITE/$MPI"
MPI_INSTALL="$INSTALL_PATH/mpi"
MPI_PATH="$MPI_INSTALL/$MPI_VERSION"

HADOOP_DOWNLOAD_SITE="http://www-us.apache.org/dist/hadoop/common"
HADOOP_VERSION="hadoop-2.8.1"
HADOOP="$HADOOP_VERSION.tar.gz"
HADOOP_URI="$HADOOP_DOWNLOAD_SITE/$HADOOP_VERSION/$HADOOP"
HADOOP_PATH="$INSTALL_PATH/$HADOOP_VERSION"

ZULU_DOWNLOAD_SITE="http://repos.azulsystems.com/debian"
ZULU="zulu-8"
JAVA_HOME="/usr/lib/jvm/zulu-8-amd64"

## Install NVIDIA driver
cd $INSTALL_PATH
wget $NVIDIA_DRIVER_URI

if [ -f "$NVIDIA_DRIVER" ]; then
	chmod +x $NVIDIA_DRIVER
else
	echo "$NVIDIA_DRIVER not found"
	exit 1
fi

# Install gcc/g++ and make if not installed 
set +e
if dpkg -s gcc | grep "ok"; then echo "gcc installed"; else apt update && apt install -y gcc; fi
if dpkg -s g++ | grep "ok"; then echo "g++ installed"; else apt update && apt install -y g++; fi
if dpkg -s make | grep "ok"; then echo "make installed"; else apt update && apt install -y make; fi
set -e

# Install NVIDIA driver in silent mode
sh $NVIDIA_DRIVER -s

## Install prebuilt cntk with parquet reader
if [ -d $CNTK_INSTALL ]; then
    echo "$MPI_INSTALL exists"
else
    mkdir $CNTK_INSTALL
fi

cd $CNTK_INSTALL
wget $CNTK_BINARY_URI

if [ -f "$CNTK" ]; then
	tar -xzvf $CNTK
else
	echo "$CNTK not found"
	exit 1
fi

if [ -f "$CNTK" ]; then
	rm $CNTK
fi

## Install MPI
if [ -d $MPI_INSTALL ]; then
    echo "$MPI_INSTALL exists"
else
    mkdir $MPI_INSTALL
fi

cd $MPI_INSTALL
wget $MPI_URI
tar -xzvf $MPI

cd $MPI_PATH
./configure --prefix=$MPI_INSTALL
make -j all
make install

cd $MPI_INSTALL
if [ -f "$MPI" ]; then
	rm $MPI
fi

## Install Hadoop binary
cd $INSTALL_PATH
wget $HADOOP_URI
tar -xzvf $HADOOP

if [ -f "$HADOOP" ]; then
	rm $HADOOP
fi

## Install Zulu
echo "deb $ZULU_DOWNLOAD_SITE stable main" > /etc/apt/sources.list.d/zulu.list
apt-get update && apt-get install -y --allow-unauthenticated $ZULU 
apt-get clean && rm -rf /var/lib/apt/lists/*

## Set environment variables
echo "CNTK_PATH=$CNTK_PATH" >> /etc/environment
echo "PATH=$MPI_INSTALL/bin:$CNTK_PATH/bin:$PATH" >> /etc/environment
echo "LD_LIBRARY_PATH=$MPI_INSTALL/lib:$CNTK_PATH/dependencies/lib" >> /etc/environment
echo "JAVA_HOME=$JAVA_HOME" >> /etc/environment
echo "HADOOP_HOME=$HADOOP_PATH" >> /etc/environment
echo 'export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath --glob)' >> /etc/profile
