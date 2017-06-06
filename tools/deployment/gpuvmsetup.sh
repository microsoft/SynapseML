#!/bin/bash
## Install hdfs-mount, NVIDIA driver and cntk on a GPU VM (UbuntuServer 16.04-LTS)

set -eux

# -e: immediately exit if any command has a non-zero exit status
# -u: immediately exit when script tries to use undeclared variables
# -x: trace what gets executed (useful for debugging)

BASE_URI="https://tongtest.blob.core.windows.net/cntk"
HDFSMOUNT="hdfs-mount"
HDFSMOUNT_BINARY_URI="$BASE_URI/$HDFSMOUNT"
NVIDIA_DRIVER_DOWNLOAD_SITE="http://us.download.nvidia.com/XFree86/Linux-x86_64/375.39"
NVIDIA_DRIVER="NVIDIA-Linux-x86_64-375.39.run"
NVIDIA_DRIVER_URI="$NVIDIA_DRIVER_DOWNLOAD_SITE/NVIDIA-Linux-x86_64-375.39.run"
CNTK="CNTK-2-0-beta11-0-Linux-64bit-GPU-1bit-SGD.tar.gz"
CNTK_BINARY_URI="$BASE_URI/$CNTK"
CNTK_PATH="/usr/bin"
HDFSMOUNT_PATH="/usr/bin"
CNTK_INSTALLER_PATH="$CNTK_PATH/cntk/Scripts/install/linux"
ANACONDA_BASEPATH="/usr/bin/anaconda3"

## Install NVIDIA driver

cd $CNTK_PATH
wget $NVIDIA_DRIVER_URI

if [ -f "$NVIDIA_DRIVER" ]; then
	chmod +x $NVIDIA_DRIVER
else
	echo "$NVIDIA_DRIVER not found"
	exit 1
fi

# Install gcc and make if not installed
set +e
if dpkg -s gcc | grep "ok"; then echo "gcc installed"; else apt update && apt install -y gcc; fi
if dpkg -s make | grep "ok"; then echo "make installed"; else apt update && apt install -y make; fi
set -e

# Install NVIDIA driver in silent mode
sh $NVIDIA_DRIVER -s

## Install hdfs-mount

cd $HDFSMOUNT_PATH
wget $HDFSMOUNT_BINARY_URI

if [ -f "$HDFSMOUNT" ]; then
	chmod +x $HDFSMOUNT
else
	echo "$HDFSMOUNT not found"
	exit 1
fi

echo HDFS_MOUNT_PATH=$HDFSMOUNT_PATH/$HDFSMOUNT >> /etc/environment
echo 'user_allow_other' >> /etc/fuse.conf

## Install cntk

cd $CNTK_PATH
wget $CNTK_BINARY_URI

if [ -f "$CNTK" ]; then
	tar -xvf $CNTK
else
	echo "$CNTK not found"
	exit 1
fi

cd $CNTK_INSTALLER_PATH
./install-cntk.sh

cd $CNTK_PATH
if [ -f "$CNTK" ]; then
	rm $CNTK
fi

echo CNTK_PATH=$CNTK_PATH/cntk >> /etc/environment
