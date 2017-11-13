#!/usr/bin/env bash
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

usage() {
  echo "Usage: $(basename "$0") <machine-name> [<username>]"
  echo ""
  echo "Use this script from an HDInsight (master) node to set up key-based"
  echo "(passwordless) access to an external machine.  Mainly intended for"
  echo "attaching a GPU VM to an HDInsight cluster where <machine-name> would"
  echo "be the address (or IP) of a GPU VM."
  exit
}

if [[ "$#" = 0 || "$1" = "-h" || "$1" == "--help" ]]; then usage; fi

vmname="$1"; shift
user="$1"; shift

if [[ -z "$vmname" ]]; then usage; fi

wasb_dir="wasb:///MML-GPU"
wasb_private="$wasb_dir/identity"
wasb_public="$wasb_dir/identity.pub"
local_private="$HOME/.ssh/id_rsa"
local_public="$HOME/.ssh/id_rsa.pub"

# Create the directory if needed
hdfs dfs -test -d "$wasb_dir" || hdfs dfs -mkdir "$wasb_dir"

if ! hdfs dfs -test -e "$wasb_private"; then
  if [[ ! -f "$local_private" ]]; then
    echo ""
    echo "Creating an SSH public/private key pair"
    ssh-keygen -f "$local_private" -t rsa -N "" -C "MMLSpark GPU access key"
  fi
  echo ""
  echo "Copying SSH key pair to WASB"
  hdfs dfs -copyFromLocal "$local_private" "$wasb_private"
  hdfs dfs -copyFromLocal "$local_public"  "$wasb_public"
fi

echo ""
echo "Copying public key to the virtual machine"
if ssh-copy-id -i "$local_public" -o StrictHostKeyChecking=no "$user${user:+@}$vmname"; then
  echo "SSH keys were set up successfully"
else
  echo "Key copying failed!" 1>&2; exit 1
fi
