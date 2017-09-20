#!/usr/bin/env bash
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# <=<= this line is replaced with variables defined with `defvar -X` =>=>

NB_DIR="$CLUSTER_SDK_DIR/notebooks" # gets created as root, need to chown to spark

# This is the public key used by the build to access the test cluster
PUB_KEY="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC0FUryXQloryZQGXVP9vOqBVsuUWihHs"
PUB_KEY+="YPHvNf8PgR6ctUxPrvdZheAuJ+JLmauZeV2B01lSqCdyhnkwxTKiwLh2dDFx2yruAcXd2"
PUB_KEY+="0MGjD3bc8kC60GxMgRMsRxL6Jgz9FtauLFLxiDuvsRxQcSCBGd+l+pPR/NuFZeSHlmRWC"
PUB_KEY+="mb25fY29tqyitEqytRT9viBA1QpoERSPuzr3DEy3YIJ4BLVen0VYLKMU58L7oyEZxTElm"
PUB_KEY+="7nQMeQKgRBWUZZgCB1pXR3JiTYni/bWP2t9wCWfgfNfSs1oUttt14Libm9NgRbjq2QzN8"
PUB_KEY+="aQtVv1KyAUKOEdPmFqiGCPh1lRvm4KB7MF key-for-VSO"

cd ~spark || { echo "ERROR: could not find ~spark, aborting" 1>&2; exit 1; }

# Add public key to authorized key
if [[ ! -f ".ssh/authorized_keys" ]] || ! grep -q " key-for-VSO\$" ".ssh/authorized_keys"; then
  echo "Public key not found in authorized keys. Adding key..."
  mkdir -p ".ssh"
  echo "$PUB_KEY" >> ".ssh/authorized_keys"
  chown -R "spark:spark" ".ssh"
  chmod 700 ".ssh"
else
  echo "Public key already added to authorized keys. Skipping..."
fi

chown -R "spark:spark" "$NB_DIR"

. /usr/bin/anaconda/bin/activate
conda update setuptools
pip install --upgrade nbconvert
pip install xmlrunner
