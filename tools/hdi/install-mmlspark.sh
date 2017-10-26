#!/usr/bin/env bash
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# -----------------------------------------------------------------------------
# Configurations for installing mmlspark + dependencies on an HDI
# cluster, from a specific storage blob (which is created by the build).

# <=<= this line is replaced with variables defined with `defvar -X` =>=>
DOWNLOAD_URL="$STORAGE_URL/$MML_VERSION"

HDFS_NOTEBOOKS_FOLDER="/HdiNotebooks/Microsoft ML Spark Examples"

CPATH="/usr/bin/anaconda/bin"
CONDA_ENVS=( $("$CPATH/conda" info --envs | grep "^[^#]" | sed -e "s/ .*//") )

CNTK_BASE_URL="https://cntk.ai/PythonWheel/CPU-Only"
declare -A CNTK_WHEELS=(
  [root]="$CNTK_BASE_URL/cntk-$CNTK_VERSION-cp27-cp27mu-linux_x86_64.whl"
  [py35]="$CNTK_BASE_URL/cntk-$CNTK_VERSION-cp35-cp35m-linux_x86_64.whl")

get_headnodes() {
  hdfssite="$(< "/etc/hadoop/conf/hdfs-site.xml")"
  host1="${hdfssite#*<name>dfs.namenode.http-address.mycluster.nn1*<value>}"
  host2="${hdfssite#*<name>dfs.namenode.http-address.mycluster.nn2*<value>}"
  host1="${host1%%:*</value>*}"; num1="${host1%%-*}"; num1="${num1#hn}"
  host2="${host2%%:*</value>*}"; num2="${host2%%-*}"; num2="${num2#hn}"
  if [[ "$host1,$host2" = "," ]]; then return; fi
  if (($num1 < $num2)); then echo "$host1,$host2"; else echo "$host2,$host1"; fi
}

get_primary_headnode() {
  headnodes="$(get_headnodes)"
  echo "${headnodes%%,*}"
}

# -----------------------------------------------------------------------------
# Run on all nodes

# Install prerequisites
apt-get install -y openmpi-bin libunwind8 libpng12-0 libjasper1

# Install CNTK in all environments
for env in "${CONDA_ENVS[@]}"; do
  # wheel="${CNTK_WHEELS[$env]:?"Unknown conda env for CNTK: $env"}"
  # ... but hdi will have additional environments, created by vienna
  if [[ -z "${CNTK_WHEELS[$env]}" ]]; then continue; fi
  echo -n "[$env] "
  . "$CPATH/activate" "$env"
  pkg="$(pip freeze | grep "^cntk")"
  if [[ "$pkg" != "cntk"* ]]; then echo "Installing CNTK..."; pip install "$wheel"
  elif [[ "$pkg" = *"$CNTK_VERSION" ]]; then echo "CNTK is already installed."
  else echo "Updating CNTK..."; pip install --upgrade --no-deps "$wheel"
  fi
  . "$CPATH/deactivate"
done

# Add CNTK dependencies to the system library paths
cntklibs="$("$CPATH/conda" info --envs | grep "^py35")"
if [[ -z "$cntklibs" ]]; then
  echo "Error: no \"py35\" environment found" 1>&2; exit 1
fi
cntklibs="/${cntklibs#*/}/lib/python3.5/site-packages/cntk/libs"
if [[ ! -d "$cntklibs" ]]; then
  echo "Error: no \"$cntklibs\" directory found" 1>&2; exit 1
fi
echo "$cntklibs" > "/etc/ld.so.conf.d/cntk-mmlspark.conf"
rm "/etc/ld.so.cache"; ldconfig # reload

# Download build artifacts & scripts
tmp="/tmp/mmlinstall-$$"
curlflags="--silent --show-error"
mkdir "$tmp"
echo "Downloading materials..."
curl $curlflags -o "$tmp/BuildArifacts.zip" "$DOWNLOAD_URL/BuildArtifacts.zip"
curl $curlflags -o "$tmp/update_livy.py" "$DOWNLOAD_URL/update_livy.py"
rm -rf "$CLUSTER_SDK_DIR"; mkdir -p "$CLUSTER_SDK_DIR"
cd "$CLUSTER_SDK_DIR"; unzip "$tmp/BuildArifacts.zip"; rm "$tmp/BuildArifacts.zip"

# Change the Livy configuration
echo "Updating the Livy configuration..."
python "$tmp/update_livy.py" "/home/spark/.sparkmagic/config.json" "$MAVEN_PACKAGE"
rm -rf "$tmp"

for env in "${CONDA_ENVS[@]}"; do
  . "$CPATH/activate" "$env"
  # first uninstall it, since otherwise an existing "1.2.dev3+4" version makes
  # pip consider "1.2.dev3" as "already satisfied"
  pip uninstall -y "mmlspark"
  pip install "$PIP_URL/$PIP_PACKAGE"
  . "$CPATH/deactivate"
done

/bin/su livy -c \
  "spark-shell --packages \"$MAVEN_PACKAGE\" --repositories \"$MAVEN_URL\" < /dev/null"

# Check whether script is running on headnode
if [[ "$(get_primary_headnode)" != "$(hostname -f)" ]]; then
  echo "$(hostname -f) is not primary headnode, exiting."
  exit 0
fi

# -----------------------------------------------------------------------------
# Run only on the main head node

# Copy notebooks to storage
hdfs dfs -rm -f -r -skipTrash "$HDFS_NOTEBOOKS_FOLDER"
hdfs dfs -mkdir -p "$HDFS_NOTEBOOKS_FOLDER"

# pure bash url encoder
urlencode() {
  local str="$1" ch
  for ((i=0; i < ${#str}; i++)); do
    ch="${str:i:1}"
    case "$ch" in
      ( [a-zA-Z0-9_.-] ) printf '%s' "$ch" ;;
      ( * ) printf '%%%02x' "'$ch" ;;
    esac
  done
  printf '\n'
}

for f in "$CLUSTER_SDK_DIR/notebooks/hdinsight/"*.ipynb; do
  hdfs dfs -copyFromLocal "$(urlencode "$f")" "$HDFS_NOTEBOOKS_FOLDER"
done

# Constants needed for changing Ambari configs
AMBARI_HOST="headnodehost"
AMBARI_PORT="8080"
AMBARI_USER="$(python -c '
import hdinsight_common.Constants as C
print C.AMBARI_WATCHDOG_USERNAME')"
AMBARI_PASSWD="$(python -c '
import hdinsight_common.ClusterManifestParser as P, hdinsight_common.Constants as C, base64
base64pwd = P.parse_local_manifest().ambari_users.usersmap[C.AMBARI_WATCHDOG_USERNAME].password
print base64.b64decode(base64pwd)')"
CLUSTERNAME="$(python -c '
import hdinsight_common.ClusterManifestParser as P
print P.parse_local_manifest().deployment.cluster_name')"

# Stop and restart affected services
stop_service_via_rest() { # service-name
  local name="$1"; echo "Stopping $name"
  local data='{"RequestInfo": {"context" :"Stopping service '"$name"' to install MMLSpark"},'
  data+=' "Body": {"ServiceInfo": {"state": "INSTALLED"}}}'
  curl $curlflags -u "$AMBARI_USER:$AMBARI_PASSWD" -i -H "X-Requested-By: ambari" -X PUT -d "$data" \
       "http://$AMBARI_HOST:$AMBARI_PORT/api/v1/clusters/$CLUSTERNAME/services/$name"
  echo ""
}
start_service_via_rest() { # service-name
  local name="$1"; echo "Starting $name"
  sleep 2
  local data='{"RequestInfo": {"context" :"Starting service '"$name"' with a new MMLSpark version"},'
  data+=' "Body": {"ServiceInfo": {"state": "STARTED"}}}'
  local args=($curlflags
              -u "$AMBARI_USER:$AMBARI_PASSWD" -i -H "X-Requested-By: ambari" -X PUT -d "$data"
              "http://$AMBARI_HOST:$AMBARI_PORT/api/v1/clusters/$CLUSTERNAME/services/$name")
  local r="$(curl "${args[@]}")"
  if [[ "$r" = *"500 Server Error"* || "$r" = *"internal system exception occurred"* ]]; then
    sleep 60
    echo "Retry starting $name"
    r="$(curl "${args[@]}")"
  fi
  echo "$r"
  echo ""
}

# Restart affected services
stop_service_via_rest LIVY
stop_service_via_rest JUPYTER
start_service_via_rest LIVY
start_service_via_rest JUPYTER

echo "Done."
