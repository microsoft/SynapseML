#!/usr/bin/env bash

# Install the prerequisites for MMLSpark on a GPU VM

INSTALL_DIR="/usr/local"

NVIDIA_VERSION="384.66"
NVIDIA_INSTALLER_URL="http://us.download.nvidia.com/XFree86/Linux-x86_64/$NVIDIA_VERSION"
NVIDIA_INSTALLER_URL+="/NVIDIA-Linux-x86_64-$NVIDIA_VERSION.run"

# Note: this is a custom build that includes the Parquet reader,
# all of these should change when it is part of CNTK
#  (Probably also run "Scripts/install/linux/install-cntk.sh")
CNTK_VERSION="2.0rc2"
CNTK_INSTALLER_URL="https://mmlspark.blob.core.windows.net/installers"
CNTK_INSTALLER_URL+="/cntk-$CNTK_VERSION-parquet-gpu.tgz"
CNTK_PATH="cntk-$CNTK_VERSION-parquet-gpu"

MPI_VERSION="1.10.3"
MPI_INSTALLER_URL="https://www.open-mpi.org/software/ompi/v1.10/downloads"
MPI_INSTALLER_URL+="/openmpi-$MPI_VERSION.tar.gz"
MPI_PATH="openmpi-$MPI_VERSION"

HADOOP_VERSION="2.8.1"
HADOOP_INSTALLER_URL="http://www-us.apache.org/dist/hadoop/common"
HADOOP_INSTALLER_URL+="/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz"
HADOOP_PATH="hadoop-$HADOOP_VERSION"

ZULU_DOWNLOAD_SITE="http://repos.azulsystems.com/debian"
ZULU_PKG="zulu-8"
JAVA_HOME="/usr/lib/jvm/zulu-8-amd64"

failwith() { show error "Error: $*" 1>&2; exit 1; }

if [[ "$(lsb_release -i)" != "Distributor ID:"*"Ubuntu" ]]; then
  failwith "This script is incompatible with non-Ubuntu machines"
fi

CURL_FLAGS="-f --location --retry 20 --retry-max-time 60 --connect-timeout 120"
CURL_FLAGS="$CURL_FLAGS --speed-limit 10 --speed-time 120"
install_url() { # dir, url, inst_arg...
  local url="$1"; shift
  local file="${url##*/}"
  local tmp="/tmp/$file"
  local owd="$PWD"
  curl $CURL_FLAGS "$url" > "$tmp" || \
    failwith "error retrieving $url"
  cd "$INSTALL_DIR"
  case "$file" in
  *.tgz | *.tar.gz ) tar xzf "$tmp" || failwith "Could not extract $file";;
  *.sh  | *.run )     chmod +x "$tmp"
                     "$tmp" "$@" || failwith "Errors while running $file";;
  * ) failwith "Internal error: unknown file extension: $file";;
  esac
  rm -f "$tmp"
  cd "$owd"
}

maybe_install() { # pkg...
  dpkg-query -W "$@" > /dev/null 2>&1 || \
    apt install -y --allow-unauthenticated "$@"
}

add_new_line() { # file, line...
  local file="$1"; shift
  local line
  for line in "$@"; do
    if [[ ! -e "$file" ]] || ! grep -qF "$line" "$file"; then
      printf "%s\n" "$line" >> "$file"
    fi
  done
}

# Install prerequisites
maybe_install "gcc" "g++" "make"

# Install Zulu
echo "deb $ZULU_DOWNLOAD_SITE stable main" > "/etc/apt/sources.list.d/zulu.list"
apt-get update && apt-get install -y --allow-unauthenticated "$ZULU_PKG"
add_new_line "/etc/environment" "JAVA_HOME=$JAVA_HOME"

# Install NVIDIA driver (in silent mode)
install_url "$NVIDIA_INSTALLER_URL" -s

# Install prebuilt cntk with parquet reader
install_url "$CNTK_INSTALLER_URL"
add_new_line "/etc/environment" \
  "CNTK_HOME=$INSTALL_DIR/$CNTK_PATH" \
  "PATH=$INSTALL_DIR/$CNTK_PATH/bin:$PATH" \
  "LD_LIBRARY_PATH=$INSTALL_DIR/$CNTK_PATH/dependencies/lib"

# Install MPI
install_url "$MPI_INSTALLER_URL"
cd "$INSTALL_DIR/$MPI_PATH"
./configure --prefix="$INSTALL_DIR" && make -j all && make install || \
   failwith "Error building MPI"
add_new_line "/etc/environment" "LD_LIBRARY_PATH=$INSTALL_DIR/lib:$LD_LIBRARY_PATH"

# Install Hadoop binary
install_url "$HADOOP_INSTALLER_URL"
add_new_line "/etc/environment" "HADOOP_HOME=$INSTALL_DIR/$HADOOP_PATH"
add_new_line "/etc/profile" \
  "export CLASSPATH=\$(\$HADOOP_HOME/bin/hadoop classpath --glob)"
