#!/usr/bin/env bash
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# Load once
if [[ "${RUNME_LOADED:-}" = "$$" ]]; then return; else RUNME_LOADED="$$"; fi

# extra bash globs, quote expansion of quoted parameters
shopt -s globstar nullglob extglob extquote

# Where are we?
RUNMEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"; pwd)"
TOOLSDIR="$(dirname "$RUNMEDIR")"
BASEDIR="$(dirname "$TOOLSDIR")"

if [[ "${OS:-}" = "Windows_NT" ]]; then
  echo "This script cannot run on Windows (yet)." 1>&2; exit 1
fi

# PATH for these scripts: conservative (will include "$HOME/bin" later)
PATH="/usr/bin:/bin"

# shared for runme and all scriplets
. "$RUNMEDIR/utils.sh"
[[ -r "$TOOLSDIR/local-config.sh" ]] && @ "$TOOLSDIR/local-config.sh"
@ "../config.sh"; _post_config

# arrange to set these lazily, when `az` is called
az() {
  unset -f az
  export AZURE_STORAGE_ACCOUNT="$MAIN_STORAGE"
  export AZURE_STORAGE_KEY="$(
    x="$(az storage account keys list -n "$MAIN_STORAGE" -g "$MAIN_RESOURCE_GROUP" \
            -o tsv | head -1)"
    echo "${x##*$'\t'}")"
  az "$@"
}

# main runme functionality
_runme() {
  @ "install.sh"
  @ "build.sh"
  case "$BUILDMODE" in
    ( "build" | "server" )
      _install_environment
      _full_build
      ;;
    ( "setup" | "runtime" )
      _install_environment
      ;;
    ( "" )
      _install_environment
      if [[ "$inst_work_done" = "" ]]; then _full_build; exit; fi
      show section "$inst_work_done done"
      show warning "You can use the environment now," \
                   "or run this script again to build."
      ;;
    ( * )
      failwith "unknown build mode: $BUILDMODE"
      ;;
  esac
}
