#!/usr/bin/env bash
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# This script deploys a Spark Cluster and a GPU, see docs/gpu-setup.md
# for details.

# <=<= this line is replaced with variables defined with `defvar -X` =>=>
DOWNLOAD_URL="$STORAGE_URL/$MML_VERSION"
if [[ -z "$MML_VERSION" ]]; then
  echo "Error: this script cannot be executed as-is" 1>&2; exit 1
fi

set -euo pipefail
# -e: exit if any command has a non-zero exit status
# -u: unset variables are an error
# -o: prevents errors in a pipeline from being masked

here="$(dirname "$0")"
interactive=0; if [[ "$#" = "0" ]]; then interactive=1; fi

usage() {
  echo "Usage: $(basename "$0") \\"
  echo "           -i <subscriptionId> -g <resourceGroupName> \\"
  echo "           -n <deploymentName> -l <resourceGroupLocation> \\"
  echo "           -t <templateLocation> -p <parametersFilePath>"
  echo "Run without any arguments for interactive argument reading."
  echo "Use \"$here/deploy-parameters.template\" to create your parameters file."
  exit
}

if [[ "${1:-x}" = "-h" || "${1:-x}" == "--help" ]]; then usage; fi

failwith() { echo "Error: $*" 1>&2; exit 1; }

subscriptionId=""
resourceGroupName=""
deploymentName=""
resourceGroupLocation=""
templateLocation=""
parametersFilePath=""
while getopts ":i:g:n:l:t:p:" arg; do
  case "${arg}" in
    ( i ) subscriptionId="${OPTARG}"        ;;
    ( g ) resourceGroupName="${OPTARG}"     ;;
    ( n ) deploymentName="${OPTARG}"        ;;
    ( l ) resourceGroupLocation="${OPTARG}" ;;
    ( t ) templateLocation="${OPTARG}"      ;;
    ( p ) parametersFilePath="${OPTARG}"    ;;
  esac
done
shift $((OPTIND-1))

readarg() { # [-rf] varname name [default]
  # -r: required argument; -f: the default is a path that should exist
  local opts=""; while [[ "x$1" = "x-"* ]]; do opts+="${1:1}"; shift; done
  local req=0 file=0 reqstr
  if [[ "$opts" = *r* ]]; then req=1; fi
  if [[ "$opts" = *f* ]]; then file=1; fi
  local var="$1" name="$2" dflt=""; local -n X="$var"; shift 2
  if [[ "$#" -gt 0 ]]; then dflt="$1"; shift; fi
  if ((req)); then reqstr+="required"; else reqstr+="optional"; fi
  if [[ -z "$X" ]]; then
    if ((interactive)); then read -p "$name ($reqstr): " X; fi
    if [[ -z "$X" && -n "$dflt" ]]; then
      echo "Setting $var to default value: \"$dflt\""; X="$dflt"
    fi
  fi
  if [[ $req = 1 && -z "$X" ]]; then failwith "$name required"; fi
  if [[ $file = 1 && ! -r "$X" ]]; then failwith "$var: \"$X\" not found"; fi
}

# login if needed
cursub="$(az account show -o tsv 2> /dev/null \
  || { az login && az account show -o tsv; })"
if [[ "$cursub" != *$'\t'*$'\t'* ]]; then failwith "couldn't get login info"; fi
cursub="${cursub#*$'\t'}"; cursub="${cursub%%$'\t'*}"

readarg    subscriptionId        "Subscription ID" "$cursub"
readarg -r resourceGroupName     "Resource Group Name"
readarg    deploymentName        "Deployment Name"
readarg    resourceGroupLocation "Resource Group Location"
readarg    templateLocation      "Template Location URL" \
             "$DOWNLOAD_URL/deploy-main-template.json"
readarg -rf parametersFilePath   "Parameters File"

if [[ "$subscriptionId" != "$cursub" ]]; then
  # set subscription id, restore on exit
  restore_cursub() {
    echo "Restoring previous default subscription"
    az account set --subscription "$cursub"
  }
  trap restore_cursub EXIT
  az account set --subscription "$subscriptionId"
fi

# check for existing RG
if az group show -n "$resourceGroupName" | grep -q "$resourceGroupName"; then
  echo "Using existing resource group..."
else
  echo "Resource group with name $resourceGroupName not found, creating it."
  if [[ -z "$resourceGroupLocation" ]]; then failwith "resource group location required"; fi
  az group create --name "$resourceGroupName" --location "$resourceGroupLocation" \
     > /dev/null \
    || failwith "resource group creation failure"
fi

echo "Starting deployment..."
args=()
if [[ -n "$deploymentName" ]]; then args+=(--name "$deploymentName"); fi
args+=(--resource-group "$resourceGroupName")
args+=(--template-uri "$templateLocation")
args+=(--parameters "@$parametersFilePath")

az group deployment create "${args[@]}" || failwith "Deployment failed"
echo "Template has been successfully deployed"
