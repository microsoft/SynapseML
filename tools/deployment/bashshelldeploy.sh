#!/usr/bin/env bash
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

set -euo pipefail
IFS=$'\n\t'

# -e: immediately exit if any command has a non-zero exit status
# -o: prevents errors in a pipeline from being masked
# IFS new value is less likely to cause confusing bugs when looping arrays or arguments (e.g. $@)

usage() {
  echo "Usage: $(basename "$0") -i <subscriptionId> -g <resourceGroupName> -n <deploymentName> -l <resourceGroupLocation> -t <templateFilePath> -p <parametersFilePath>"
  exit
}

failwith() { echo "$*" 1>&2; exit 1; }

subscriptionId=""
resourceGroupName=""
deploymentName=""
resourceGroupLocation=""
templateFilePath=""
parametersFilePath=""

# initialize parameters specified from command line
while getopts ":i:g:n:l:t:p:" arg; do
  case "${arg}" in
    ( i ) subscriptionId="${OPTARG}"        ;;
    ( g ) resourceGroupName="${OPTARG}"     ;;
    ( n ) deploymentName="${OPTARG}"        ;;
    ( l ) resourceGroupLocation="${OPTARG}" ;;
    ( t ) templateFilePath="${OPTARG}"      ;;
    ( p ) parametersFilePath="${OPTARG}"    ;;
  esac
done
shift $((OPTIND-1))

# required
if [[ -z "$subscriptionId" ]]; then read -p "Subscription Id: " subscriptionId; fi
if [[ -z "$subscriptionId" ]]; then failwith "subscription id required"; fi

# required
if [[ -z "$resourceGroupName" ]]; then read -p "ResourceGroupName: " resourceGroupName; fi
if [[ -z "$resourceGroupName" ]]; then failwith "resource group name required"; fi

# optional
if [[ -z "$deploymentName" ]]; then read -p "DeploymentName: " deploymentName; fi

# optional
if [[ -z "$resourceGroupLocation" ]]; then
  echo "Enter a location to create a new resource group otherwise skip it"
  read -p "ResourceGroupLocation: " resourceGroupLocation
fi

if [[ -z "$templateFilePath" ]]; then templateFilePath="azureDeployMainTemplate.json"; fi

if [[ ! -f "$templateFilePath" ]]; then failwith "$templateFilePath not found"; fi

if [[ -z "$parametersFilePath" ]]; then parametersFilePath="azureDeployParameters.json"; fi

if [[ ! -f "$parametersFilePath" ]]; then failwith "$parametersFilePath not found"; fi

# login to azure using your credentials
az account show > /dev/null || az login

# set the default subscription id
az account set --subscription "$subscriptionId"

# check for existing RG
if az group show -n "$resourceGroupName" | grep -q "$resourceGroupName"; then
  echo "Using existing resource group..."
else
  echo "Resource group with name $resourceGroupName not found,"
  echo "Creating a new resource group."
  if [[ -z "$resourceGroupLocation" ]]; then failwith "resource group location required"; fi
  az group create --name "$resourceGroupName" --location "$resourceGroupLocation" \
     > /dev/null \
    || { echo "Resource group creation failure" 1>&2; exit 1; }
fi

echo "Starting deployment..."
az group deployment create \
   --name "$deploymentName" --resource-group "$resourceGroupName" \
   --template-file "$templateFilePath" --parameters "@$parametersFilePath" \
  || failwith "Deployment failed"
echo "Template has been successfully deployed"
