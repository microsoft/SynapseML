# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

<#

 .SYNOPSIS
    Deploys a template to Azure

 .DESCRIPTION
    Deploys an Azure Resource Manager template with a given parameters file.

 .PARAMETER subscriptionId
    The Subscription ID where the template will be deployed.

 .PARAMETER resourceGroupName
    The Resource Group where the template will be deployed.
    Can be the name of an existing resource group or a new one which will be
    created.

 .PARAMETER resourceGroupLocation
    A resource group location.
    If the resourceGroupName does not exist, this parameter is required for the
    creation of the group, specifying its location.

 .PARAMETER deploymentName
    The deployment name.

 .PARAMETER templateLocation
    URL of the template to deploy.
    Optional, defaults to the one corresponding to this script.

 .PARAMETER parametersFilePath
    Path of the parameters file to use for the template, use
    deploy-parameters.template to create this file.

    If file is not found, will prompt for parameter values based on
    template.

 .EXAMPLE
   Deploy-Arm
   Interactively read values and run.

 .EXAMPLE
   Deploy-Arm deploy-main-template.json -resourceGroupName MyCluster -parametersFilePath MyParameters.json

   Deploy the Cluster + GPU template, in the default subscription, under
   the existing "MyCluster" group with the parameters in MyParameters.json

#>

param(
  [Parameter(Mandatory=$True)]
  [string]
  $subscriptionId,

  [Parameter(Mandatory=$True)]
  [string]
  $resourceGroupName,

  [Parameter(Mandatory=$False)]
  [string]
  $resourceGroupLocation,

  [Parameter(Mandatory=$False)]
  [string]
  $deploymentName,

  [Parameter(Mandatory=$False)]
  [string]
  $templateLocation,

  [Parameter(Mandatory=$True)]
  [string]
  $parametersFilePath
)

# <=<= this line is replaced with variables defined with `defvar -X` =>=>
$DOWNLOAD_URL = "$STORAGE_URL/$MML_VERSION"
# TODO: throw an error if $MML_VERSION is not defined

<#
.SYNOPSIS
    Registers RPs
#>
Function RegisterRP {
  Param(
    [string]$ResourceProviderNamespace
  )
  Write-Host "Registering resource provider '$ResourceProviderNamespace'";
  Register-AzureRmResourceProvider -ProviderNamespace $ResourceProviderNamespace;
}

#******************************************************************************
# Script body
# Execution begins here
#******************************************************************************

if (!$templateLocation) {
  $templateLocation = $DOWNLOAD_URL + "/deploy-main-template.json";
}

$ErrorActionPreference = "Stop"

# sign in
Write-Host "Logging in...";
Login-AzureRmAccount;

# select subscription
Write-Host "Selecting subscription '$subscriptionId'";
Select-AzureRmSubscription -SubscriptionID $subscriptionId;

# Register RPs
$resourceProviders = @("microsoft.hdinsight");
if ($resourceProviders.length) {
  Write-Host "Registering resource providers"
  foreach ($resourceProvider in $resourceProviders) {
    RegisterRP($resourceProvider);
  }
}

#Create or check for existing resource group
$resourceGroup = Get-AzureRmResourceGroup -Name $resourceGroupName -ErrorAction SilentlyContinue
if (!$resourceGroup) {
  Write-Host "Resource group '$resourceGroupName' does not exist. To create a new resource group, please enter a location.";
  if (!$resourceGroupLocation) {
    $resourceGroupLocation = Read-Host "resourceGroupLocation";
  }
  Write-Host "Creating resource group '$resourceGroupName' in location '$resourceGroupLocation'";
  New-AzureRmResourceGroup -Name $resourceGroupName -Location $resourceGroupLocation
} else {
  Write-Host "Using existing resource group '$resourceGroupName'";
}

# Start the deployment
Write-Host "Starting deployment...";
if (Test-Path $parametersFilePath) {
  New-AzureRmResourceGroupDeployment -ResourceGroupName $resourceGroupName -TemplateUri $templateLocation -TemplateParameterFile $parametersFilePath;
} else {
  New-AzureRmResourceGroupDeployment -ResourceGroupName $resourceGroupName -TemplateUri $templateLocation;
}
