#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

# -e: immediately exit if any command has a non-zero exit status
# -o: prevents errors in a pipeline from being masked
# IFS new value is less likely to cause confusing bugs when looping arrays or arguments (e.g. $@)

usage() { echo "Usage: $0 -i <subscriptionId> -g <resourceGroupName> -n <deploymentName> -l <resourceGroupLocation> -t <templateFilePath> -p <parametersFilePath>" 1>&2; exit 1; }

declare subscriptionId=""
declare resourceGroupName=""
declare deploymentName=""
declare resourceGroupLocation=""
declare templateFilePath=""
declare parametersFilePath=""

# Initialize parameters specified from command line
while getopts ":i:g:n:l:t:p:" arg; do
	case "${arg}" in
		i)
			subscriptionId=${OPTARG}
			;;
		g)
			resourceGroupName=${OPTARG}
			;;
		n)
			deploymentName=${OPTARG}
			;;
		l)
			resourceGroupLocation=${OPTARG}
			;;
	        t)
			templateFilePath=${OPTARG}
			;;
		p)
			parametersFilePath=${OPTARG}
			;;
		esac
done
shift $((OPTIND-1))

#Prompt for parameters is some required parameters are missing
if [[ -z "$subscriptionId" ]]; then
	echo "Subscription Id:"
	read subscriptionId
	[[ "${subscriptionId:?}" ]]
fi

if [[ -z "$resourceGroupName" ]]; then
	echo "ResourceGroupName:"
	read resourceGroupName
	[[ "${resourceGroupName:?}" ]]
fi

if [[ -z "$deploymentName" ]]; then
	echo "DeploymentName:"
	read deploymentName
fi

if [[ -z "$resourceGroupLocation" ]]; then
	echo "Enter a location below to create a new resource group else skip this"
	echo "ResourceGroupLocation:"
	read resourceGroupLocation
fi

if [[ -z "$templateFilePath" ]]; then
	#templateFile Path - default template file to be used
	templateFilePath="azureDeployMainTemplate.json"
fi

if [ ! -f "$templateFilePath" ]; then
	echo "$templateFilePath not found"
	exit 1
fi

if [[ -z "$parametersFilePath" ]]; then
	#parameter file path - default parameter file to be used
	parametersFilePath="azureDeployParameters.json"
fi

if [ ! -f "$parametersFilePath" ]; then 
	echo "$parametersFilePath not found"
	exit 1
fi

if [ -z "$subscriptionId" ] || [ -z "$resourceGroupName" ] || [ -z "$deploymentName" ]; then
	echo "Either one of subscriptionId, resourceGroupName, deploymentName is empty"
	usage
fi

#login to azure using your credentials
set +e
az account show 1> /dev/null

if [ $? != 0 ]; 
then
	az login
fi

#set the default subscription id
set -e
az account set --subscription $subscriptionId

#Check for existing RG
set +e
az group show -n $resourceGroupName | grep -q $resourceGroupName

if [ $? != 0 ]; then
	echo "Resource group with name" $resourceGroupName "could not be found. Creating new resource group.."
	set -e
	(
		set -x
		az group create --name $resourceGroupName --location $resourceGroupLocation 1> /dev/null
	)
	else
	echo "Using existing resource group..."
fi

#set -e

#Start deployment
echo "Starting deployment..."
(
	set -x
	az group deployment create --name $deploymentName --resource-group $resourceGroupName --template-file $templateFilePath --parameters @$parametersFilePath
)

if [ $?  == 0 ]; 
 then
	echo "Template has been successfully deployed"
fi
