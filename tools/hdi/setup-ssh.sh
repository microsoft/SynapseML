#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

# -e: immediately exit if any command has a non-zero exit status
# -o: prevents errors in a pipeline from being masked
# IFS new value is less likely to cause confusing bugs when looping arrays or arguments (e.g. $@)

usage() { echo "Usage: $0 -m <virtualMachineName> -u <userName>" 1>&2; exit 1; }

declare virtualMachineName=""
declare userName=""


# Initialize parameters specified from command line
while getopts ":m:u:" arg; do
	case "${arg}" in
		m)
			virtualMachineName=${OPTARG}
			;;
		u)
			userName=${OPTARG}
			;;
		esac
done
shift $((OPTIND-1))

# Prompt for parameters if some required parameters are missing
if [[ -z "$virtualMachineName" ]]; then
	echo "virtualMachineName:"
	read virtualMachineName
	[[ "${virtualMachineName:?}" ]]
fi

if [[ -z "$virtualMachineName" ]]; then
	echo "virtualMachineName is empty!"
	usage
fi

# Check if the private key has been generated
privateKeyWasbFolder="wasb:///MML-GPU"
privateKeyWasbFile="$privateKeyWasbFolder/identity"
privaeKeyFile="$HOME/.ssh/id_rsa"
publicKeyFile="$HOME/.ssh/id_rsa.pub"
set +e

hdfs dfs -ls $privateKeyWasbFolder 1> /dev/null 2>&1

if [ $? != 0 ]; then
    	echo "Creating wasb drop folder.."

	set -e
	(
		hdfs dfs -mkdir $privateKeyWasbFolder 1> /dev/null
	)
fi

set +e
hdfs dfs -ls $privateKeyWasbFile 1> /dev/null 2>&1

if [ $? != 0 ]; then
	echo "Generating ssh key pair if they are not there.."
	set -e
	(
		if [ ! -f "$privaeKeyFile" ]; then
		    ssh-keygen -f $privaeKeyFile -t rsa -N '' 1> /dev/null
		fi		

		hdfs dfs -copyFromLocal $privaeKeyFile $privateKeyWasbFile 1> /dev/null
	)
	else
	echo "Using existing ssh key pair..."
fi

# Copy public key to the virtual machine
set -e
echo "Copying public key to the virtual machine.."

if [[ -z "$userName" ]]; then
	ssh-copy-id -i $publicKeyFile -o StrictHostKeyChecking=no $virtualMachineName 1> /dev/null
	else
	ssh-copy-id -i $publicKeyFile -o StrictHostKeyChecking=no "$userName@$virtualMachineName" 1> /dev/null
fi
	
if [ $?  == 0 ]; then
	echo "Ssh connection has been successfully set up"
fi
