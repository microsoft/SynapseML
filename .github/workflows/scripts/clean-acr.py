import os
import json
from azure.storage.blob import BlobClient
import sys
import time
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
"""
run this if sas expires and place result in keyvault under secret name 

 IMPORT_SAS=?$(az storage container generate-sas \
   --name acrbackup \
   --account-name mmlspark \
   --expiry 2024-01-01 \
   --permissions rawdl \
   --https-only \
   --output tsv \
   --auth-mode key \
   --account-key <key>)
   echo $IMPORT_SAS
"""

acr = "mmlsparkmcr"
container = "acrbackup"
rg = "marhamil-mmlspark"
pipeline = "mmlsparkacrexport3"

keyvaultName = sys.argv[1]
secretName = sys.argv[2]
kvUri = f"https://{keyvaultName}.vault.azure.net"
kvClient = SecretClient(vault_url=kvUri, credential=DefaultAzureCredential())
conn_string = kvClient.get_secret(secretName).value


def retry_command(command, tries):
    delay = 5
    for i in range(tries):
        print(command)
        result = os.system(command)
        if result == 0:
            break
        print(f"Command '{command}' failed. Retrying after {delay} seconds")
        time.sleep(delay)
        delay = delay * 3

    return result


os.system("az extension add --name acrtransfer")

repos = json.loads(os.popen(f"az acr repository list -n {acr}").read())
for repo in repos:
    tags = json.loads(
        os.popen(
            f"az acr repository show-tags -n {acr} --repository {repo} --orderby time_desc"
        ).read()
    )

    for tag in tags:
        target_blob = repo + "/" + tag + ".tar"
        image = repo + ":" + tag

        backup_exists = BlobClient.from_connection_string(
            conn_string, container_name=container, blob_name=target_blob
        ).exists()
        if not backup_exists:
            cmd = (
                f"az acr pipeline-run create --resource-group {rg} --registry {acr} --pipeline {pipeline} "
                + f"--name {str(abs(hash(target_blob)))} --pipeline-type export --storage-blob {target_blob} -a {image}"
            )
            result = retry_command(cmd, 5)
            assert result == 0
            print(f"Transferred {target_blob}")
        else:
            print(f"Skipped existing {image}")

        backup_exists = BlobClient.from_connection_string(
            conn_string, container_name=container, blob_name=target_blob
        ).exists()
        if backup_exists:
            print(f"Deleting {image}")
            cmd = f"az acr repository delete --name {acr} --image {image} --yes"
            result = retry_command(cmd, 5)
            assert result == 0
