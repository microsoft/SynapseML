import os
import json
from azure.storage.blob import BlobClient
import sys
import subprocess
from tqdm import tqdm

acr = "mmlsparkmcr"
container = "acrbackup"
rg = "marhamil-mmlspark"
pipeline = "mmlsparkacrexport3"

conn_string = sys.argv[1]

repos = json.loads(os.popen(
    'az acr repository list -n {}'.format(acr)).read())
for repo in repos:
    tags = json.loads(os.popen(
        'az acr repository show-tags -n {} --repository {} --orderby time_desc'.format(acr, repo)).read())

    for tag in tqdm(tags):
        target_blob = repo + "/" + tag + ".tar"
        image = repo + ":" + tag

        backup_exists = BlobClient.from_connection_string(
            conn_string, container_name=container, blob_name=target_blob).exists()
        if not backup_exists:
            subprocess.run(["sudo", "az", "acr", "pipeline-run", "create", "--resource-group", rg,
                            "--registry", acr, "--pipeline", pipeline, "--name", str(abs(hash(target_blob))),
                            "--pipeline-type", "export", "--storage-blob", target_blob, "-a", image])
            print("Transferred {}".format(target_blob))
        else:
            print("Skipped existing {}".format(image))

        backup_exists = BlobClient.from_connection_string(
            conn_string, container_name=container, blob_name=target_blob).exists()
        if backup_exists:
            print("Deleting {}".format(image))
            result = os.system("az acr repository delete --name {} --image {} --yes".format(acr, image))
            assert result == 0
