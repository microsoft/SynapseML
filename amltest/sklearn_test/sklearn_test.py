from azureml.core import Workspace
from adal import AuthenticationContext
import json
import requests
from azureml.core.authentication import ServicePrincipalAuthentication
import time

client_id = "caddbe75-030e-4b57-bae3-b39e32051009"
client_secret = "b77cead6-f87e-45c7-a5da-9d23a19fd9e2"
resource_url = "https://login.microsoftonline.com"
tenant_id = "72f988bf-86f1-41af-91ab-2d7cd011db47"

authority = "{}/{}".format(resource_url, tenant_id)
auth_context = AuthenticationContext(authority)
token_response = auth_context.acquire_token_with_client_credentials("https://management.azure.com/", client_id, client_secret)
token = token_response['accessToken']

subid = "ce1dee05-8cf6-4ad6-990a-9c80868800ba"
region = "eastus"
rg = "extern2020"
ws = "exten-amls"

header = {'Authorization': 'Bearer ' + token}
hosturl = "https://{}.api.azureml.ms/".format(region)

historybase = "history/v1.0/"
resourcebase = "subscriptions/{}/resourceGroups/{}/providers/Microsoft.MachineLearningServices/workspaces/{}/".format(subid,rg,ws)
experiment_name = "new_experiment"
# create_experiment = hosturl + historybase + resourcebase + "experiments/{}".format(experiment_name)
# resp = requests.post(create_experiment, headers=header)
# print("RESP 1:",resp.text)
# print(resp.status_code)
get_experiment = hosturl + historybase + resourcebase + "experiments/{}".format(experiment_name)
resp = requests.get(get_experiment, headers=header)
print(resp.text)
print(resp.status_code)
print()

executionbase = "execution/v1.0/"
resourcebase = "subscriptions/{}/resourceGroups/{}/providers/Microsoft.MachineLearningServices/workspaces/{}/".format(subid,rg,ws)

start_run = hosturl + executionbase + resourcebase + "experiments/{}/startrun".format(experiment_name)

run_files = {"runDefinitionFile": ("definition.json", open("definition.json","rb")), "projectZipFile": ("project.zip", open("project.zip","rb"))}

resp = requests.post(start_run, files=run_files, headers=header)

print("RESP 2:",resp.text)
print(resp.status_code)

run_id = json.loads(resp.text)["runId"]

get_run = hosturl + historybase + resourcebase + "experiments/{}/runs/{}".format(experiment_name,run_id)

status = None

while status not in ["Completed", "Failed", "Cancelled"]:
    time.sleep(5)
    resp = requests.get(get_run, headers=header)
    status = json.loads(resp.text)["status"]
    print(status)

