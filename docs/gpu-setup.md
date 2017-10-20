# Azure Environment GPU Setup for MMLSpark

## Requirements

CNTK training using MMLSpark in Azure requires an HDInsight Spark cluster and a
GPU virtual machine (VM).  The GPU VM should be reachable via SSH from the
cluster, but no public SSH access (or even a public IP address) is required.
As an example, it can be on a private Azure virtual network (VNet), and within
this VNet, it can be addressed directly by its name and access the Spark
clsuter nodes (e.g., use the active NameNode RPC endpoint).

See the original [copyright and license notices](third-party-notices.txt) of
third party software used by MMLSpark.

### Data Center Compatibility

Not all data centers currently have GPU VMs available.  See [the Linux VMs
page](https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/)
to check availability in your data center.

## Connect an HDI cluster and a GPU VM via the ARM template

MMLSpark provides an Azure Resource Manager (ARM) template to create a setup
that includes an HDInsight cluster and/or a GPU machine for training.  The
[template](../tools/deployment/azureDeployMainTemplate.json) has the following
parameters to allow you to configure the HDI Spark cluster and the GPU VM:
- `clusterName`: The name of the HDInsight Spark cluster to create
- `clusterLoginUserName`: These credentials can be used to submit jobs to the
  cluster and to log into cluster dashboards
- `clusterLoginPassword`: The password must be at least 10 characters in length
  and must contain at least one digit, one non-alphanumeric character, and one
  upper or lower case letter
- `sshUserName`: These credentials can be used to remotely access the cluster
- `sshPassword`: The password must be at least 10 characters in length and must
  contain at least one digit, one non-alphanumeric character, and one upper or
  lower case letter
- `headNodeSize`: The virtual machine size of the head nodes in the HDInsight
  cluster
- `workerNodeCount`: The number of the worker nodes in the cluster
- `workerNodeSize`: The virtual machine size of the worker nodes in the cluster
- `gpuVirtualMachineName`: The name of the GPU virtual machine to create
- `gpuVirtualMachineSize`: The size of the GPU virtual machine to create

If you need to further configure the environment (for example, to change [the
class of VM
sizes](https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/)
for HDI cluster nodes), modify the template directly before deployment.  See
also [the guide for best ARM template
practices](https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-manager-template-best-practices).
For the naming rules and restrictions for Azure resources please refer to the
[Naming conventions
article](https://docs.microsoft.com/en-us/azure/architecture/best-practices/naming-conventions).

MMLSpark provides three ARM templates:
- [`azureDeployMainTemplate.json`](../tools/deployment/azureDeployMainTemplate.json):
  This is the main template, references the following two child templates
- [`sparkClusterInVnetTemplate.json`](../tools/deployment/sparkClusterInVnetTemplate.json):
  The template for creating an HDI Spark cluster within a VNet, including
  MMLSpark and its dependencies
- [`gpuVmExistingVNetTemplate.json`](../tools/deployment/gpuVmExistingVNetTemplate.json):
  The template for creating a GPU VM within an existing VNet, including CNTK
  and other dependencies that MMLSpark needs for GPU training.

Note that the last two child templates can also be deployed independently, if
you don't need both parts of the installation.

## Deploying an ARM template

### 1. Deploy an ARM template within the [Azure Portal](https://ms.portal.azure.com/)

An ARM template can be opened within the Azure Portal via the following REST
API:

    https://portal.azure.com/#create/Microsoft.Template/uri/<ARM-template-URI>

The URI can be one for either an *Azure Blob* or a *GitHub file*.  For example,

    https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fmystorage.blob.core.windows.net%2FazureDeployMainTemplate.json

(Note that the URL is percent-encoded.)  Clicking on the above link will
open the template in the Portal.  If needed, click the **Edit template** button
(see screenshot below) to view and edit the template.

![ARM template in Portal](http://image.ibb.co/gZ6iiF/arm_Template_In_Portal.png)

### 2. Deploy an ARM template with the [MMLSpark Azure PowerShell](../tools/deployment/powershelldeploy.ps1)

MMLSpark provides a [PowerShell
script](../tools/deployment/powershelldeploy.ps1) to deploy an ARM template
(such as
[azureDeployMainTemplate.json](../tools/deployment/azureDeployMainTemplate.json))
along with a parameter file (such as
[azureDeployParameters.json](../tools/deployment/azureDeployParameters.json)).

The script take the following parameters:
- `subscriptionId`: The GUID that identifies your subscription (e.g.,
  `01234567-89ab-cdef-0123-456789abcdef`)
- `resourceGroupName`: If the name doesn’t exist a new Resource Group will be
  created
- `resourceGroupLocation`: The location of the Resource Group (e.g., `East US`)
- `deploymentName`: The name for this deployment
- `templateFilePath`: The path to the ARM template file.  By default, it is set
  to `azureDeployMainTemplate.json`
- `parametersFilePath`: The path to the parameter file.  By default, it is set
  to `azureDeployParameters.json`

If no parameters are specified on the command line, the scripts will prompt you
for the required ones (`subscriptionId`, `resourceGroupName`, and
`deploymentName`) and will default values for the rest.  If needed, install the
Azure PowerShell using the instructions found in the [Azure PowerShell
guide](https://docs.microsoft.com/powershell/azureps-cmdlets-docs/).

### 3. Deploy an ARM template with [MMLSpark Azure CLI 2.0](../tools/deployment/bashshelldeploy.sh)

MMLSpark provides an Azure CLI 2.0 script
([`bashshelldeploy.sh`](../tools/deployment/bashshelldeploy.sh)) to deploy an ARM
template (such as
[`azureDeployMainTemplate.json`](../tools/deployment/azureDeployMainTemplate.json))
along with a parameter file (such as
[`azureDeployParameters.json`](../tools/deployment/azureDeployMainTemplate.json)).

The script takes the same set of parameters as the above PowerShell version:

    ./bashshelldeploy.sh -i <subscriptionId> -g <resourceGroupName> \
                         -n <deploymentName> -l <resourceGroupLocation> \
                         -t <templateFilePath> -p <parametersFilePath>

Again, if no parameters are specified on the command line, the script will
prompt for required values and use defaults for the rest.  If needed, install
the Azure CLI 2.0 using the instruction found in [Install Azure CLI
2.0](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli).

## Set up passwordless SSH login to the GPU VM

CNTK training using MMLSpark requires passwordless SSH access from the
HDInsight Spark cluster to the GPU VM.  The setup of this connection is done
through a [script](../tools/hdi/setup-ssh-keys.sh) and it needs to be done
once.  The script takes two parameters:

    ./setup-ssh-keys.sh <vm-name> [<username>]

The `<username>` parameter is optional, defaulting to the cluster's username
(if they share the same ssh user name).

## Shutdown the GPU VM to save money

Azure will stop billing if a VM is in a "Stopped (**Deallocated**)" state,
which is different from the "Stopped" state.  So make sure it is *Deallocated*
to avoid billing.  In the Azure Portal, clicking the "Stop" button will put the
VM into a "Stopped (Deallocated)" state and clicking the "Start" button brings
it VM.  See "[Properly Shutdown Azure VM to Save
Money](https://buildazure.com/2017/03/16/properly-shutdown-azure-vm-to-save-money/)"
for futher details.

Here is an example of switching VM states using the Azure CLI:

    az login
    az account set --subscription 01234567-89ab-cdef-0123-456789abcdef
    az vm deallocate --resource-group MyResourceGroupName --name mygpuvm
    az vm start --resource-group MyResourceGroupName --name mygpuvm

and the equivalent PowerShell lines:

    Login-AzureRmAccount
    Select-AzureRmSubscription -SubscriptionID "01234567-89ab-cdef-0123-456789abcdef"
    Stop-AzureRmVM -ResourceGroupName "MyResourceGroupName" -Name "mygpuvm"
    Start-AzureRmVM -ResourceGroupName "MyResourceGroupName" -Name "mygpuvm"
