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

Currently, not all data centers have GPU VMs available.  See [the Linux
VMs page](https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/)
to check availability in your data center.

## Connect an HDI cluster and a GPU VM via the ARM template

MMLSpark provides an Azure Resource Manager (ARM) template to create a setup
that includes an HDInsight cluster and/or a GPU machine for training.  The
template can be found here:
<https://mmlspark.azureedge.net/buildartifacts/0.9/deploy-main-template.json>.

It has the following parameters that configure the HDI Spark cluster and
the associated GPU VM:
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

There are actually three templates that are used for deployment:
- [`deploy-main-template.json`](https://mmlspark.azureedge.net/buildartifacts/0.9/deploy-main-template.json):
  This is the main template.  It referencs the following two child
  templates — these are relative references so they are expected to be
  found in the same base URL.
- [`spark-cluster-template.json`](https://mmlspark.azureedge.net/buildartifacts/0.9/spark-cluster-template.json):
  A template for creating an HDI Spark cluster within a VNet, including
  MMLSpark and its dependencies.  (This template installs MMLSpark using
  the HDI script action:
  [`install-mmlspark.sh`](https://mmlspark.azureedge.net/buildartifacts/0.9/install-mmlspark.sh).)
- [`gpu-vm-template.json`](https://mmlspark.azureedge.net/buildartifacts/0.9/gpu-vm-template.json):
  A template for creating a GPU VM within an existing VNet, including
  CNTK and other dependencies that MMLSpark needs for GPU training.
  (This is done via a script action that runs
  [`gpu-setup.sh`](https://mmlspark.azureedge.net/buildartifacts/0.9/gpu-setup.sh).)

Note that the last two child templates can also be deployed independently, if
you don't need both parts of the installation.

## Deploying an ARM template

### 1. Deploy an ARM template within the [Azure Portal](https://ms.portal.azure.com/)

An ARM template can be opened within the Azure Portal via the following REST
API:

    https://portal.azure.com/#create/Microsoft.Template/uri/<ARM-template-URI>

The URI can be one for either an *Azure Blob* or a *GitHub file*.  For example,

    https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fmystorage.blob.core.windows.net%2Fdeploy-main-template.json

(Note that the URL is percent-encoded.)  Clicking on the above link will
open the template in the Portal.  If needed, click the **Edit template** button
(see screenshot below) to view and edit the template.

![ARM template in Portal](http://image.ibb.co/gZ6iiF/arm_Template_In_Portal.png)

### 2. Deploy an ARM template with [MMLSpark Azure CLI 2.0](https://mmlspark.azureedge.net/buildartifacts/0.9/deploy-arm.sh)

MMLSpark provides an Azure CLI 2.0 script
([`deploy-arm.sh`](../tools/deployment/deploy-arm.sh)) to deploy an ARM
template (such as
[`deploy-main-template.json`](https://mmlspark.azureedge.net/buildartifacts/0.9/deploy-main-template.json))
along with a parameter file (see
[deploy-parameters.template](../tools/deployment/deploy-parameters.template)
for a template of such a file).

> Note that you cannot use the
> [template file](../tools/deployment/deploy-main-template.json) from
> the source tree, since it requires additional resources that are
> created by the build (specifically, a working version of
> [`install-mmlspark.sh`](../tools/hdi/install-mmlspark.sh)).

The script take the following arguments:
- `subscriptionId`: The GUID that identifies your subscription (e.g.,
  `01234567-89ab-cdef-0123-456789abcdef`), defaults to setting in your
  `az` environment.
- `resourceGroupName` (required): If the name doesn’t exist a new
  resource group will be created.
- `resourceGroupLocation`: The location of the resource group (e.g.,
  `East US`), note that this is required if creating a new resource
  group.
- `deploymentName`: The name for this deployment.
- `templateLocation`: The URL of an ARM template file, or the path to
  one.  By default, it is set to `deploy-main-template.json` in the same
  directory, but note that this will normally not work without the rest
  of the required resources.
- `parametersFilePath`: The path to the parameter file, which you need
  to create.  Use `deploy-parameters.template` as a template for
  creating a parameters file.

Run the script with a `-h` or `--help` to see the flags that are used to
set these arguments:

    ./deploy-arm.sh -h

If no flags are specified on the command line, the script will prompt
you for all values.  If needed, install the Azure CLI 2.0 using the
instruction found in the [Azure CLI Installation
Guide](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli).

### 3. Deploy an ARM template with the [MMLSpark Azure PowerShell](https://mmlspark.azureedge.net/buildartifacts/0.9/deploy-arm.ps1)

MMLSpark also provides a [PowerShell
script](https://mmlspark.azureedge.net/buildartifacts/0.9/deploy-arm.ps1)
to deploy ARM templates, similar to the above bash script, run it with
`-?` to see the usage instructions (or use `get-help`).  If needed,
install the Azure PowerShell cmdlets using the instructions in the
[Azure PowerShell
Guide](https://docs.microsoft.com/powershell/azureps-cmdlets-docs/).

## Set up passwordless SSH login to the GPU VM

CNTK training using MMLSpark requires passwordless SSH access from the
HDInsight Spark cluster to the GPU VM.  The setup of this connection can
be done by a [helper shell script](../tools/hdi/setup-ssh-access.sh)
which you need to run once.  This script takes two parameters:

    ./setup-ssh-access.sh <vm-name> [<username>]

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
