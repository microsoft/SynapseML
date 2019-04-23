# Azure Environment GPU Setup for MMLSpark

## Requirements

CNTK training using MMLSpark in Azure requires an HDInsight Spark
cluster and at least one GPU virtual machine (VM).  The GPU VM(s) should
be reachable via SSH from the cluster, but no public SSH access (or even
a public IP address) is required, and the cluster's NameNode should be
accessible from the GPU machines via the HDFS RPC.  As an example, the
GPU VMs can be on a private Azure virtual network (VNet), and within
this VNet, they can be addressed directly by their names and access the
Spark clsuter nodes (e.g., use the active NameNode RPC endpoint).

(See the original [copyright and license
notices](third-party-notices.txt) of third party software used by
MMLSpark.)

### Data Center Compatibility

Currently, not all data centers have GPU VMs available.  See [the Linux VMs
page](https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/)
to check availability in your data center.

## Connect an HDI cluster and GPU VMs via the ARM template

MMLSpark provides an Azure Resource Manager (ARM) template to create a
default setup that includes an HDInsight cluster and a GPU machine for
training.  The template can be found here:
<https://mmlspark.azureedge.net/buildartifacts/0.17/deploy-main-template.json>.

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

There are actually two additional templates that are used from this main template:
- [`spark-cluster-template.json`](https://mmlspark.azureedge.net/buildartifacts/0.13/spark-cluster-template.json):
  A template for creating an HDI Spark cluster within a VNet, including
  MMLSpark and its dependencies.  (This template installs MMLSpark using
  the HDI script action:
  [`install-mmlspark.sh`](https://mmlspark.azureedge.net/buildartifacts/0.13/install-mmlspark.sh).)
- [`gpu-vm-template.json`](https://mmlspark.azureedge.net/buildartifacts/0.13/gpu-vm-template.json):
  A template for creating a GPU VM within an existing VNet, including
  CNTK and other dependencies that MMLSpark needs for GPU training.
  (This is done via a script action that runs
  [`gpu-setup.sh`](https://mmlspark.azureedge.net/buildartifacts/0.13/gpu-setup.sh).)

Note that these child templates can also be deployed independently, if
you don't need both parts of the installation.  Particularly, to scale
out an existing environment, a new GPU VM can be added to it using the
GPU VM setup template at experimentation time.

## Deploying an ARM template

### 1. Deploy an ARM template within the [Azure Portal](https://ms.portal.azure.com/)

[Click here to open the above main
template](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fmmlspark.azureedge.net%2Fbuildartifacts%2F0.17%2Fdeploy-main-template.json)
in the Azure portal.

(If needed, you click the **Edit template** button to view and edit the
template.)

This link is using the Azure Portal API:

    https://portal.azure.com/#create/Microsoft.Template/uri/〈ARM-template-URI〉

where the template URI is percent-encoded.

### 2. Deploy an ARM template with MMLSpark Azure CLI 2.0

We also provide a convenient shell script to create a deployment on the
command line:

* Download the [shell
  script](https://mmlspark.azureedge.net/buildartifacts/0.13/deploy-arm.sh)
  and make a local copy of it

* Create a JSON parameter file by downloading [this template
  file](https://mmlspark.azureedge.net/buildartifacts/0.13/deploy-parameters.template)
  and modify it according to your specification.

You can now run the script — it takes the following arguments:
- `subscriptionId`: The GUID that identifies your subscription (e.g.,
  `01234567-89ab-cdef-0123-456789abcdef`), defaults to setting in your
  `az` environment.
- `resourceGroupName` (required): If the name doesn’t exist a new
  resource group will be created.
- `resourceGroupLocation`: The location of the resource group (e.g.,
  `East US`), note that this is required if creating a new resource
  group.
- `deploymentName`: The name for this deployment.
- `templateLocation`: The URL of an ARM template file.  By default, it
  is set to the above main template.
- `parametersFilePath`: The path to the parameter file, which you have
  created.

Run the script with a `-h` or `--help` to see the flags that are used to
set these arguments:

    ./deploy-arm.sh -h

If no flags are specified on the command line, the script will prompt
you for all needed values.

> Note that the script uses the Azure CLI 2.0, see the
> [Azure CLI Installation Guide](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)
> if you need to install it.

### 3. Deploy an ARM template with the MMLSpark Azure PowerShell

MMLSpark also provides a [PowerShell
script](https://mmlspark.azureedge.net/buildartifacts/0.13/deploy-arm.ps1)
to deploy ARM templates, similar to the above bash script.  Run it with
`-?` to see the usage instructions (or use `get-help`).  If needed,
install the Azure PowerShell cmdlets using the instructions in the
[Azure PowerShell
Guide](https://docs.microsoft.com/powershell/azureps-cmdlets-docs/).

## Set up passwordless SSH login to the GPU VM

CNTK training using MMLSpark requires passwordless SSH access from the
HDInsight Spark cluster to the GPU VM(s).  The setup of this connection
can be done by a [helper shell script](../tools/hdi/setup-ssh-access.sh)
which you need to run once for each GPU VM.  This script takes two
parameters:

    ./setup-ssh-access.sh <vm-name> [<username>]

The `<username>` parameter is optional, defaulting to the cluster's username
(if they share the same ssh user name).

## Shutdown GPU VMs to save money

Azure will stop billing if a VM is in a "Stopped (**Deallocated**)" state,
which is different from the "Stopped" state.  So make sure it is *Deallocated*
to avoid billing.  In the Azure Portal, clicking the "Stop" button will put the
VM into a "Stopped (Deallocated)" state and clicking the "Start" button brings
it back up.  See "[Properly Shutdown Azure VM to Save
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
