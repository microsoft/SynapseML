# Azure Environment GPU Setup

## Requirements on the connection between HDI Spark cluster and GPU VM

CNTK training using MMLSpark in Azure requires an HDInsight spark
cluster and a GPU virtual machine (VM) connected via a virtual network
(VNet) and that the GPU VM allow SSH connection to itself.  The GPU VM
doesn’t need to be publicly accessible, that is, to have a public IP
address.  Within the VNet, it can be addressed directly by its name and
it can talk to a Spark component service on a node in the cluster such
as the active NameNode RPC endpoint.

See [third-party-notices.txt](third-party-notices.txt) for the original
copyright and license notices of third party software used by MMLSpark.

### Data Center Compatibility

Not all data centers currently have GPU VMs available.  See [this
link](https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/)
to check availability in your data center.

## Connect an HDI cluster and GPU VM via ARM template

MMLSpark provides an Azure Resource Manager (ARM) template to create
such an environment in Azure.  The
[template](https://tongtest.blob.core.windows.net/cntk/azureDeployMainTemplate.json)
has the following parameters to allow you to configure the HDI Spark
cluster and GPU VM:

- `clusterName`: The name of the HDInsight Spark cluster to create
- `clusterLoginUserName`: These credentials can be used to submit jobs
  to the cluster and to log into cluster dashboards
- `clusterLoginPassword`: The password must be at least 10 characters in
  length and must contain at least one digit, one non-alphanumeric
  character, and one upper or lower case letter
- `sshUserName`: These credentials can be used to remotely access the
  cluster
- `sshPassword`: The password must be at least 10 characters in length
  and must contain at least one digit, one non-alphanumeric character,
  and one upper or lower case letter
- `headNodeSize`: The virtual machine size of the head nodes in the
  HDInsight Spark cluster
- `workerNodeCount`: The number of the worker nodes in the HDInsight
  Spark cluster
- `workerNodeSize`: The virtual machine size of the worker nodes in the
  HDInsight Spark cluster
- `gpuVirtualMachineName`: The name of the GPU virtual machine to create
- `gpuVirtualMachineSize`: The size of the GPU virtual machine to create

If you need to further configure the environment (for example, to change
[the class of VM
sizes](https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/)
for HDI cluster nodes), modify the template directly before deployment.
See [this
guide](https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-manager-template-best-practices)
for best practices to create ARM templates.  For the naming rules and
restrictions for Azure resources please refer to [this
article](https://docs.microsoft.com/en-us/azure/architecture/best-practices/naming-conventions).

MMLSpark provides three ARM templates:

- [azureDeployMainTemplate.json](https://tongtest.blob.core.windows.net/cntk/azureDeployMainTemplate.json):
  The main template which references the following two child templates

- [sparkClusterInVnetTemplate.json](https://tongtest.blob.core.windows.net/cntk/sparkClusterInVnetTemplate.json):
  The template for creating an HDI Spark cluster within a VNet and with
  MMLSpark and its dependencies

- [gpuVmExistingVNetTempate2.json](https://tongtest.blob.core.windows.net/cntk/gpuVmExistingVNetTempate2.json):
  The template for creating a GPU VM within an existing VNet and with
  CNTK and other dependencies MMLSpark needs for training on GPUs.

Please note that the child templates can be deployed independently.  For
example, to deploy only an HDI cluster, you may use the aforementioned
[sparkClusterInVnetTemplate.json](https://tongtest.blob.core.windows.net/cntk/sparkClusterInVnetTemplate.json).

There are three ways to deploy an ARM template.

### 1. Deploy an ARM template within [Azure Portal](https://ms.portal.azure.com/)

An ARM template can be opened within Azure Portal via the following REST API:

    https://portal.azure.com/#create/Microsoft.Template/uri/<ARM template URI>

The URI can be one for either an Azure Blob or a GitHub file.  For example,

    https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Ftongtest.blob.core.windows.net%2Fcntk%2FazureDeployMainTemplate.json

Please note that the template URI is URL encoded.  Clicking on the above
link will open the template in Azure Portal.  You can click on the “Edit
template” button as shown in the screenshot below to view and edit the
template if needed.

![ARM template in Portal](http://image.ibb.co/gZ6iiF/arm_Template_In_Portal.png)

### 2. Deploy an ARM template with the [MMLSpark Azure PowerShell](https://tongtest.blob.core.windows.net/cntk/powershelldeploy.ps1)

MMLSpark provides a [PowerShell
script](https://tongtest.blob.core.windows.net/cntk/powershelldeploy.ps1)
to deploy an ARM template (such as
[azureDeployMainTemplate.json](https://tongtest.blob.core.windows.net/cntk/azureDeployMainTemplate.json))
along with a parameter file (such as
[azureDeployParameters.json](https://tongtest.blob.core.windows.net/cntk/azureDeployMainTemplate.json)).

The script take the following parameters:
- `subscriptionId`: The GUID that identifies your subscription (e.g.,
  `01234567-89ab-cdef-0123-456789abcdef`)
- `resourceGroupName`: If the name doesn’t exist a new Resource Group
  will be created
- `resourceGroupLocation`: The location of the Resource Group
  (e.g., `East US`)
- `deploymentName`: The name for this deployment
- `templateFilePath`: The path to the ARM template file.  By default, it
  is set to `azureDeployMainTemplate.json`
- `parametersFilePath`: The path to the parameter file.  By default, it
  is set to `azureDeployParameters.json`

If no parameters are specified in the command line, the scripts will
prompt you for the required ones (subscriptionId, resourceGroupName, and
deploymentName) and will use the default values for the rest.  If
needed, install the Azure PowerShell using the instructions found in the
[Azure PowerShell
guide](https://docs.microsoft.com/powershell/azureps-cmdlets-docs/).

### 3. Deploy an ARM template with [MMLSpark Azure CLI 2.0](https://tongtest.blob.core.windows.net/cntk/bashshelldeploy.sh)

MMLSpark provides an Azure CLI 2.0 script
([bashshelldeploy.sh](https://tongtest.blob.core.windows.net/cntk/bashshelldeploy.sh))
to deploy an ARM template (such as
[azureDeployMainTemplate.json](https://tongtest.blob.core.windows.net/cntk/azureDeployMainTemplate.json))
along with a parameter file (such as
[azureDeployParameters.json](https://tongtest.blob.core.windows.net/cntk/azureDeployMainTemplate.json)).

The script takes the same set of parameters as in the PowerShell:

    ./bashshelldeploy.sh -i <subscriptionId> -g <resourceGroupName> \
                         -n <deploymentName> -l <resourceGroupLocation> \
                         -t <templateFilePath> -p <parametersFilePath>

If no parameters are specified in the command line, the scripts will
prompt for the required ones and uses the default values for the rest.
If needed, install the Azure CLI 2.0 using the instruction found in
[Install Azure CLI
2.0](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli).

*Apache®, Apache Spark, and Spark® are either registered trademarks or
trademarks of the Apache Software Foundation in the United States and/or
other countries.*
