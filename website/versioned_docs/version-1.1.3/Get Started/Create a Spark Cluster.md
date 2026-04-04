---
title: Create a Spark Cluster
hide_title: true
status: stable
---
# Setting up your computing platform for SynapseML 

SynapseML is preinstalled on Microsoft Fabric and Synapse Analytics. Follow the instructions to get started with these platforms.

## Microsoft Fabric
[Microsoft Fabric](https://www.microsoft.com/microsoft-fabric/) is an all-in-one analytics solution for enterprises that covers everything from data movement to data science, Real-Time Analytics, and business intelligence. It offers a comprehensive suite of services, including data lake, data engineering, and data integration, all in one place.

SynapseML is preinstalled on Fabric, and this guide will walk you through getting access to fabric.

* [Get a Microsoft Fabric license](https://learn.microsoft.com/fabric/enterprise/licenses) or sign-up for a free [Microsoft Fabric (Preview) trial](https://learn.microsoft.com/fabric/get-started/fabric-trial).
* Sign in to [Microsoft Fabric](https://fabric.microsoft.com/)
* Go to the Data Science experience.
* [Create a new notebook](https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook#create-notebooks) or attach your notebook to a lakehouse. On the left side, select **Add** to add an existing lakehouse or [create a lakehouse](https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook#connect-lakehouses-and-notebooks).

SynapseML is preinstalled on Fabric, but if you want to use another version of SynapseML, follow [this guide on updating SynapseML](https://learn.microsoft.com/en-us/fabric/data-science/install-synapseml).

## Synapse Analytics
[Azure Synapse Analytics](https://azure.microsoft.com/products/synapse-analytics) is an enterprise analytics service that accelerates time to insight across data warehouses and big data systems.

SynapseML is preinstalled on Synapse Analytics. To start with Synapse Analytics, you need:

* A valid Azure subscription - [Create one for free](https://azure.microsoft.com/free/cognitive-services/).
* [Create a Synapse workspace and launch Synapse studio](https://docs.microsoft.com/en-us/azure/synapse-analytics/get-started-create-workspace)
* [Create a serverless Apache Spark pool](https://docs.microsoft.com/en-us/azure/synapse-analytics/get-started-analyze-spark#create-a-serverless-apache-spark-pool)
* Once Synapse Studio has launched, select **Develop**. Then, select the **"+"** icon to add a new resource. From there, select **Notebook**. A new notebook is created and opened. Alternatively, you can select **Import** to upload your notebook.

SynapseML is preinstalled on Azure Synapse Analytics, but if you want to use another version of SynapseML, follow [this guide on updating SynapseML](../Install%20SynapseML).
