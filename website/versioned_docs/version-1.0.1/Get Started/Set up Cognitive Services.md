---
title: Set up Cognitive Services
hide_title: true
status: stable
---
# Setting up Azure AI Services and Azure OpenAI resources for SynapseML 

In order to use SynapseML's OpenAI or Azure AI Services features, specific Azure resources are required. This documentation walks you through the process of setting up these resources and acquiring the necessary credentials.

First, create an Azure subscription to create resources.
* A valid Azure subscription - [Create one for free](https://azure.microsoft.com/free/cognitive-services/).

## Azure OpenAI

The [Azure OpenAI service](https://azure.microsoft.com/products/cognitive-services/openai-service/) can be used to solve a large number of natural language tasks through prompting the completion API. To make it easier to scale your prompting workflows from a few examples to large datasets of examples, we have integrated the Azure OpenAI service with the distributed machine learning library SynapseML. This integration makes it easy to use the Apache Spark distributed computing framework to process millions of prompts with the OpenAI service.

To set up your Azure OpenAI Resource for SynapseML usage you need to: 
* [Apply for access to Azure OpenAI](https://aka.ms/oai/access) if you do not already have access. 
* [Create an Azure OpenAI resource](https://docs.microsoft.com/azure/cognitive-services/openai/how-to/create-resource?pivots=web-portal#create-a-resource) 
* Get your Azure OpenAI resource's key. After your resource is successfully deployed, select **Next Steps** > **Go to resource**. Once at the resource, you can get the key from **Resource Management** > **Keys and Endpoint**. Copy the key and paste it into the notebook. Store keys securely and do not share them. 

## Azure AI Services

To set up [Azure AI Services](https://azure.microsoft.com/en-us/products/ai-services) for use with SynapseML you first need to:
* [Assign yourself the Azure AI Services Contributor role](https://learn.microsoft.com/azure/role-based-access-control/role-assignments-steps) to agree to the responsible AI terms and create a resource. 
* [Create Azure AI service (Decision, Language, Speech, Vision) resource](https://ms.portal.azure.com/#create/Microsoft.CognitiveServicesAllInOne). You can follow the steps at [Create a multi-service resource for Azure AI services](https://learn.microsoft.com/en-us/azure/ai-services/multi-service-resource?tabs=windows&pivots=azportal#create-a-new-azure-cognitive-services-resource). 
* Get your Azure AI Services resource's key. After your resource is successfully deployed, select **Next Steps** > **Go to resource**. Once at the resource, you can get the key from **Resource Management** > **Keys and Endpoint**. Copy the key and paste it into the notebook. Store keys securely and do not share them. 
