---
title: Set up Cognitive Services
hide_title: true
status: stable
---
# Setting up Cognitive Services and Azure OpenAI resources for SynapseML 

In order to use SynapseML's OpenAI or Cognitive Services features, specific Azure resources are required. This documentation walks you through the process of setting up these resources and acquiring the necessary credentials.

First, create an Azure subscription to create resources.
* A valid Azure subscription - [Create one for free](https://azure.microsoft.com/free/cognitive-services/).

## Azure OpenAI

The [Azure OpenAI service](https://azure.microsoft.com/products/cognitive-services/openai-service/) can be used to solve a large number of natural language tasks through prompting the completion API. To make it easier to scale your prompting workflows from a few examples to large datasets of examples, we have integrated the Azure OpenAI service with the distributed machine learning library SynapseML. This integration makes it easy to use the Apache Spark distributed computing framework to process millions of prompts with the OpenAI service.

To set up your Azure OpenAI Resource for SynapseML usage you need to: 
* [Apply for access to Azure OpenAI](https://aka.ms/oai/access) if you do not already have access. 
* [Create an Azure OpenAI resource](https://docs.microsoft.com/azure/cognitive-services/openai/how-to/create-resource?pivots=web-portal#create-a-resource) 
* Get your Azure OpenAI resource's key. After your resource is successfully deployed, select **Next Steps** > **Go to resource**. Once at the resource, you can get the key from **Resource Management** > **Keys and Endpoint**. Copy the key and paste it into the notebook. Store keys securely and do not share them. 

## Cognitive Services

To set up [Cognitive Services](https://azure.microsoft.com/products/cognitive-services/) for use with SynapseML you first need to:
* [Assign yourself the Cognitive Services Contributor role](https://learn.microsoft.com/azure/role-based-access-control/role-assignments-steps) to agree to the responsible AI terms and create a resource. 
* [Create an Azure Cognitive multi-service (Decision, Language, Speech, Vision) resource](https://portal.azure.com/#create/Microsoft.CognitiveServicesAllInOne). Alternatively, you can follow the steps to [create Single-service resource](https://learn.microsoft.com/en-us/azure/cognitive-services/cognitive-services-apis-create-account?tabs=decision%2Canomaly-detector%2Clanguage-service%2Ccomputer-vision%2Cwindows#create-a-new-azure-cognitive-services-resource). 
* Get your Cognitive Service resource's key. After your resource is successfully deployed, select **Next Steps** > **Go to resource**. Once at the resource, you can get the key from **Resource Management** > **Keys and Endpoint**. Copy the key and paste it into the notebook. Store keys securely and do not share them. 
