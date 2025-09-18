---
title: Quickstart - Chat Completion with Azure AI Foundry Model
hide_title: true
status: stable
---
# Chat Completion with Azure AI Foundry model

[Azure AI Foundry](https://learn.microsoft.com/en-us/azure/ai-foundry/what-is-azure-ai-foundry) enables the execution of a wide range of natural language tasks using the Completion API. Its integration with SynapseML simplifies leveraging the Apache Spark distributed computing framework to handle large volumes of prompts across various models, including those from Deepseek, Meta, Microsoft, xAI, and others. For a full list of [supported models](https://learn.microsoft.com/en-us/azure/ai-foundry/foundry-models/concepts/models), refer to Azure AI Foundry documentation.
Note: To use OpenAI models, integration is available through the OpenAIChatCompletion class. Refer to the relevant documentation for details on [using OpenAI models](https://microsoft.github.io/SynapseML/docs/Explore%20Algorithms/OpenAI/).


## Prerequisites
The key prerequisites for this quickstart include 

- An [Azure subscription](https://azure.microsoft.com/en-us/pricing/purchase-options/azure-account)

- A working Azure AI Foundry project resource and a model deployed
    * Sign in to the [Azure AI foundry portal](https://ai.azure.com/)
    * Select a chat completion model. We use Phi-4-mini-instruct model as an example. 

        <img src="https://mmlspark.blob.core.windows.net/graphics/phi_4.png" width="500" />
    
    * On the model details page, select Use this model.
    * Fill in a name to use for your project and select Create.

- An Apache Spark cluster with SynapseML installed.

# Fill in service information
Next, edit the cell in the notebook to point to your service. 

In particular set the service_name, api_version to match them to your AI Foundry model.
To get your service_name, api_version and api_key, Select My Asset, Find Target URI. API version is also in target URI.

<img src="https://mmlspark.blob.core.windows.net/graphics/phi_4_2.png" width="500" />




```python
from synapse.ml.core.platform import find_secret

# Fill in the following lines with your service information
service_name = "synapseml-ai-foundry-resource"
api_verion = "2024-05-01-preview"
model = "Phi-4-mini-instruct"
api_key = find_secret(
    secret_name="synapseml-ai-foundry-resource-key", keyvault="mmlspark-build-keys"
)  # please replace this line with your key as a string

assert api_key is not None and service_name is not None
```

## Chat Completion

Models such as Phi-4 and llama are capable of understanding chats instead of single prompts. The `AIFoundryChatCompletion` transformer exposes this functionality at scale.


```python
from synapse.ml.services.aifoundry import AIFoundryChatCompletion
from pyspark.sql import Row
from pyspark.sql.types import *


def make_message(role, content):
    return Row(role=role, content=content, name=role)


chat_df = spark.createDataFrame(
    [
        (
            [
                make_message(
                    "system", "You are an AI chatbot with red as your favorite color"
                ),
                make_message("user", "Whats your favorite color"),
            ],
        ),
        (
            [
                make_message("system", "You are very excited"),
                make_message("user", "How are you today"),
            ],
        ),
    ]
).toDF("messages")


chat_completion = (
    AIFoundryChatCompletion()
    .setSubscriptionKey(api_key)
    .setCustomServiceName(service_name)
    .setModel(model)
    .setApiVersion("2024-05-01-preview")
    .setMessagesCol("messages")
    .setErrorCol("error")
    .setOutputCol("chat_completions")
)

display(
    chat_completion.transform(chat_df).select(
        "messages", "chat_completions.choices.message.content"
    )
)
```
