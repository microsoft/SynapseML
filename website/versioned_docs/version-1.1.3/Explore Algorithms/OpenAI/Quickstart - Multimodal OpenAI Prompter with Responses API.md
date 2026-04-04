---
title: Quickstart - Multimodal OpenAI Prompter with Responses API
hide_title: true
status: stable
---
# Quickstart: Multimodal OpenAI Prompter with Responses API

Use OpenAI's Responses API to enrich your data with multimodal content such as PDFs, images or documentations. This quickstart shows how to use multimodal content from a Spark DataFrame by pointing `OpenAIPrompt` to columns that contain file paths or URLs.

## Prerequisites
- An active Spark session (`spark`)
- Access to an OpenAI resource with the Responses API enabled. Deployment should be a multimodal model.
- File you want to talk to.

## Fill in service information

Next, edit the cell in the notebook to point to your service. In particular set the `service_name`, `deployment_name`, `location`, and `key` variables to match them to your OpenAI service:


```python
from synapse.ml.core.platform import find_secret

# Fill in the following lines with your service information
# Learn more about selecting which embedding model to choose: https://openai.com/blog/new-and-improved-embedding-model
service_name = "synapseml-openai-2"
deployment_name = "gpt-4.1"
api_version = (
    "2025-04-01-preview"  # Responses API is only supported in this version and later
)

key = find_secret(
    secret_name="openai-api-key-2", keyvault="mmlspark-build-keys"
)  # please replace this line with your key as a string

assert key is not None and service_name is not None
```

## Build a multimodal dataset
Each row combines a natural-language question with a reference to a file. Attachments can be local paths or remote URLs(Currently only support http(s)).


```python
qa_df = spark.createDataFrame(
    [
        (
            "What's in the image?",
            "https://mmlspark.blob.core.windows.net/datasets/OCR/test2.png",
        ),
        (
            "Summarize this document.",
            "https://mmlspark.blob.core.windows.net/datasets/OCR/paper.pdf",
        ),
        (
            "What are the related works to SynapseML?",
            "https://mmlspark.blob.core.windows.net/publicwasb/Overview.md",
        ),
    ]
).toDF("questions", "urls")
```

## Configure `OpenAIPrompt` for file-aware responses
Any column declared as `path` is treated as an attachment. The prompt template automatically replaces `{file_path}` with a notice that the model will receive the referenced file in the message payload.


```python
from synapse.ml.services.openai import OpenAIPrompt

prompt_template = "{questions}: {urls}"

prompter = (
    OpenAIPrompt()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setCustomServiceName(service_name)
    .setApiVersion(api_version)
    .setApiType(
        "responses"
    )  # We need to use the Responses API, instead of Chat Completions API.
    .setPromptTemplate(prompt_template)
    .setColumnTypes(
        {"urls": "path"}
    )  # Configure the column type to 'path' for the url column
    .setErrorCol("error")
    .setOutputCol("outputCol")
)
```

## Generate multimodal responses
See how the model react to your various file data.


```python
prompter.transform(qa_df).show(truncate=False)
```
