---
title: OpenAI
hide_title: true
status: stable
---
# Azure OpenAI for big data

The Azure OpenAI service can be used to solve a large number of natural language tasks through prompting the completion API. To make it easier to scale your prompting workflows from a few examples to large datasets of examples, we have integrated the Azure OpenAI service with the distributed machine learning library [SynapseML](https://www.microsoft.com/en-us/research/blog/synapseml-a-simple-multilingual-and-massively-parallel-machine-learning-library/). This integration makes it easy to use the [Apache Spark](https://spark.apache.org/) distributed computing framework to process millions of prompts with the OpenAI service. This tutorial shows how to apply large language models at a distributed scale using Azure OpenAI. 

## Prerequisites

The key prerequisites for this quickstart include a working Azure OpenAI resource, and an Apache Spark cluster with SynapseML installed. We suggest creating a Synapse workspace, but an Azure Databricks, HDInsight, or Spark on Kubernetes, or even a python environment with the `pyspark` package will work. 

1. An Azure OpenAI resource – request access [here](https://customervoice.microsoft.com/Pages/ResponsePage.aspx?id=v4j5cvGGr0GRqy180BHbR7en2Ais5pxKtso_Pz4b1_xUOFA5Qk1UWDRBMjg0WFhPMkIzTzhKQ1dWNyQlQCN0PWcu) before [creating a resource](https://docs.microsoft.com/en-us/azure/cognitive-services/openai/how-to/create-resource?pivots=web-portal#create-a-resource)
1. [Create a Synapse workspace](https://docs.microsoft.com/en-us/azure/synapse-analytics/get-started-create-workspace)
1. [Create a serverless Apache Spark pool](https://docs.microsoft.com/en-us/azure/synapse-analytics/get-started-analyze-spark#create-a-serverless-apache-spark-pool)


## Import this guide as a notebook

The next step is to add this code into your Spark cluster. You can either create a notebook in your Spark platform and copy the code into this notebook to run the demo. Or download the notebook and import it into Synapse Analytics

-	[Download this demo as a notebook](https://github.com/microsoft/SynapseML/blob/master/docs/Explore%20Algorithms/OpenAI/OpenAI.ipynb) (select **Raw**, then save the file)
-	Import the notebook. 
    * If you are using Synapse Analytics [into the Synapse Workspace](https://docs.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-development-using-notebooks#create-a-notebook) 
    * If your are using Databricks [import into the Databricks Workspace](https://docs.microsoft.com/en-us/azure/databricks/notebooks/notebooks-manage#create-a-notebook). 
    * If you are using Fabric [import into the Fabric Workspace](https://learn.microsoft.com/en-us/fabric/data-engineering/how-to-use-notebook)
-   Install SynapseML on your cluster. See the installation instructions for Synapse at the bottom of [the SynapseML website](https://microsoft.github.io/SynapseML/). 
    * If you are using Fabric, please check [Installation Guide](https://learn.microsoft.com/en-us/fabric/data-science/install-synapseml). This requires pasting an extra cell at the top of the notebook you imported. 
-  	Connect your notebook to a cluster and follow along, editing and running the cells.

## Fill in service information

Next, edit the cell in the notebook to point to your service. In particular set the `service_name`, `deployment_name`, `location`, and `key` variables to match them to your OpenAI service:


```python
from synapse.ml.core.platform import find_secret

# Fill in the following lines with your service information
# Learn more about selecting which embedding model to choose: https://openai.com/blog/new-and-improved-embedding-model
service_name = "synapseml-openai-2"
deployment_name = "gpt-4.1-mini"
deployment_name_embeddings = "text-embedding-ada-002"

key = find_secret(
    secret_name="openai-api-key-2", keyvault="mmlspark-build-keys"
)  # please replace this line with your key as a string

assert key is not None and service_name is not None
```

## Create a dataset of prompts

Next, create a dataframe consisting of a series of rows, with one prompt per row. 

You can also load data directly from ADLS or other databases. For more information on loading and preparing Spark dataframes, see the [Apache Spark data loading guide](https://spark.apache.org/docs/latest/sql-data-sources.html).


```python
# spark session is assumed to be created in the environment already such as in Fabric notebooks with Spark environment
df = spark.createDataFrame(
    [
        ("Hello my name is",),
        ("The best code is code thats",),
        ("SynapseML is ",),
    ]
).toDF("prompt")
```

## More Usage Examples

### Generating Text Embeddings

In addition to completing text, we can also embed text for use in downstream algorithms or vector retrieval architectures. Creating embeddings allows you to search and retrieve documents from large collections and can be used when prompt engineering isn't sufficient for the task.

For more information on using `OpenAIEmbedding` see our [embedding guide](./Quickstart%20-%20OpenAI%20Embedding).


```python
from synapse.ml.services.openai import OpenAIEmbedding

embedding = (
    OpenAIEmbedding()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name_embeddings)
    .setCustomServiceName(service_name)
    .setTextCol("prompt")
    .setErrorCol("error")
    .setOutputCol("embeddings")
)

display(embedding.transform(df).show(truncate=False))
```

### Chat Completion

Models such as ChatGPT and GPT-4 are capable of understanding chats instead of single prompts. The `OpenAIChatCompletion` transformer exposes this functionality at scale.


```python
from synapse.ml.services.openai import OpenAIChatCompletion
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
    OpenAIChatCompletion()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setCustomServiceName(service_name)
    .setMessagesCol("messages")
    .setOutputCol("chat_completions")
    .setErrorCol("chat_completions_error")
)

display(
    chat_completion.transform(chat_df)
    .select("messages", "chat_completions.choices.message.content")
    .show(truncate=False)
)
```

### Chat Completion - Advanced Parameters for Reproducible Outputs

SynapseML now supports additional parameters for enhanced control over OpenAI model behavior for reproducible outputs:

- **`temperature`**: Reduces randomness. OpenAI models accept float temperature value between [0, 2]. Set to 0 for best reproducibility.
- **`top_p`**: Controls nucleus sampling as an alternative to temperature. OpenAI models accept float top_p value between [0, 1]. Set close to 0 for best reproducibility.
- **`seed`**: Enables deterministic sampling for reproducible results. Set to any constant int value.


These parameters can be set globally using `OpenAIDefaults` or on individual transformer instances.


```python
from synapse.ml.services.openai import OpenAIChatCompletion
from synapse.ml.services.openai.OpenAIDefaults import OpenAIDefaults

# Set global defaults including new parameters
defaults = OpenAIDefaults()
defaults.set_deployment_name(deployment_name)
defaults.set_subscription_key(key)
defaults.set_URL(f"https://{service_name}.openai.azure.com/")
defaults.set_temperature(0)
defaults.set_top_p(0.1)
defaults.set_seed(42)

chat_completion = (
    OpenAIChatCompletion()
    .setMessagesCol("messages")
    .setOutputCol("chat_completions")
    .setErrorCol("chat_completions_error")
)

display(
    chat_completion.transform(chat_df)
    .select("messages", "chat_completions.choices.message.content")
    .show(truncate=False)
)
```

## (Legacy) Create the OpenAICompletion Apache Spark Client

To apply the OpenAI Completion service to your dataframe you created, create an OpenAICompletion object, which serves as a distributed client. Parameters of the service can be set either with a single value, or by a column of the dataframe with the appropriate setters on the `OpenAICompletion` object. Here we're setting `maxTokens` to 200. A token is around four characters, and this limit applies to the sum of the prompt and the result. We're also setting the `promptCol` parameter with the name of the prompt column in the dataframe.


```python
from synapse.ml.services.openai import OpenAICompletion

completion = (
    OpenAICompletion()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setCustomServiceName(service_name)
    .setMaxTokens(200)
    .setPromptCol("prompt")
    .setErrorCol("error")
    .setOutputCol("completions")
)
```

## (Legacy) Transform the dataframe with the OpenAICompletion Client

After creating the dataframe and the completion client, you can transform your input dataset and add a column called `completions` with all of the information the service adds. Select just the text for simplicity.


```python
from pyspark.sql.functions import col

completed_df = completion.transform(df).cache()
display(
    completed_df.select(
        col("prompt"),
        col("error"),
        col("completions.choices.text").getItem(0).alias("text"),
    ).show(truncate=False)
)
```

Your output should look something like this. The completion text will be different from the sample.

| **prompt**                   	| **error** 	| **text**                                                                                                                              	|
|:----------------------------:	|:----------:	|:-------------------------------------------------------------------------------------------------------------------------------------:	|
| Hello my name is            	| null      	| Makaveli I'm eighteen years old and I want to   be a rapper when I grow up I love writing and making music I'm from Los   Angeles, CA 	|
| The best code is code thats 	| null      	| understandable This is a subjective statement,   and there is no definitive answer.                                                   	|
| SynapseML is                	| null      	| A machine learning algorithm that is able to learn how to predict the future outcome of events.                                       	|

### Improve throughput with request batching for OpenAICompletion

The example makes several requests to the service, one for each prompt. To complete multiple prompts in a single request, use batch mode. First, in the OpenAICompletion object, instead of setting the Prompt column to "Prompt", specify "batchPrompt" for the BatchPrompt column.
To do so, create a dataframe with a list of prompts per row.

As of this writing there's currently a limit of 20 prompts in a single request, and a hard limit of 2048 "tokens", or approximately 1500 words.


```python
batch_df = spark.createDataFrame(
    [
        (["The time has come", "Pleased to", "Today stocks", "Here's to"],),
        (["The only thing", "Ask not what", "Every litter", "I am"],),
    ]
).toDF("batchPrompt")
```

Next we create the OpenAICompletion object. Rather than setting the prompt column, set the batchPrompt column if your column is of type `Array[String]`.


```python
batch_completion = (
    OpenAICompletion()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setCustomServiceName(service_name)
    .setMaxTokens(200)
    .setBatchPromptCol("batchPrompt")
    .setErrorCol("error")
    .setOutputCol("completions")
)
```

In the call to transform, a request will be made per row. Since there are multiple prompts in a single row, each request is sent with all prompts in that row. The results contain a row for each row in the request.


```python
completed_batch_df = batch_completion.transform(batch_df).cache()
display(completed_batch_df.show(truncate=False))
```

### Using an automatic minibatcher

If your data is in column format, you can transpose it to row format using SynapseML's `FixedMiniBatcherTransformer`.


```python
from pyspark.sql.types import StringType
from synapse.ml.stages import FixedMiniBatchTransformer
from synapse.ml.core.spark import FluentAPI

completed_autobatch_df = (
    df.coalesce(
        1
    )  # Force a single partition so that our little 4-row dataframe makes a batch of size 4, you can remove this step for large datasets
    .mlTransform(FixedMiniBatchTransformer(batchSize=4))
    .withColumnRenamed("prompt", "batchPrompt")
    .mlTransform(batch_completion)
)

display(completed_autobatch_df.show(truncate=False))
```

### Prompt engineering for translation

The Azure OpenAI service can solve many different natural language tasks through [prompt engineering](https://docs.microsoft.com/en-us/azure/cognitive-services/openai/how-to/completions). Here, we show an example of prompting for language translation:


```python
translate_df = spark.createDataFrame(
    [
        ("Japanese: Ookina hako English: Big box Japanese: Midori takoEnglish:",),
        (
            "French: Quel heure et il au Montreal? English: What time is it in Montreal? French: Ou est le poulet? English:",
        ),
    ]
).toDF("prompt")

display(completion.transform(translate_df).show(truncate=False))
```

### Prompt for question answering

Here, we prompt GPT-3 for general-knowledge question answering:


```python
qa_df = spark.createDataFrame(
    [
        (
            "Q: Where is the Grand Canyon?A: The Grand Canyon is in Arizona.Q: What is the weight of the Burj Khalifa in kilograms?A:",
        )
    ]
).toDF("prompt")

display(completion.transform(qa_df).show(truncate=False))
```

### Structured JSON output with json_schema

Use the new `responseFormat` option `json_schema` to force the model to return JSON strictly matching a provided JSON Schema. This improves reliability for downstream parsing and analytics.

**Key points:**
- Set `responseFormat` to a dict containing `type="json_schema"` and a nested `json_schema` object (JSON string form not supported).
- Provide the schema as a Python dict / Scala Map plus a `name` and optional `strict` flag.
- Bare `json_schema` string is rejected; supply the full dict form.
- Allowed simple String values for `responseFormat`: `text`, `json_object`.
- For `json_schema` minimal validation only checks the presence of the `json_schema` key; the nested schema is passed through unchanged.
- The model output will be constrained to the schema (no extra properties when `additionalProperties: false`).

**Summary table**

| Type        | How to set             | Requires nested schema | Notes |
|-------------|------------------------|------------------------|-------|
| text        | String("text")         | No                     | Raw string output |
| json_object | String("json_object")  | No                     | Model attempts well‑formed JSON (not strictly validated) |
| json_schema | Dict/Map only          | Yes                    | Strict; reject bare string or JSON string form |

Below we request a single field `answer` as structured JSON.


```python
from synapse.ml.services.openai import OpenAIChatCompletion
from pyspark.sql import Row

# Define the JSON Schema we want the model to satisfy
schema = {
    "type": "object",
    "properties": {"answer": {"type": "string"}},
    "required": ["answer"],
    "additionalProperties": False,
}

# Single user message requesting structured output
messages_df = spark.createDataFrame(
    [
        (
            [
                Row(
                    role="user",
                    content="What is the capital of France?",
                )
            ],
        )
    ]
).toDF("messages")

chat_structured = (
    OpenAIChatCompletion()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setCustomServiceName(service_name)
    .setMessagesCol("messages")
    .setResponseFormat(
        {
            "type": "json_schema",
            "json_schema": {
                "name": "answer_schema",
                "strict": True,
                "schema": schema,
            },
        }
    )
    .setOutputCol("chat_structured")
    .setErrorCol("chat_structured_error")
)

display(
    chat_structured.transform(messages_df)
    .select("chat_structured.choices.message.content")
    .show(truncate=False)
)
# The returned content should be a JSON object: {\"answer\": \"Paris\"}
```

### ResponseFormat Options Quick Tests

Below we validate `text`, `json_object`, and `json_schema` across APIs in separate cells for clarity.


```python
from synapse.ml.services.openai import OpenAIChatCompletion
from pyspark.sql import Row

messages_df = spark.createDataFrame(
    [([Row(role="user", content="Say hello")],)], ["messages"]
)
chat_text = (
    OpenAIChatCompletion()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setCustomServiceName(service_name)
    .setMessagesCol("messages")
    .setResponseFormat("text")
    .setOutputCol("chat_text")
)
display(
    chat_text.transform(messages_df)
    .select("chat_text.choices.message.content")
    .show(truncate=False)
)
```


```python
messages_df = spark.createDataFrame(
    [([Row(role="user", content="Return a JSON object with key greeting")],)],
    ["messages"],
)
chat_json_obj = (
    OpenAIChatCompletion()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setCustomServiceName(service_name)
    .setMessagesCol("messages")
    .setResponseFormat("json_object")
    .setOutputCol("chat_json_obj")
)
display(
    chat_json_obj.transform(messages_df)
    .select("chat_json_obj.choices.message.content")
    .show(truncate=False)
)
```

### Responses API Structured JSON with json_schema (flattened form)

For the Responses API, you can pass a flattened `json_schema` dict: top-level `name`, `strict`, and `schema` keys.


```python
from synapse.ml.services.openai import OpenAIPrompt

schema = {
    "type": "object",
    "properties": {"answer": {"type": "string"}},
    "required": ["answer"],
    "additionalProperties": False,
}
df = spark.createDataFrame(
    [("France", "capital"), ("Germany", "capital")], ["text", "category"]
)
prompt_flat = (
    OpenAIPrompt()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setCustomServiceName(service_name)
    .setApiType("responses")
    .setApiVersion("2025-04-01-preview")
    .setPromptTemplate("What is the {category} of {text}.")
    .setResponseFormat(
        {
            "type": "json_schema",
            "name": "answer_schema",
            "strict": True,
            "schema": schema,
        }
    )
    .setOutputCol("out_flat")
)
display(prompt_flat.transform(df).select("out_flat").show(truncate=False))
```

### Invalid: Bare json_schema String

Attempting to set `responseFormat` to the bare string `json_schema` should raise an error.


```python
try:

    OpenAIPrompt().setResponseFormat("json_schema")

except Exception as e:

    print("Expected error:", e)
```

## Working with Usage Statistics

The following examples show how to enable usage tracking by setting the `usageCol` parameter. When set, usage statistics are returned in a separate column alongside the model output.

### Chat Completions with `usageCol`

Set the `usageCol` parameter to get usage statistics in a separate column.


```python
from synapse.ml.services.openai import OpenAIPrompt

chat_usage_prompt = (
    OpenAIPrompt()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setCustomServiceName(service_name)
    .setApiType("chat_completions")
    .setUsageCol("usage")  # Enable usage tracking with separate column
    .setPromptTemplate("Provide a fun fact about {topic}.")
    .setOutputCol("chat_output")
)

chat_usage_df = spark.createDataFrame([("Azure AI",)], ["topic"])
chat_usage_result = chat_usage_prompt.transform(chat_usage_df)

# Response is in chat_output, usage statistics are in the separate 'usage' column
display(chat_usage_result.select("chat_output", "usage").show(truncate=False))
```

### Responses API with `usageCol`


```python
responses_usage_prompt = (
    OpenAIPrompt()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setCustomServiceName(service_name)
    .setApiType("responses")
    .setUsageCol("usage")  # Enable usage tracking
    .setPromptTemplate("List two key capabilities of {topic}.")
    .setOutputCol("responses_output")
)

responses_usage_df = spark.createDataFrame([("Azure OpenAI",)], ["topic"])
responses_usage_result = responses_usage_prompt.transform(responses_usage_df)

# Response is in responses_output, usage is in separate 'usage' column
display(responses_usage_result.select("responses_output", "usage").show(truncate=False))
```

### Embeddings with `usageCol`


```python
from synapse.ml.services.openai import OpenAIEmbedding

embedding_usage = (
    OpenAIEmbedding()
    .setSubscriptionKey(key)
    .setCustomServiceName(service_name)
    .setDeploymentName(deployment_name_embeddings)
    .setUsageCol("usage")  # Enable usage tracking with separate column
    .setTextCol("text")
    .setOutputCol("embeddings")
)

embedding_usage_df = spark.createDataFrame(
    [("Usage statistics help monitor token consumption.",)], ["text"]
)
embedding_usage_result = embedding_usage.transform(embedding_usage_df)

# Embeddings are in 'embeddings' column, usage is in separate 'usage' column
display(embedding_usage_result.select("embeddings", "usage").show(truncate=False))
```

### Responses API with `store` and Response Chaining

The Responses API supports storing generated model responses for later retrieval via API using the `store` parameter. When `store=True`, the response ID is returned in a separate column (auto-generated or custom via `setResponseIdCol`). You can chain responses together using `previousResponseId` (scalar) or `previousResponseIdCol` (per-row from column) to maintain conversation context across requests.


```python
from synapse.ml.services.openai import OpenAIResponses
from pyspark.sql.functions import col, lit, array, struct
from pyspark.sql import Row

# Create messages DataFrame
messages_df = spark.createDataFrame(
    [
        ([Row(role="user", content="What is Python?")],),
        ([Row(role="user", content="What is Spark?")],),
    ],
    ["messages"],
)

# First request with store=True to save response for later use
responses = (
    OpenAIResponses()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setCustomServiceName(service_name)
    .setApiVersion("2025-04-01-preview")
    .setStore(True)
    .setMessagesCol("messages")
    .setOutputCol("response")
)

first_result = responses.transform(messages_df).cache()
first_result.count()  # Materialize to avoid re-execution

# Show response IDs (can be used for later retrieval)
display(
    first_result.select(
        col("response.id").alias("response_id"),
        col("response.output")[0]["content"][0]["text"].alias("text"),
    ).show(truncate=80)
)

# Chain responses using previousResponseIdCol
with_ids = first_result.withColumn("prev_id", col("response.id"))
follow_up_df = with_ids.select(
    col("prev_id"),
    array(
        struct(lit("user").alias("role"), lit("Give me an example").alias("content"))
    ).alias("messages"),
)

chained = (
    OpenAIResponses()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setCustomServiceName(service_name)
    .setApiVersion("2025-04-01-preview")
    .setPreviousResponseIdCol("prev_id")
    .setMessagesCol("messages")
    .setOutputCol("chained_response")
)

chained_result = chained.transform(follow_up_df).cache()
display(
    chained_result.select(
        col("prev_id").alias("previous_response_id"),
        col("chained_response.output")[0]["content"][0]["text"].alias("chained_text"),
    ).show(truncate=80)
)
```

### OpenAIPrompt with `store` and Response Chaining

You can also use `store` with `OpenAIPrompt` for a simpler API. When `store=True`, the response ID is returned in a separate column (auto-generated by default or custom via `setResponseIdCol`). The main output column contains only the response text.


```python
from synapse.ml.services.openai import OpenAIPrompt
from pyspark.sql.functions import col

# Sample DataFrame
prompt_df = spark.createDataFrame([("Python",), ("Spark",)], ["topic"])

# First request with store=True - response ID in separate auto-generated column
prompt_store = (
    OpenAIPrompt()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setCustomServiceName(service_name)
    .setApiType("responses")
    .setStore(True)
    .setPromptTemplate("Briefly explain {topic}")
    .setOutputCol("output")
)

first_prompt_result = prompt_store.transform(prompt_df).cache()
first_prompt_result.count()

# Get the auto-generated responseIdCol name
response_id_col_name = prompt_store.getResponseIdCol()

# Response text is in 'output', response ID is in auto-generated column
display(
    first_prompt_result.select(
        col("topic"),
        col("output").alias("response"),  # Just the text
        col(response_id_col_name).alias("response_id"),  # ID in separate column
    ).show(truncate=80)
)

# Chain using previousResponseIdCol
chained_prompt = (
    OpenAIPrompt()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setCustomServiceName(service_name)
    .setApiType("responses")
    .setPreviousResponseIdCol(response_id_col_name)
    .setPromptTemplate("Give me a code example")
    .setOutputCol("chained_output")
)

chained_prompt_result = chained_prompt.transform(first_prompt_result).cache()
display(
    chained_prompt_result.select(
        col("topic"),
        col(response_id_col_name).alias("previous_id"),
        col("chained_output").alias("follow_up_response"),
    ).show(truncate=80)
)
```
