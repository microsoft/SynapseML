---
title: CognitiveServices - OpenAI
hide_title: true
status: stable
---
# Cognitive Services - OpenAI

Large language models are capable of successfully completing multiple downstream tasks with little training data required from users. This is because these models are already trained using enormous amounts of text. The 175 billion-parameter GPT-3 model for example, can generate text and even code given a short prompt containing instructions. 

While large models are becoming more powerful, more multimodal, and relatively cheaper to train, inferencing also needs to scale to handle larger volume of requests from customers. Using SynapseML, customers can now leverage enterprise grade models from Azure OpenAI Service to apply advanced language models on data stored in Azure Synapse Analytics. 

SynapseML is an open source library with a set of consistent APIs that integrate with a number of deep learning and data science tools, including Azure OpenAI. The OpenAI project itself maintains a [great tool](https://github.com/openai/openai-quickstart-node) for experimenting with GPT-3 to get an idea of how it works. SynapseML's integration with Azure OpenAI provides a simple and intuitive coding interface that can be called from Scala, Python or R. It is intended for use in industrial-grade applications, but it is also flexible enough to nimbly handle the demands of consumer website.

This tutorial walks you through a couple steps you need to perform to integrate Azure OpenAI Services to Azure SynapseML and how to apply the large language models available in Azure OpenAI at a distributed scale.

First, set up some administrative details.


```
import os

service_name = "M3Test11"
deployment_name = "text-davinci-001"
if os.environ.get("AZURE_SERVICE", None) == "Microsoft.ProjectArcadia":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    from notebookutils.mssparkutils.credentials import getSecret

    os.environ["OPENAI_API_KEY"] = getSecret("mmlspark-build-keys", "openai-api-key")
    from notebookutils.visualization import display

# put your service keys here
key = os.environ["OPENAI_API_KEY"]
location = "eastus"
assert key is not None and service_name is not None
```

Next, create a dataframe consisting of a series of rows, with one prompt per row. Each prompt is followed by a comma and then ensconsed in a set of parentheses. This format forms a tuple. Then add a string to identify the column containing the prompts.


```
# Create or load a dataframe of text, can load directly from adls or other databases

df = spark.createDataFrame(
    [
        ("Once upon a time",),
        ("Hello my name is",),
        ("The best code is code thats",),
        ("The meaning of life is",),
    ]
).toDF("prompt")
```

To set up the completion interaction with the OpenAI service, create an `OpenAICompletion` object. Set `MaxTokens` to 200. A token is around 4 characters, and this limit applies to the some of the prompt and the result. Set the prompt column with the same name used to identify the prompt column in the dataframe.


```
from synapse.ml.cognitive import OpenAICompletion

completion = (
    OpenAICompletion()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setUrl("https://{}.openai.azure.com/".format(service_name))
    .setMaxTokens(200)
    .setPromptCol("prompt")
    .setOutputCol("completions")
)
```

Now that you have the dataframe and the completion object, you can obtain the prompt completions.


```
# Map the dataframe through OpenAI
completed_df = completion.transform(df).cache()
```

And display them.


```
from pyspark.sql.functions import col

display(completed_df.select(col("prompt"), col("completions.choices.text")))
```

The method above makes several requests to the service, one for each prompt. To complete multiple prompts in a single request, use batch mode. First, in the OpenAICompletion object, instead of setting the Prompt column to "Prompt", specify "batchPrompt" for the BatchPrompt column.

The method used above makes several requests to the service, one for each prompt. To complete multiple prompts in a single request, use batch mode. 

To do so, create a dataframe with a list of prompts per row.

In the `OpenAICompletion` object, rather than setting the `prompt` column, set the `batchPrompt` column instead.

In the call to `transform` a request will then be made per row. Since there are multiple prompts in a single row, each request will be sent with all prompts in that row. The results will contain a row for each row in the request.

Note that as of this writing there is currently a limit of 20 prompts in a single request, as well as a hard limit of 2048 "tokens", or approximately 1500 words.


```
df = spark.createDataFrame(
    [
        (["The time has come", "Pleased to", "Today stocks", "Here's to"],),
        (["The only thing", "Ask not what", "Every litter", "I am"],),
    ]
).toDF("batchPrompt")

batchCompletion = (
    OpenAICompletion()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setUrl("https://{}.openai.azure.com/".format(service_name))
    .setMaxTokens(200)
    .setBatchPromptCol("batchPrompt")
    .setOutputCol("completions")
)

completed_df = batchCompletion.transform(df).cache()
display(completed_df.select(col("batchPrompt"), col("completions.choices.text")))
```

If your data is in column format, you can transpose it to row format using SynapseML's `FixedMiniBatcherTransformer`, along with help from Spark's `coalesce` method.


```
from pyspark.sql.types import StringType
from synapse.ml.stages import FixedMiniBatchTransformer

df = spark.createDataFrame(
    ["This land is", "If I had a", "How many roads", "You can get anything"],
    StringType(),
).toDF("batchPrompt")

# Force a single partition
df = df.coalesce(1)

df = FixedMiniBatchTransformer(batchSize=4, buffered=False).transform(df)

completed_df = batchCompletion.transform(df).cache()
display(completed_df.select(col("batchPrompt"), col("completions.choices.text")))
```

You can try your hand at translation.


```
df = spark.createDataFrame(
    [
        ("Japanese: Ookina hako\nEnglish: Big box\nJapanese: Midori tako\nEnglish:",),
        (
            "French: Quel heure et il au Montreal?\nEnglish: What time is it in Montreal?\nFrench: Ou est le poulet?\nEnglish:",
        ),
    ]
).toDF("prompt")

completed_df = completion.transform(df).cache()
display(completed_df.select(col("prompt"), col("completions.choices.text")))
```

You can prompt for general knowledge.


```
df = spark.createDataFrame(
    [
        (
            "Q: Where is the Grand Canyon?\nA: The Grand Canyon is in Arizona.\n\nQ: What is the weight of the Burj Khalifa in kilograms?\nA:",
        )
    ]
).toDF("prompt")

completed_df = completion.transform(df).cache()
display(completed_df.select(col("prompt"), col("completions.choices.text")))
```


```

```
