---
title: CognitiveServices - OpenAI
hide_title: true
status: stable
---
# Azure OpenAI for Big Data

The Azure OpenAI service can be used to solve a large number of natural language tasks through prompting the completion API. To make it easier to scale your prompting workflows from a few examples to large datasets of examples we have integrated the Azure OpenAI service with the distributed machine learning library [SynapseML](https://www.microsoft.com/en-us/research/blog/synapseml-a-simple-multilingual-and-massively-parallel-machine-learning-library/). This integration makes it easy to use the [Apache Spark](https://spark.apache.org/) distributed computing framework to process millions of prompts with the OpenAI service. This tutorial shows how to apply large language models at a distributed scale using Azure Open AI and Azure Synapse Analytics. 

## Step 1: Prerequisites

The key prerequisites for this quickstart include a working Azure OpenAI resource, and an Apache Spark cluster with SynapseML installed. We suggest creating a Synapse workspace, but an Azure Databricks, HDInsight, or Spark on Kubernetes, or even a python environment with the `pyspark` package will work. 

1. An Azure OpenAI resource – request access [here](https://customervoice.microsoft.com/Pages/ResponsePage.aspx?id=v4j5cvGGr0GRqy180BHbR7en2Ais5pxKtso_Pz4b1_xUOFA5Qk1UWDRBMjg0WFhPMkIzTzhKQ1dWNyQlQCN0PWcu) before [creating a resource](https://docs.microsoft.com/en-us/azure/cognitive-services/openai/how-to/create-resource?pivots=web-portal#create-a-resource)
1. [Create a Synapse workspace](https://docs.microsoft.com/en-us/azure/synapse-analytics/get-started-create-workspace)
1. [Create a serverless Apache Spark pool](https://docs.microsoft.com/en-us/azure/synapse-analytics/get-started-analyze-spark#create-a-serverless-apache-spark-pool)


## Step 2: Import this guide as a notebook

The next step is to add this code into your Spark cluster. You can either create a notebook in your Spark platform and copy the code into this notebook to run the demo. Or download the notebook and import it into Synapse Analytics

1.	[Download this demo as a notebook](https://github.com/microsoft/SynapseML/blob/master/notebooks/features/cognitive_services/CognitiveServices%20-%20OpenAI.ipynb) (click Raw, then save the file)
1.	Import the notebook [into the Synapse Workspace](https://docs.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-development-using-notebooks#create-a-notebook) or if using Databricks [into the Databricks Workspace](https://docs.microsoft.com/en-us/azure/databricks/notebooks/notebooks-manage#create-a-notebook)
1. Install SynapseML on your cluster. Please see the installation instructions for Synapse at the bottom of [the SynapseML website](https://microsoft.github.io/SynapseML/). Note that this requires pasting an additional cell at the top of the notebook you just imported
3.	Connect your notebook to a cluster and follow along, editing and rnnung the cells below.

## Step 3: Fill in your service information

Next, please edit the cell in the notebook to point to your service. In particular set the `service_name`, `deployment_name`, `location`, and `key` variables to match those for your OpenAI service:


```python
import os
from pyspark.sql import SparkSession
from synapse.ml.core.platform import running_on_synapse, find_secret

# Bootstrap Spark Session
spark = SparkSession.builder.getOrCreate()

if running_on_synapse():
    from notebookutils.visualization import display

# Fill in the following lines with your service information
service_name = "synapseml-openai"
deployment_name = "text-davinci-001"
deployment_name_embeddings = "text-search-ada-doc-001"
deployment_name_embeddings_query = "text-search-ada-query-001"

key = find_secret("openai-api-key")  # please replace this with your key as a string

assert key is not None and service_name is not None
```

## Step 4: Create a dataset of prompts

Next, create a dataframe consisting of a series of rows, with one prompt per row. 

You can also load data directly from ADLS or other databases. For more information on loading and preparing Spark dataframes, see the [Apache Spark data loading guide](https://spark.apache.org/docs/latest/sql-data-sources.html).


```python
df = spark.createDataFrame(
    [
        ("Hello my name is",),
        ("The best code is code thats",),
        ("SynapseML is ",),
    ]
).toDF("prompt")
```

## Step 5: Create the OpenAICompletion Apache Spark Client

To apply the OpenAI Completion service to your dataframe you just created, create an OpenAICompletion object which serves as a distributed client. Parameters of the service can be set either with a single value, or by a column of the dataframe with the appropriate setters on the `OpenAICompletion` object. Here we are setting `maxTokens` to 200. A token is around 4 characters, and this limit applies to the sum of the prompt and the result. We are also setting the `promptCol` parameter with the name of the prompt column in the dataframe.


```python
from synapse.ml.cognitive import OpenAICompletion

completion = (
    OpenAICompletion()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name)
    .setUrl("https://{}.openai.azure.com/".format(service_name))
    .setMaxTokens(200)
    .setPromptCol("prompt")
    .setErrorCol("error")
    .setOutputCol("completions")
)
```

## Step 5: Transform the dataframe with the OpenAICompletion Client

Now that you have the dataframe and the completion client, you can transform your input dataset and add a column called `completions` with all of the information the service adds. We will select out just the text for simplicity.


```python
from pyspark.sql.functions import col

completed_df = completion.transform(df).cache()
display(
    completed_df.select(
        col("prompt"),
        col("error"),
        col("completions.choices.text").getItem(0).alias("text"),
    )
)
```

Your output should look something like this. Please note completion text will be different

| **prompt**                 	| **error** 	| **text**                                                                                                                              	|
|-----------------------------	|-----------	|---------------------------------------------------------------------------------------------------------------------------------------	|
| Hello my name is            	| null      	| Makaveli I'm eighteen years old and I want to   be a rapper when I grow up I love writing and making music I'm from Los   Angeles, CA 	|
| The best code is code thats 	| null      	| understandable This is a subjective statement,   and there is no definitive answer.                                                   	|
| SynapseML is                	| null      	| A machine learning algorithm that is able to learn how to predict the future outcome of events.                                       	|

## Additional Usage Examples

### Improve throughput with request batching 

The example above makes several requests to the service, one for each prompt. To complete multiple prompts in a single request, use batch mode. First, in the OpenAICompletion object, instead of setting the Prompt column to "Prompt", specify "batchPrompt" for the BatchPrompt column.
To do so, create a dataframe with a list of prompts per row.

**Note** that as of this writing there is currently a limit of 20 prompts in a single request, as well as a hard limit of 2048 "tokens", or approximately 1500 words.


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
    .setUrl("https://{}.openai.azure.com/".format(service_name))
    .setMaxTokens(200)
    .setBatchPromptCol("batchPrompt")
    .setErrorCol("error")
    .setOutputCol("completions")
)
```

In the call to transform a request will then be made per row. Since there are multiple prompts in a single row, each request will be sent with all prompts in that row. The results will contain a row for each row in the request.


```python
completed_batch_df = batch_completion.transform(batch_df).cache()
display(completed_batch_df)
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

display(completed_autobatch_df)
```

### Prompt engineering for translation

The Azure OpenAI service can solve many different natural language tasks through [prompt engineering](https://docs.microsoft.com/en-us/azure/cognitive-services/openai/how-to/completions). Here we show an example of prompting for language translation:


```python
translate_df = spark.createDataFrame(
    [
        ("Japanese: Ookina hako \nEnglish: Big box \nJapanese: Midori tako\nEnglish:",),
        (
            "French: Quel heure et il au Montreal? \nEnglish: What time is it in Montreal? \nFrench: Ou est le poulet? \nEnglish:",
        ),
    ]
).toDF("prompt")

display(completion.transform(translate_df))
```

### Prompt for question answering

Here, we prompt GPT-3 for general-knowledge question answering:


```python
qa_df = spark.createDataFrame(
    [
        (
            "Q: Where is the Grand Canyon?\nA: The Grand Canyon is in Arizona.\n\nQ: What is the weight of the Burj Khalifa in kilograms?\nA:",
        )
    ]
).toDF("prompt")

display(completion.transform(qa_df))
```

# OpenAI Embeddings

We will use t-SNE to reduce the dimensionality of the embeddings from 1536 to 2. Once the embeddings are reduced to two dimensions, we can plot them in a 2D scatter plot.


```python
from synapse.ml.cognitive import OpenAIEmbedding

embedding = (
    OpenAIEmbedding()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name_embeddings)
    .setUrl("https://{}.openai.azure.com/".format(service_name))
    .setTextCol("combined")
    .setErrorCol("error")
    .setOutputCol("embeddings")
)
```


```python
import pyspark.sql.functions as F

df = spark.read.options(inferSchema="True", delimiter=",", header=True).csv(
    "wasbs://publicwasb@mmlspark.blob.core.windows.net/fine_food_reviews_1k.csv"
)

df = df.withColumn(
    "combined",
    F.format_string("Title: %s; Content: %s", F.trim(df.Summary), F.trim(df.Text)),
)

display(df)
```


```python
from pyspark.sql.functions import col

completed_df = embedding.transform(df).cache()
display(completed_df)
```

## Retrieve embeddings


```python
import numpy as np

matrix = np.array(completed_df.select("embeddings").collect())[:, 0, :]
matrix.shape
```

## Reduce dimensionality
We reduce the dimensionality to 2 dimensions using t-SNE decomposition.


```python
import pandas as pd
from sklearn.manifold import TSNE
import numpy as np

# Create a t-SNE model and transform the data
tsne = TSNE(
    n_components=2, perplexity=15, random_state=42, init="random", learning_rate=200
)
vis_dims = tsne.fit_transform(matrix)
vis_dims.shape
```

## Plot the embeddings
We colour each review by its star rating, ranging from red for negative reviews, to green for positive reviews..

We can observe a decent data separation even in the reduced 2 dimensions.


```python
import matplotlib.pyplot as plt
import matplotlib
import numpy as np

scores = np.array(completed_df.select("Score").collect()).reshape(-1)

colors = ["red", "darkorange", "gold", "turquoise", "darkgreen"]
x = [x for x, y in vis_dims]
y = [y for x, y in vis_dims]
color_indices = scores - 1

colormap = matplotlib.colors.ListedColormap(colors)
plt.scatter(x, y, c=color_indices, cmap=colormap, alpha=0.3)
for score in [0, 1, 2, 3, 4]:
    avg_x = np.array(x)[scores - 1 == score].mean()
    avg_y = np.array(y)[scores - 1 == score].mean()
    color = colors[score]
    plt.scatter(avg_x, avg_y, marker="x", color=color, s=100)

plt.title("Amazon ratings visualized in language using t-SNE")
```

## Use embeddings to build a semantic search Index

Note that for some OpenAI models, users should use separate models for embedding documents and queries. These models are denoted by the "-doc" and "-query" suffixes respectively. 


```python
embedding_query = (
    OpenAIEmbedding()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name_embeddings_query)
    .setUrl("https://{}.openai.azure.com/".format(service_name))
    .setTextCol("query")
    .setErrorCol("error")
    .setOutputCol("embeddings")
)
```

## Create a dataframe of search queries

Note: The data types of the ID columns in the document and query dataframes should be the same


```python
query_df = (
    spark.createDataFrame(
        [
            (
                0,
                "desserts",
            ),
            (
                1,
                "disgusting",
            ),
        ]
    )
    .toDF("id", "query")
    .withColumn("id", F.col("id").cast("int"))
)
```

## Generate embeddings for queries


```python
completed_query_df = embedding_query.transform(query_df).cache()
```

## Build index for fast retrieval


```python
from synapse.ml.nn import *

knn = (
    KNN()
    .setFeaturesCol("embeddings")
    .setValuesCol("id")
    .setOutputCol("output")
    .setK(10)
)  # top-k for retrieval

knn_index = knn.fit(completed_df)
```

## Retrieve results


```python
df_matches = knn_index.transform(completed_query_df).cache()

df_result = (
    df_matches.withColumn("match", F.explode("output"))
    .join(df, df["id"] == F.col("match.value"))
    .select("query", F.col("combined"), "match.distance")
)

display(df_result)
```
