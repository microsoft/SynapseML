---
title: Quickstart - OpenAI Embedding
hide_title: true
status: stable
---
# Embedding Text with Azure OpenAI

The Azure OpenAI service can be used to solve a large number of natural language tasks through prompting the completion API. To make it easier to scale your prompting workflows from a few examples to large datasets of examples we have integrated the Azure OpenAI service with the distributed machine learning library [SynapseML](https://www.microsoft.com/en-us/research/blog/synapseml-a-simple-multilingual-and-massively-parallel-machine-learning-library/). This integration makes it easy to use the [Apache Spark](https://spark.apache.org/) distributed computing framework to process millions of prompts with the OpenAI service. This tutorial shows how to apply large language models to generate embeddings for large datasets of text. 

## Step 1: Prerequisites

The key prerequisites for this quickstart include a working Azure OpenAI resource, and an Apache Spark cluster with SynapseML installed. We suggest creating a Synapse workspace, but an Azure Databricks, HDInsight, or Spark on Kubernetes, or even a python environment with the `pyspark` package will work. 

1. An Azure OpenAI resource – request access [here](https://customervoice.microsoft.com/Pages/ResponsePage.aspx?id=v4j5cvGGr0GRqy180BHbR7en2Ais5pxKtso_Pz4b1_xUOFA5Qk1UWDRBMjg0WFhPMkIzTzhKQ1dWNyQlQCN0PWcu) before [creating a resource](https://docs.microsoft.com/en-us/azure/cognitive-services/openai/how-to/create-resource?pivots=web-portal#create-a-resource)
1. [Create a Synapse workspace](https://docs.microsoft.com/en-us/azure/synapse-analytics/get-started-create-workspace)
1. [Create a serverless Apache Spark pool](https://docs.microsoft.com/en-us/azure/synapse-analytics/get-started-analyze-spark#create-a-serverless-apache-spark-pool)


## Step 2: Import this guide as a notebook

The next step is to add this code into your Spark cluster. You can either create a notebook in your Spark platform and copy the code into this notebook to run the demo. Or download the notebook and import it into Synapse Analytics

1.	[Download this demo as a notebook](https://github.com/microsoft/SynapseML/blob/master/notebooks/features/cognitive_services/CognitiveServices%20-%20OpenAI%20Embedding.ipynb) (click Raw, then save the file)
1.	Import the notebook [into the Synapse Workspace](https://docs.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-development-using-notebooks#create-a-notebook) or if using Databricks [into the Databricks Workspace](https://docs.microsoft.com/en-us/azure/databricks/notebooks/notebooks-manage#create-a-notebook)
1. Install SynapseML on your cluster. Please see the installation instructions for Synapse at the bottom of [the SynapseML website](https://microsoft.github.io/SynapseML/). Note that this requires pasting an additional cell at the top of the notebook you just imported
3.	Connect your notebook to a cluster and follow along, editing and rnnung the cells below.

## Step 3: Fill in your service information

Next, please edit the cell in the notebook to point to your service. In particular set the `service_name`, `deployment_name`, `location`, and `key` variables to match those for your OpenAI service:


```python
from synapse.ml.core.platform import find_secret

# Fill in the following lines with your service information
# Learn more about selecting which embedding model to choose: https://openai.com/blog/new-and-improved-embedding-model
service_name = "synapseml-openai"
deployment_name_embeddings = "text-embedding-ada-002"

key = find_secret(
    secret_name="openai-api-key", keyvault="mmlspark-build-keys"
)  # please replace this with your key as a string

assert key is not None and service_name is not None
```

## Step 4: Load Data

In this demo we will explore a dataset of fine food reviews


```python
import pyspark.sql.functions as F

df = (
    spark.read.options(inferSchema="True", delimiter=",", header=True)
    .csv("wasbs://publicwasb@mmlspark.blob.core.windows.net/fine_food_reviews_1k.csv")
    .repartition(5)
)

df = df.withColumn(
    "combined",
    F.format_string("Title: %s; Content: %s", F.trim(df.Summary), F.trim(df.Text)),
)

display(df)
```

## Step 5: Generate Embeddings

We will first generate embeddings for the reviews using the SynapseML OpenAIEmbedding client.


```python
from synapse.ml.services.openai import OpenAIEmbedding

embedding = (
    OpenAIEmbedding()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name_embeddings)
    .setCustomServiceName(service_name)
    .setTextCol("combined")
    .setErrorCol("error")
    .setOutputCol("embeddings")
)

completed_df = embedding.transform(df).cache()
display(completed_df)
```

## Step 6: Reduce Embedding dimensionality for Visualization
We reduce the dimensionality to 2 dimensions using t-SNE decomposition.


```python
import pandas as pd
from sklearn.manifold import TSNE
import numpy as np

collected = list(completed_df.collect())
matrix = np.array([[r["embeddings"]] for r in collected])[:, 0, :].astype(np.float64)
scores = np.array([[r["Score"]] for r in collected]).reshape(-1)

tsne = TSNE(n_components=2, perplexity=15, random_state=42, init="pca")
vis_dims = tsne.fit_transform(matrix)
vis_dims.shape
```

## Step 7: Plot the embeddings

We now use t-SNE to reduce the dimensionality of the embeddings from 1536 to 2. Once the embeddings are reduced to two dimensions, we can plot them in a 2D scatter plot. We colour each review by its star rating, ranging from red for negative reviews, to green for positive reviews. We can observe a decent data separation even in the reduced 2 dimensions.


```python
import matplotlib.pyplot as plt
import matplotlib
import numpy as np

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

## Step 8: Build a fast vector index to over review embeddings

We will use SynapseML's KNN estimator to build a fast cosine-similarity retrieval engine.


```python
from synapse.ml.nn import *

knn = (
    KNN()
    .setFeaturesCol("embeddings")
    .setValuesCol("id")
    .setOutputCol("output")
    .setK(10)
)

knn_index = knn.fit(completed_df)
```

## Step 8: Build the retrieval model pipeline

Note: The data types of the ID columns in the document and query dataframes should be the same. For some OpenAI models, users should use separate models for embedding documents and queries. These models are denoted by the "-doc" and "-query" suffixes respectively.


```python
from pyspark.ml import PipelineModel

embedding_query = (
    OpenAIEmbedding()
    .setSubscriptionKey(key)
    .setDeploymentName(deployment_name_embeddings)
    .setCustomServiceName(service_name)
    .setTextCol("query")
    .setErrorCol("error")
    .setOutputCol("embeddings")
)

retrieval_model = PipelineModel(stages=[embedding_query, knn_index])
```

## Step 9: Retrieve results


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


df_matches = retrieval_model.transform(query_df).cache()

df_result = (
    df_matches.withColumn("match", F.explode("output"))
    .join(df, df["id"] == F.col("match.value"))
    .select("query", F.col("combined"), "match.distance")
)

display(df_result)
```
