#!/usr/bin/env python
# coding: utf-8

# <h1>Creating a searchable Art Database with The MET's open-access collection</h1>

# In this example, we show how you can enrich data using Cognitive Skills and write to an Azure Search Index using MMLSpark. We use a subset of The MET's open-access collection and enrich it by passing it through 'Describe Image' and a custom 'Image Similarity' skill. The results are then written to a searchable index.

# In[3]:


import os, sys, time, json, requests
from pyspark.ml import Transformer, Estimator, Pipeline
from pyspark.ml.feature import SQLTransformer
from pyspark.sql.functions import lit, udf, col, split


# In[4]:


VISION_API_KEY = os.environ['VISION_API_KEY']
AZURE_SEARCH_KEY = os.environ['AZURE_SEARCH_KEY']
search_service = "mmlspark-azure-search"
search_index = "test"


# In[5]:


data = spark.read  .format("csv")  .option("header", True)  .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/metartworks_sample.csv")  .withColumn("searchAction", lit("upload"))  .withColumn("Neighbors", split(col("Neighbors"), ",").cast("array<string>"))  .withColumn("Tags", split(col("Tags"), ",").cast("array<string>"))  .limit(25)


# <img src="https://mmlspark.blob.core.windows.net/graphics/CognitiveSearchHyperscale/MetArtworkSamples.png" width="800" style="float: center;"/>

# In[7]:


from mmlspark.cognitive import AnalyzeImage
from mmlspark.stages import SelectColumns

#define pipeline
describeImage = (AnalyzeImage()
  .setSubscriptionKey(VISION_API_KEY)
  .setLocation("eastus")
  .setImageUrlCol("PrimaryImageUrl")
  .setOutputCol("RawImageDescription")
  .setErrorCol("Errors")
  .setVisualFeatures(["Categories", "Tags", "Description", "Faces", "ImageType", "Color", "Adult"])
  .setConcurrency(5))

df2 = describeImage.transform(data)  .select("*", "RawImageDescription.*").drop("Errors", "RawImageDescription")


# <img src="https://mmlspark.blob.core.windows.net/graphics/CognitiveSearchHyperscale/MetArtworksProcessed.png" width="800" style="float: center;"/>

# Before writing the results to a Search Index, you must define a schema which must specify the name, type, and attributes of each field in your index. Refer [Create a basic index in Azure Search](https://docs.microsoft.com/en-us/azure/search/search-what-is-an-index) for more information.

# In[10]:


from mmlspark.cognitive import *
df2.writeToAzureSearch(
  subscriptionKey=AZURE_SEARCH_KEY,
  actionCol="searchAction",
  serviceName=search_service,
  indexName=search_index,
  keyCol="ObjectID"
)


# The Search Index can be queried using the [Azure Search REST API](https://docs.microsoft.com/rest/api/searchservice/) by sending GET or POST requests and specifying query parameters that give the criteria for selecting matching documents. For more information on querying refer [Query your Azure Search index using the REST API](https://docs.microsoft.com/en-us/rest/api/searchservice/Search-Documents)

# In[12]:


url = 'https://{}.search.windows.net/indexes/{}/docs/search?api-version=2019-05-06'.format(search_service, search_index)
requests.post(url, json={"search": "Glass"}, headers = {"api-key": AZURE_SEARCH_KEY}).json()


# In[13]:


# 

