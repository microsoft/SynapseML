---
title: Cognitive Services - Overview
hide_title: true
type: notebook
status: stable
categories: ["Cognitive Services"]
---

<img src="/img/notebooks/cog_services_on_spark_2.svg" width="200" />

# Cognitive Services

[Azure Cognitive Services](https://azure.microsoft.com/en-us/services/cognitive-services/) are a suite of APIs, SDKs, and services available to help developers build intelligent applications without having direct AI or data science skills or knowledge by enabling developers to easily add cognitive features into their applications. The goal of Azure Cognitive Services is to help developers create applications that can see, hear, speak, understand, and even begin to reason. The catalog of services within Azure Cognitive Services can be categorized into five main pillars - Vision, Speech, Language, Web Search, and Decision.

## Usage

### Vision
[**Computer Vision**](https://azure.microsoft.com/en-us/services/cognitive-services/computer-vision/)
- Describe: provides description of an image in human readable language ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/DescribeImage.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.DescribeImage))
- Analyze (color, image type, face, adult/racy content): analyzes visual features of an image ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/AnalyzeImage.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.AnalyzeImage))
- OCR: reads text from an image ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/OCR.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.OCR))
- Recognize Text: reads text from an image ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/RecognizeText.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.RecognizeText))
- Thumbnail: generates a thumbnail of user-specified size from the image ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/GenerateThumbnails.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.GenerateThumbnails))
- Recognize domain-specific content: recognizes domain-specific content (celebrity, landmark) ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/RecognizeDomainSpecificContent.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.RecognizeDomainSpecificContent))
- Tag: identifies list of words that are relevant to the in0put image ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/TagImage.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.TagImage))

[**Face**](https://azure.microsoft.com/en-us/services/cognitive-services/face/)
- Detect: detects human faces in an image ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/DetectFace.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.DetectFace))
- Verify: verifies whether two faces belong to a same person, or a face belongs to a person ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/VerifyFaces.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.VerifyFaces))
- Identify: finds the closest matches of the specific query person face from a person group ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/IdentifyFaces.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.IdentifyFaces))
- Find similar: finds similar faces to the query face in a face list ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/FindSimilarFace.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.FindSimilarFace))
- Group: divides a group of faces into disjoint groups based on similarity ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/GroupFaces.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.GroupFaces))

### Speech
[**Speech Services**](https://azure.microsoft.com/en-us/services/cognitive-services/speech-services/)
- Speech-to-text: transcribes audio streams ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/SpeechToText.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.SpeechToText))

### Language
[**Text Analytics**](https://azure.microsoft.com/en-us/services/cognitive-services/text-analytics/)
- Language detection: detects language of the input text ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/LanguageDetector.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.LanguageDetector))
- Key phrase extraction: identifies the key talking points in the input text ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/KeyPhraseExtractor.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.KeyPhraseExtractor))
- Named entity recognition: identifies known entities and general named entities in the input text ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/NER.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.NER))
- Sentiment analysis: returns a score betwee 0 and 1 indicating the sentiment in the input text ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/TextSentiment.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.TextSentiment))

### Decision
[**Anomaly Detector**](https://azure.microsoft.com/en-us/services/cognitive-services/anomaly-detector/)
- Anomaly status of latest point: generates a model using preceding points and determines whether the latest point is anomalous ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/DetectLastAnomaly.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.DetectLastAnomaly))
- Find anomalies: generates a model using an entire series and finds anomalies in the series ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/DetectAnomalies.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.DetectAnomalies))

### Search
- [Bing Image search](https://azure.microsoft.com/en-us/services/cognitive-services/bing-image-search-api/) ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/com/microsoft/ml/spark/cognitive/BingImageSearch.html), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/pyspark/mmlspark.cognitive.html#module-mmlspark.cognitive.BingImageSearch))
- [Azure Cognitive search](https://docs.microsoft.com/en-us/azure/search/search-what-is-azure-search) ([Scala](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/index.html#com.microsoft.ml.spark.cognitive.AzureSearchWriter$), [Python](https://mmlspark.blob.core.windows.net/docs/1.0.0-rc3/scala/index.html#com.microsoft.ml.spark.cognitive.AzureSearchWriter$))


## Prerequisites

1. Follow the steps in [Getting started](https://docs.microsoft.com/en-us/azure/cognitive-services/big-data/getting-started) to set up your Azure Databricks and Cognitive Services environment. This tutorial shows you how to install MMLSpark and how to create your Spark cluster in Databricks.
1. After you create a new notebook in Azure Databricks, copy the **Shared code** below and paste into a new cell in your notebook.
1. Choose a service sample, below, and copy paste it into a second new cell in your notebook.
1. Replace any of the service subscription key placeholders with your own key.
1. Choose the run button (triangle icon) in the upper right corner of the cell, then select **Run Cell**.
1. View results in a table below the cell.

## Shared code

To get started, we'll need to add this code to the project:


```python
from mmlspark.cognitive import *
import os

# A general Cognitive Services key for Text Analytics and Computer Vision (or use separate keys that belong to each service)
service_key = os.environ["TEXT_API_KEY"]
# A Bing Search v7 subscription key
bing_search_key = os.environ["BING_IMAGE_SEARCH_KEY"]
# An Anomaly Dectector subscription key
anomaly_key = os.environ["ANOMALY_API_KEY"]
```

## Text Analytics sample

The [Text Analytics](../text-analytics/index.yml) service provides several algorithms for extracting intelligent insights from text. For example, we can find the sentiment of given input text. The service will return a score between 0.0 and 1.0 where low scores indicate negative sentiment and high score indicates positive sentiment.  This sample uses three simple sentences and returns the sentiment for each.


```python
from pyspark.sql.functions import col

# Create a dataframe that's tied to it's column names
df = spark.createDataFrame([
  ("I am so happy today, its sunny!", "en-US"),
  ("I am frustrated by this rush hour traffic", "en-US"),
  ("The cognitive services on spark aint bad", "en-US"),
], ["text", "language"])

# Run the Text Analytics service with options
sentiment = (TextSentiment()
    .setTextCol("text")
    .setLocation("eastus")
    .setSubscriptionKey(service_key)
    .setOutputCol("sentiment")
    .setErrorCol("error")
    .setLanguageCol("language"))

# Show the results of your text query in a table format
display(sentiment.transform(df).select("text", col("sentiment")[0].getItem("sentiment").alias("sentiment")))
```

## Computer Vision sample

[Computer Vision](../computer-vision/index.yml) analyzes images to identify structure such as faces, objects, and natural-language descriptions. In this sample, we tag a list of images. Tags are one-word descriptions of things in the image like recognizable objects, people, scenery, and actions.


```python
# Create a dataframe with the image URLs
df = spark.createDataFrame([
        ("https://raw.githubusercontent.com/Azure-Samples/cognitive-services-sample-data-files/master/ComputerVision/Images/objects.jpg", ),
        ("https://raw.githubusercontent.com/Azure-Samples/cognitive-services-sample-data-files/master/ComputerVision/Images/dog.jpg", ),
        ("https://raw.githubusercontent.com/Azure-Samples/cognitive-services-sample-data-files/master/ComputerVision/Images/house.jpg", )
    ], ["image", ])

# Run the Computer Vision service. Analyze Image extracts infortmation from/about the images.
analysis = (AnalyzeImage()
    .setLocation("eastus")
    .setSubscriptionKey(service_key)
    .setVisualFeatures(["Categories","Color","Description","Faces","Objects","Tags"])
    .setOutputCol("analysis_results")
    .setImageUrlCol("image")
    .setErrorCol("error"))

# Show the results of what you wanted to pull out of the images.
display(analysis.transform(df).select("image", "analysis_results.description.tags"))
```

## Bing Image Search sample

[Bing Image Search](https://azure.microsoft.com/en-us/services/cognitive-services/bing-image-search-api/) searches the web to retrieve images related to a user's natural language query. In this sample, we use a text query that looks for images with quotes. It returns a list of image URLs that contain photos related to our query.


```python
from pyspark.ml import PipelineModel

# Number of images Bing will return per query
imgsPerBatch = 10
# A list of offsets, used to page into the search results
offsets = [(i*imgsPerBatch,) for i in range(100)]
# Since web content is our data, we create a dataframe with options on that data: offsets
bingParameters = spark.createDataFrame(offsets, ["offset"])

# Run the Bing Image Search service with our text query
bingSearch = (BingImageSearch()
    .setSubscriptionKey(bing_search_key)
    .setOffsetCol("offset")
    .setQuery("Martin Luther King Jr. quotes")
    .setCount(imgsPerBatch)
    .setOutputCol("images"))

# Transformer that extracts and flattens the richly structured output of Bing Image Search into a simple URL column
getUrls = BingImageSearch.getUrlTransformer("images", "url")

# This displays the full results returned, uncomment to use
# display(bingSearch.transform(bingParameters))

# Since we have two services, they are put into a pipeline
pipeline = PipelineModel(stages=[bingSearch, getUrls])

# Show the results of your search: image URLs
display(pipeline.transform(bingParameters))
```

## Speech-to-Text sample
The [Speech-to-text](../speech-service/index-speech-to-text.yml) service converts streams or files of spoken audio to text. In this sample, we transcribe one audio file.


```python
# Create a dataframe with our audio URLs, tied to the column called "url"
df = spark.createDataFrame([("https://mmlspark.blob.core.windows.net/datasets/Speech/audio2.wav",)
                           ], ["url"])

# Run the Speech-to-text service to translate the audio into text
speech_to_text = (SpeechToTextSDK()
    .setSubscriptionKey(service_key)
    .setLocation("eastus")
    .setOutputCol("text")
    .setAudioDataCol("url")
    .setLanguage("en-US")
    .setProfanity("Masked"))

# Show the results of the translation
display(speech_to_text.transform(df).select("url", "text.DisplayText"))
```

## Anomaly Detector sample

[Anomaly Detector](../anomaly-detector/index.yml) is great for detecting irregularities in your time series data. In this sample, we use the service to find anomalies in the entire time series.


```python
from pyspark.sql.functions import lit

# Create a dataframe with the point data that Anomaly Detector requires
df = spark.createDataFrame([
    ("1972-01-01T00:00:00Z", 826.0),
    ("1972-02-01T00:00:00Z", 799.0),
    ("1972-03-01T00:00:00Z", 890.0),
    ("1972-04-01T00:00:00Z", 900.0),
    ("1972-05-01T00:00:00Z", 766.0),
    ("1972-06-01T00:00:00Z", 805.0),
    ("1972-07-01T00:00:00Z", 821.0),
    ("1972-08-01T00:00:00Z", 20000.0),
    ("1972-09-01T00:00:00Z", 883.0),
    ("1972-10-01T00:00:00Z", 898.0),
    ("1972-11-01T00:00:00Z", 957.0),
    ("1972-12-01T00:00:00Z", 924.0),
    ("1973-01-01T00:00:00Z", 881.0),
    ("1973-02-01T00:00:00Z", 837.0),
    ("1973-03-01T00:00:00Z", 9000.0)
], ["timestamp", "value"]).withColumn("group", lit("series1"))

# Run the Anomaly Detector service to look for irregular data
anamoly_detector = (SimpleDetectAnomalies()
  .setSubscriptionKey(anomaly_key)
  .setLocation("eastus")
  .setTimestampCol("timestamp")
  .setValueCol("value")
  .setOutputCol("anomalies")
  .setGroupbyCol("group")
  .setGranularity("monthly"))

# Show the full results of the analysis with the anomalies marked as "True"
display(anamoly_detector.transform(df).select("timestamp", "value", "anomalies.isAnomaly"))
```

## Arbitrary web APIs

With HTTP on Spark, any web service can be used in your big data pipeline. In this example, we use the [World Bank API](http://api.worldbank.org/v2/country/) to get information about various countries around the world.


```python
from requests import Request
from mmlspark.io.http import HTTPTransformer, http_udf
from pyspark.sql.functions import udf, col

# Use any requests from the python requests library
def world_bank_request(country):
  return Request("GET", "http://api.worldbank.org/v2/country/{}?format=json".format(country))

# Create a dataframe with spcificies which countries we want data on
df = (spark.createDataFrame([("br",),("usa",)], ["country"])
  .withColumn("request", http_udf(world_bank_request)(col("country"))))

# Much faster for big data because of the concurrency :)
client = (HTTPTransformer()
      .setConcurrency(3)
      .setInputCol("request")
      .setOutputCol("response"))

# Get the body of the response
def get_response_body(resp):
  return resp.entity.content.decode()

# Show the details of the country data returned
display(client.transform(df).select("country", udf(get_response_body)(col("response")).alias("response")))
```

## Azure Cognitive search sample

In this example, we show how you can enrich data using Cognitive Skills and write to an Azure Search Index using MMLSpark.


```python
from mmlspark.cognitive import *

VISION_API_KEY = os.environ['VISION_API_KEY']
AZURE_SEARCH_KEY = os.environ['AZURE_SEARCH_KEY']
search_service = "mmlspark-azure-search"
search_index = "test-33467690"

df = spark.createDataFrame([("upload", "0", "https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg"), 
                            ("upload", "1", "https://mmlspark.blob.core.windows.net/datasets/DSIR/test2.jpg")], 
                           ["searchAction", "id", "url"])

tdf = AnalyzeImage()\
  .setSubscriptionKey(VISION_API_KEY)\
  .setLocation("eastus")\
  .setImageUrlCol("url")\
  .setOutputCol("analyzed")\
  .setErrorCol("errors")\
  .setVisualFeatures(["Categories", "Tags", "Description", "Faces", "ImageType", "Color", "Adult"])\
  .transform(df)\
  .select("*", "analyzed.*")\
  .drop("errors", "analyzed")

tdf.writeToAzureSearch(subscriptionKey=AZURE_SEARCH_KEY,
  actionCol="searchAction",
  serviceName=search_service,
  indexName=search_index,
  keyCol="id")
```
