---
title: CognitiveServices - Celebrity Quote Analysis
hide_title: true
status: stable
---
# Celebrity Quote Analysis with The Cognitive Services on Spark

<img src="https://mmlspark.blob.core.windows.net/graphics/SparkSummit2/cog_services.png" width="800" />


```python
from synapse.ml.cognitive import *
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, udf
from pyspark.ml.feature import SQLTransformer
import os

if os.environ.get("AZURE_SERVICE", None) == "Microsoft.ProjectArcadia":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    from notebookutils.mssparkutils.credentials import getSecret
    os.environ['VISION_API_KEY'] = getSecret("mmlspark-keys", "mmlspark-cs-key")
    os.environ['TEXT_API_KEY'] = getSecret("mmlspark-keys", "mmlspark-cs-key")
    os.environ['BING_IMAGE_SEARCH_KEY'] = getSecret("mmlspark-keys", "mmlspark-bing-search-key")

#put your service keys here
TEXT_API_KEY          = os.environ["TEXT_API_KEY"]
VISION_API_KEY        = os.environ["VISION_API_KEY"]
BING_IMAGE_SEARCH_KEY = os.environ["BING_IMAGE_SEARCH_KEY"]
```

### Extracting celebrity quote images using Bing Image Search on Spark

Here we define two Transformers to extract celebrity quote images.

<img src="https://mmlspark.blob.core.windows.net/graphics/Cog%20Service%20NB/step%201.png" width="600" />


```python
imgsPerBatch = 10 #the number of images Bing will return for each query
offsets = [(i*imgsPerBatch,) for i in range(100)] # A list of offsets, used to page into the search results
bingParameters = spark.createDataFrame(offsets, ["offset"])

bingSearch = BingImageSearch()\
  .setSubscriptionKey(BING_IMAGE_SEARCH_KEY)\
  .setOffsetCol("offset")\
  .setQuery("celebrity quotes")\
  .setCount(imgsPerBatch)\
  .setOutputCol("images")

#Transformer to that extracts and flattens the richly structured output of Bing Image Search into a simple URL column
getUrls = BingImageSearch.getUrlTransformer("images", "url")
```

### Recognizing Images of Celebrities
This block identifies the name of the celebrities for each of the images returned by the Bing Image Search.

<img src="https://mmlspark.blob.core.windows.net/graphics/Cog%20Service%20NB/step%202.png" width="600" />


```python
celebs = RecognizeDomainSpecificContent()\
          .setSubscriptionKey(VISION_API_KEY)\
          .setModel("celebrities")\
          .setUrl("https://eastus.api.cognitive.microsoft.com/vision/v2.0/")\
          .setImageUrlCol("url")\
          .setOutputCol("celebs")

#Extract the first celebrity we see from the structured response
firstCeleb = SQLTransformer(statement="SELECT *, celebs.result.celebrities[0].name as firstCeleb FROM __THIS__")
```

### Reading the quote from the image.
This stage performs OCR on the images to recognize the quotes.

<img src="https://mmlspark.blob.core.windows.net/graphics/Cog%20Service%20NB/step%203.png" width="600" />


```python
from synapse.ml.stages import UDFTransformer

recognizeText = RecognizeText()\
  .setSubscriptionKey(VISION_API_KEY)\
  .setUrl("https://eastus.api.cognitive.microsoft.com/vision/v2.0/recognizeText")\
  .setImageUrlCol("url")\
  .setMode("Printed")\
  .setOutputCol("ocr")\
  .setConcurrency(5)

def getTextFunction(ocrRow):
    if ocrRow is None: return None
    return "\n".join([line.text for line in ocrRow.recognitionResult.lines])

# this transformer wil extract a simpler string from the structured output of recognize text
getText = UDFTransformer().setUDF(udf(getTextFunction)).setInputCol("ocr").setOutputCol("text")

```

### Understanding the Sentiment of the Quote

<img src="https://mmlspark.blob.core.windows.net/graphics/Cog%20Service%20NB/step4.jpg" width="600" />


```python
sentimentTransformer = TextSentiment()\
    .setTextCol("text")\
    .setUrl("https://eastus.api.cognitive.microsoft.com/text/analytics/v3.0/sentiment")\
    .setSubscriptionKey(TEXT_API_KEY)\
    .setOutputCol("sentiment")

#Extract the sentiment score from the API response body
getSentiment = SQLTransformer(statement="SELECT *, sentiment[0].sentiment as sentimentLabel FROM __THIS__")
```

### Tying it all together

Now that we have built the stages of our pipeline its time to chain them together into a single model that can be used to process batches of incoming data

<img src="https://mmlspark.blob.core.windows.net/graphics/Cog%20Service%20NB/full_pipe_2.jpg" width="800" />


```python
from synapse.ml.stages import SelectColumns
# Select the final coulmns
cleanupColumns = SelectColumns().setCols(["url", "firstCeleb", "text", "sentimentLabel"])

celebrityQuoteAnalysis = PipelineModel(stages=[
  bingSearch, getUrls, celebs, firstCeleb, recognizeText, getText, sentimentTransformer, getSentiment, cleanupColumns])

celebrityQuoteAnalysis.transform(bingParameters).show(5)
```
