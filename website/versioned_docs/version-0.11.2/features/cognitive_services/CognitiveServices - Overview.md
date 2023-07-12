---
title: CognitiveServices - Overview
hide_title: true
status: stable
---
# Cognitive Services

<image width="200" alt-text="icon" src="https://mmlspark.blob.core.windows.net/graphics/Readme/cog_services_on_spark_2.svg" />

[Azure Cognitive Services](https://azure.microsoft.com/services/cognitive-services/) are a suite of APIs, SDKs, and services available to help developers build intelligent applications without having direct AI or data science skills or knowledge by enabling developers to easily add cognitive features into their applications. The goal of Azure Cognitive Services is to help developers create applications that can see, hear, speak, understand, and even begin to reason. The catalog of services within Azure Cognitive Services can be categorized into five main pillars - Vision, Speech, Language, Web Search, and Decision.

## Usage

### Vision
[**Computer Vision**](https://azure.microsoft.com/services/cognitive-services/computer-vision/)
- Describe: provides description of an image in human readable language ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/vision/DescribeImage.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.vision.html#module-synapse.ml.cognitive.vision.DescribeImage))
- Analyze (color, image type, face, adult/racy content): analyzes visual features of an image ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/vision/AnalyzeImage.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.vision.html#module-synapse.ml.cognitive.vision.AnalyzeImage))
- OCR: reads text from an image ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/vision/OCR.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.vision.html#module-synapse.ml.cognitive.vision.OCR))
- Recognize Text: reads text from an image ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/vision/RecognizeText.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.vision.html#module-synapse.ml.cognitive.vision.RecognizeText))
- Thumbnail: generates a thumbnail of user-specified size from the image ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/vision/GenerateThumbnails.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.vision.html#module-synapse.ml.cognitive.vision.GenerateThumbnails))
- Recognize domain-specific content: recognizes domain-specific content (celebrity, landmark) ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/vision/RecognizeDomainSpecificContent.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.vision.html#module-synapse.ml.cognitive.vision.RecognizeDomainSpecificContent))
- Tag: identifies list of words that are relevant to the input image ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/vision/TagImage.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.vision.html#module-synapse.ml.cognitive.vision.TagImage))

[**Face**](https://azure.microsoft.com/services/cognitive-services/face/)
- Detect: detects human faces in an image ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/face/DetectFace.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.face.html#module-synapse.ml.cognitive.face.DetectFace))
- Verify: verifies whether two faces belong to a same person, or a face belongs to a person ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/face/VerifyFaces.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.face.html#module-synapse.ml.cognitive.face.VerifyFaces))
- Identify: finds the closest matches of the specific query person face from a person group ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/face/IdentifyFaces.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.face.html#module-synapse.ml.cognitive.face.IdentifyFaces))
- Find similar: finds similar faces to the query face in a face list ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/face/FindSimilarFace.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.face.html#module-synapse.ml.cognitive.face.FindSimilarFace))
- Group: divides a group of faces into disjoint groups based on similarity ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/face/GroupFaces.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.face.html#module-synapse.ml.cognitive.face.GroupFaces))

### Speech
[**Speech Services**](https://azure.microsoft.com/services/cognitive-services/speech-services/)
- Speech-to-text: transcribes audio streams ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/speech/SpeechToText.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.speech.html#module-synapse.ml.cognitive.speech.SpeechToText))
- Conversation Transcription: transcribes audio streams into live transcripts with identified speakers. ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/speech/ConversationTranscription.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.speech.html#module-synapse.ml.cognitive.speech.ConversationTranscription))
- Text to Speech: Converts text to realistic audio ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/speech/TextToSpeech.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.speech.html#module-synapse.ml.cognitive.speech.TextToSpeech))


### Language
[**Text Analytics**](https://azure.microsoft.com/services/cognitive-services/text-analytics/)
- Language detection: detects language of the input text ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/text/LanguageDetector.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.text.html#module-synapse.ml.cognitive.text.LanguageDetector))
- Key phrase extraction: identifies the key talking points in the input text ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/text/KeyPhraseExtractor.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.text.html#module-synapse.ml.cognitive.text.KeyPhraseExtractor))
- Named entity recognition: identifies known entities and general named entities in the input text ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/text/NER.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.text.html#module-synapse.ml.cognitive.text.NER))
- Sentiment analysis: returns a score between 0 and 1 indicating the sentiment in the input text ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/text/TextSentiment.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.text.html#module-synapse.ml.cognitive.text.TextSentiment))
- Healthcare Entity Extraction: Extracts medical entities and relationships from text. ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/text/AnalyzeHealthText.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.text.html#module-synapse.ml.cognitive.text.AnalyzeHealthText))


### Translation
[**Translator**](https://azure.microsoft.com/services/cognitive-services/translator/)
- Translate: Translates text. ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/translate/Translate.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.translate.html#module-synapse.ml.cognitive.translate.Translate))
- Transliterate: Converts text in one language from one script to another script. ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/translate/Transliterate.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.translate.html#module-synapse.ml.cognitive.translate.Transliterate))
- Detect: Identifies the language of a piece of text. ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/translate/Detect.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.translate.html#module-synapse.ml.cognitive.translate.Detect))
- BreakSentence: Identifies the positioning of sentence boundaries in a piece of text. ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/translate/BreakSentence.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.translate.html#module-synapse.ml.cognitive.translate.BreakSentence))
- Dictionary Lookup: Provides alternative translations for a word and a small number of idiomatic phrases. ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/translate/DictionaryLookup.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.translate.html#module-synapse.ml.cognitive.translate.DictionaryLookup))
- Dictionary Examples: Provides examples that show how terms in the dictionary are used in context. ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/translate/DictionaryExamples.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.translate.html#module-synapse.ml.cognitive.translate.DictionaryExamples))
- Document Translation: Translates documents across all supported languages and dialects while preserving document structure and data format. ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/translate/DocumentTranslator.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.translate.html#module-synapse.ml.cognitive.translate.DocumentTranslator))

### Form Recognizer
[**Form Recognizer**](https://azure.microsoft.com/services/form-recognizer/)
- Analyze Layout: Extract text and layout information from a given document. ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/form/AnalyzeLayout.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.form.html#module-synapse.ml.cognitive.form.AnalyzeLayout))
- Analyze Receipts: Detects and extracts data from receipts using optical character recognition (OCR) and our receipt model, enabling you to easily extract structured data from receipts such as merchant name, merchant phone number, transaction date, transaction total, and more. ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/form/AnalyzeReceipts.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.form.html#module-synapse.ml.cognitive.form.AnalyzeReceipts))
- Analyze Business Cards: Detects and extracts data from business cards using optical character recognition (OCR) and our business card model, enabling you to easily extract structured data from business cards such as contact names, company names, phone numbers, emails, and more. ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/form/AnalyzeBusinessCards.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.form.html#module-synapse.ml.cognitive.form.AnalyzeBusinessCards))
- Analyze Invoices: Detects and extracts data from invoices using optical character recognition (OCR) and our invoice understanding deep learning models, enabling you to easily extract structured data from invoices such as customer, vendor, invoice ID, invoice due date, total, invoice amount due, tax amount, ship to, bill to, line items and more. ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/form/AnalyzeInvoices.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.form.html#module-synapse.ml.cognitive.form.AnalyzeInvoices))
- Analyze ID Documents: Detects and extracts data from identification documents using optical character recognition (OCR) and our ID document model, enabling you to easily extract structured data from ID documents such as first name, last name, date of birth, document number, and more. ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/form/AnalyzeIDDocuments.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.form.html#module-synapse.ml.cognitive.form.AnalyzeIDDocuments))
- Analyze Custom Form: Extracts information from forms (PDFs and images) into structured data based on a model created from a set of representative training forms. ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/form/AnalyzeCustomModel.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.form.html#module-synapse.ml.cognitive.form.AnalyzeCustomModel))
- Get Custom Model: Get detailed information about a custom model. ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/form/GetCustomModel.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/form/ListCustomModels.html))
- List Custom Models: Get information about all custom models. ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/form/ListCustomModels.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.form.html#module-synapse.ml.cognitive.form.ListCustomModels))

### Decision
[**Anomaly Detector**](https://azure.microsoft.com/services/cognitive-services/anomaly-detector/)
- Anomaly status of latest point: generates a model using preceding points and determines whether the latest point is anomalous ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/anomaly/DetectLastAnomaly.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.anomaly.html#module-synapse.ml.cognitive.anomaly.DetectLastAnomaly))
- Find anomalies: generates a model using an entire series and finds anomalies in the series ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/anomaly/DetectAnomalies.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.anomaly.html#module-synapse.ml.cognitive.anomaly.DetectAnomalies))

### Search
- [Bing Image search](https://azure.microsoft.com/services/cognitive-services/bing-image-search-api/) ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/bing/BingImageSearch.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.bing.html#module-synapse.ml.cognitive.bing.BingImageSearch))
- [Azure Cognitive search](https://docs.microsoft.com/azure/search/search-what-is-azure-search) ([Scala](https://mmlspark.blob.core.windows.net/docs/0.11.2/scala/com/microsoft/azure/synapse/ml/cognitive/search/AzureSearchWriter$.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.11.2/pyspark/synapse.ml.cognitive.search.html#module-synapse.ml.cognitive.search.AzureSearchWriter))

## Prerequisites

1. Follow the steps in [Getting started](https://docs.microsoft.com/en-us/azure/cognitive-services/big-data/getting-started) to set up your Azure Databricks and Cognitive Services environment. This tutorial shows you how to install SynapseML and how to create your Spark cluster in Databricks.
1. After you create a new notebook in Azure Databricks, copy the **Shared code** below and paste into a new cell in your notebook.
1. Choose a service sample, below, and copy paste it into a second new cell in your notebook.
1. Replace any of the service subscription key placeholders with your own key.
1. Choose the run button (triangle icon) in the upper right corner of the cell, then select **Run Cell**.
1. View results in a table below the cell.

## Shared code

To get started, we'll need to add this code to the project:


```python
from pyspark.sql.functions import udf, col
from synapse.ml.io.http import HTTPTransformer, http_udf
from requests import Request
from pyspark.sql.functions import lit
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col
import os
```


```python
from pyspark.sql import SparkSession
from synapse.ml.core.platform import *

# Bootstrap Spark Session
spark = SparkSession.builder.getOrCreate()

from synapse.ml.core.platform import materializing_display as display
```


```python
from synapse.ml.cognitive import *

# A general Cognitive Services key for Text Analytics, Computer Vision and Form Recognizer (or use separate keys that belong to each service)
service_key = find_secret("cognitive-api-key")
service_loc = "eastus"

# A Bing Search v7 subscription key
bing_search_key = find_secret("bing-search-key")

# An Anomaly Dectector subscription key
anomaly_key = find_secret("anomaly-api-key")
anomaly_loc = "westus2"

# A Translator subscription key
translator_key = find_secret("translator-key")
translator_loc = "eastus"

# An Azure search key
search_key = find_secret("azure-search-key")
```

## Text Analytics sample

The [Text Analytics](https://azure.microsoft.com/en-us/services/cognitive-services/text-analytics/) service provides several algorithms for extracting intelligent insights from text. For example, we can find the sentiment of given input text. The service will return a score between 0.0 and 1.0 where low scores indicate negative sentiment and high score indicates positive sentiment.  This sample uses three simple sentences and returns the sentiment for each.


```python
# Create a dataframe that's tied to it's column names
df = spark.createDataFrame(
    [
        ("I am so happy today, its sunny!", "en-US"),
        ("I am frustrated by this rush hour traffic", "en-US"),
        ("The cognitive services on spark aint bad", "en-US"),
    ],
    ["text", "language"],
)

# Run the Text Analytics service with options
sentiment = (
    TextSentiment()
    .setTextCol("text")
    .setLocation(service_loc)
    .setSubscriptionKey(service_key)
    .setOutputCol("sentiment")
    .setErrorCol("error")
    .setLanguageCol("language")
)

# Show the results of your text query in a table format
display(
    sentiment.transform(df).select(
        "text", col("sentiment.document.sentiment").alias("sentiment")
    )
)
```

## Text Analytics for Health Sample

The [Text Analytics for Health Service](https://docs.microsoft.com/en-us/azure/cognitive-services/language-service/text-analytics-for-health/overview?tabs=ner) extracts and labels relevant medical information from unstructured texts such as doctor's notes, discharge summaries, clinical documents, and electronic health records.


```python
df = spark.createDataFrame(
    [
        ("20mg of ibuprofen twice a day",),
        ("1tsp of Tylenol every 4 hours",),
        ("6-drops of Vitamin B-12 every evening",),
    ],
    ["text"],
)

healthcare = (
    AnalyzeHealthText()
    .setSubscriptionKey(service_key)
    .setLocation(service_loc)
    .setLanguage("en")
    .setOutputCol("response")
)

display(healthcare.transform(df))
```

## Translator sample
[Translator](https://azure.microsoft.com/en-us/services/cognitive-services/translator/) is a cloud-based machine translation service and is part of the Azure Cognitive Services family of cognitive APIs used to build intelligent apps. Translator is easy to integrate in your applications, websites, tools, and solutions. It allows you to add multi-language user experiences in 90 languages and dialects and can be used for text translation with any operating system. In this sample, we do a simple text translation by providing the sentences you want to translate and target languages you want to translate to.


```python
from pyspark.sql.functions import col, flatten

# Create a dataframe including sentences you want to translate
df = spark.createDataFrame(
    [(["Hello, what is your name?", "Bye"],)],
    [
        "text",
    ],
)

# Run the Translator service with options
translate = (
    Translate()
    .setSubscriptionKey(translator_key)
    .setLocation(translator_loc)
    .setTextCol("text")
    .setToLanguage(["zh-Hans"])
    .setOutputCol("translation")
)

# Show the results of the translation.
display(
    translate.transform(df)
    .withColumn("translation", flatten(col("translation.translations")))
    .withColumn("translation", col("translation.text"))
    .select("translation")
)
```

## Form Recognizer sample
[Form Recognizer](https://azure.microsoft.com/en-us/services/form-recognizer/) is a part of Azure Applied AI Services that lets you build automated data processing software using machine learning technology. Identify and extract text, key/value pairs, selection marks, tables, and structure from your documents—the service outputs structured data that includes the relationships in the original file, bounding boxes, confidence and more. In this sample, we analyze a business card image and extract its information into structured data.


```python
from pyspark.sql.functions import col, explode

# Create a dataframe containing the source files
imageDf = spark.createDataFrame(
    [
        (
            "https://mmlspark.blob.core.windows.net/datasets/FormRecognizer/business_card.jpg",
        )
    ],
    [
        "source",
    ],
)

# Run the Form Recognizer service
analyzeBusinessCards = (
    AnalyzeBusinessCards()
    .setSubscriptionKey(service_key)
    .setLocation(service_loc)
    .setImageUrlCol("source")
    .setOutputCol("businessCards")
)

# Show the results of recognition.
display(
    analyzeBusinessCards.transform(imageDf)
    .withColumn(
        "documents", explode(col("businessCards.analyzeResult.documentResults.fields"))
    )
    .select("source", "documents")
)
```

## Computer Vision sample

[Computer Vision](https://azure.microsoft.com/en-us/services/cognitive-services/computer-vision/) analyzes images to identify structure such as faces, objects, and natural-language descriptions. In this sample, we tag a list of images. Tags are one-word descriptions of things in the image like recognizable objects, people, scenery, and actions.


```python
# Create a dataframe with the image URLs
base_url = "https://raw.githubusercontent.com/Azure-Samples/cognitive-services-sample-data-files/master/ComputerVision/Images/"
df = spark.createDataFrame(
    [
        (base_url + "objects.jpg",),
        (base_url + "dog.jpg",),
        (base_url + "house.jpg",),
    ],
    [
        "image",
    ],
)

# Run the Computer Vision service. Analyze Image extracts infortmation from/about the images.
analysis = (
    AnalyzeImage()
    .setLocation(service_loc)
    .setSubscriptionKey(service_key)
    .setVisualFeatures(
        ["Categories", "Color", "Description", "Faces", "Objects", "Tags"]
    )
    .setOutputCol("analysis_results")
    .setImageUrlCol("image")
    .setErrorCol("error")
)

# Show the results of what you wanted to pull out of the images.
display(analysis.transform(df).select("image", "analysis_results.description.tags"))
```

## Bing Image Search sample

[Bing Image Search](https://azure.microsoft.com/en-us/services/cognitive-services/bing-image-search-api/) searches the web to retrieve images related to a user's natural language query. In this sample, we use a text query that looks for images with quotes. It returns a list of image URLs that contain photos related to our query.


```python
# Number of images Bing will return per query
imgsPerBatch = 10
# A list of offsets, used to page into the search results
offsets = [(i * imgsPerBatch,) for i in range(100)]
# Since web content is our data, we create a dataframe with options on that data: offsets
bingParameters = spark.createDataFrame(offsets, ["offset"])

# Run the Bing Image Search service with our text query
bingSearch = (
    BingImageSearch()
    .setSubscriptionKey(bing_search_key)
    .setOffsetCol("offset")
    .setQuery("Martin Luther King Jr. quotes")
    .setCount(imgsPerBatch)
    .setOutputCol("images")
)

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
The [Speech-to-text](https://azure.microsoft.com/en-us/services/cognitive-services/speech-services/) service converts streams or files of spoken audio to text. In this sample, we transcribe one audio file.


```python
# Create a dataframe with our audio URLs, tied to the column called "url"
df = spark.createDataFrame(
    [("https://mmlspark.blob.core.windows.net/datasets/Speech/audio2.wav",)], ["url"]
)

# Run the Speech-to-text service to translate the audio into text
speech_to_text = (
    SpeechToTextSDK()
    .setSubscriptionKey(service_key)
    .setLocation(service_loc)
    .setOutputCol("text")
    .setAudioDataCol("url")
    .setLanguage("en-US")
    .setProfanity("Masked")
)

# Show the results of the translation
display(speech_to_text.transform(df).select("url", "text.DisplayText"))
```

## Text-to-Speech sample
[Text to speech](https://azure.microsoft.com/en-us/services/cognitive-services/text-to-speech/#overview) is a service that allows one to build apps and services that speak naturally, choosing from more than 270 neural voices across 119 languages and variants.


```python
from synapse.ml.cognitive import TextToSpeech

fs = ""
if running_on_databricks():
    fs = "dbfs:"
elif running_on_synapse_internal():
    fs = "Files"

# Create a dataframe with text and an output file location
df = spark.createDataFrame(
    [
        (
            "Reading out loud is fun! Check out aka.ms/spark for more information",
            fs + "/output.mp3",
        )
    ],
    ["text", "output_file"],
)

tts = (
    TextToSpeech()
    .setSubscriptionKey(service_key)
    .setTextCol("text")
    .setLocation(service_loc)
    .setVoiceName("en-US-JennyNeural")
    .setOutputFileCol("output_file")
)

# Check to make sure there were no errors during audio creation
display(tts.transform(df))
```

## Anomaly Detector sample

[Anomaly Detector](https://azure.microsoft.com/en-us/services/cognitive-services/anomaly-detector/) is great for detecting irregularities in your time series data. In this sample, we use the service to find anomalies in the entire time series.


```python
# Create a dataframe with the point data that Anomaly Detector requires
df = spark.createDataFrame(
    [
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
        ("1973-03-01T00:00:00Z", 9000.0),
    ],
    ["timestamp", "value"],
).withColumn("group", lit("series1"))

# Run the Anomaly Detector service to look for irregular data
anamoly_detector = (
    SimpleDetectAnomalies()
    .setSubscriptionKey(anomaly_key)
    .setLocation(anomaly_loc)
    .setTimestampCol("timestamp")
    .setValueCol("value")
    .setOutputCol("anomalies")
    .setGroupbyCol("group")
    .setGranularity("monthly")
)

# Show the full results of the analysis with the anomalies marked as "True"
display(
    anamoly_detector.transform(df).select("timestamp", "value", "anomalies.isAnomaly")
)
```

## Arbitrary web APIs

With HTTP on Spark, any web service can be used in your big data pipeline. In this example, we use the [World Bank API](http://api.worldbank.org/v2/country/) to get information about various countries around the world.


```python
# Use any requests from the python requests library


def world_bank_request(country):
    return Request(
        "GET", "http://api.worldbank.org/v2/country/{}?format=json".format(country)
    )


# Create a dataframe with spcificies which countries we want data on
df = spark.createDataFrame([("br",), ("usa",)], ["country"]).withColumn(
    "request", http_udf(world_bank_request)(col("country"))
)

# Much faster for big data because of the concurrency :)
client = (
    HTTPTransformer().setConcurrency(3).setInputCol("request").setOutputCol("response")
)

# Get the body of the response


def get_response_body(resp):
    return resp.entity.content.decode()


# Show the details of the country data returned
display(
    client.transform(df).select(
        "country", udf(get_response_body)(col("response")).alias("response")
    )
)
```

## Azure Cognitive search sample

In this example, we show how you can enrich data using Cognitive Skills and write to an Azure Search Index using SynapseML.


```python
search_service = "mmlspark-azure-search"
search_index = "test-33467690"

df = spark.createDataFrame(
    [
        (
            "upload",
            "0",
            "https://mmlspark.blob.core.windows.net/datasets/DSIR/test1.jpg",
        ),
        (
            "upload",
            "1",
            "https://mmlspark.blob.core.windows.net/datasets/DSIR/test2.jpg",
        ),
    ],
    ["searchAction", "id", "url"],
)

tdf = (
    AnalyzeImage()
    .setSubscriptionKey(service_key)
    .setLocation(service_loc)
    .setImageUrlCol("url")
    .setOutputCol("analyzed")
    .setErrorCol("errors")
    .setVisualFeatures(
        ["Categories", "Tags", "Description", "Faces", "ImageType", "Color", "Adult"]
    )
    .transform(df)
    .select("*", "analyzed.*")
    .drop("errors", "analyzed")
)

tdf.writeToAzureSearch(
    subscriptionKey=search_key,
    actionCol="searchAction",
    serviceName=search_service,
    indexName=search_index,
    keyCol="id",
)
```
