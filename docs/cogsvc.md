<img width="200"  src="https://mmlspark.blob.core.windows.net/graphics/Readme/cog_services_on_spark_2.svg">

# The Azure Cognitive Services on Apache Spark™

## Azure Cognitive Services
[Azure Cognitive Services](https://azure.microsoft.com/en-us/services/cognitive-services/) are a suite of APIs, SDKs, and services available to help developers build intelligent applications without having direct AI or data science skills or knowledge by enabling developers to easily add cognitive features into their applications. The goal of Azure Cognitive Services is to help developers create applications that can see, hear, speak, understand, and even begin to reason. The catalog of services within Azure Cognitive Services can be categorized into five main pillars - Vision, Speech, Language, Web Search, and Decision.

## Why use Cognitive Services on Spark?
Azure Cognitive Services on Spark enable working with Azure’s Intelligent Services at massive scales with the Apache Spark™ distributed computing ecosystem. Cognitive Services on Spark allows users to embed general purpose and continuously improving intelligent models directly into their Apache Spark™ and SQL computations. This liberates developers from low-level networking details, so they can focus on creating intelligent, distributed applications. Each Cognitive Service acts as a SparkML transformer, so users can add services to existing SparkML pipelines. This is a great example of our [HTTP-on-Spark](http.md) capability that lets you interact with HTTP services from Apache Spark.

## Usage
To see an example of Cognitive Services on Spark in action, take a look at [this sample notebook](../notebooks/CognitiveServices%20-%20Celebrity%20Quote%20Analysis.ipynb).

## Cognitive Services on Apache Spark™
Currently, the following Cognitive Services are available on Apache Spark™ through SynapseML:
### Vision
[**Computer Vision**](https://azure.microsoft.com/en-us/services/cognitive-services/computer-vision/)
- Describe: provides description of an image in human readable language ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/DescribeImage.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.DescribeImage))
- Analyze (color, image type, face, adult/racy content): analyzes visual features of an image ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/AnalyzeImage.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.AnalyzeImage))
- OCR: reads text from an image ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/OCR.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.OCR))
- Recognize Text: reads text from an image ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/RecognizeText.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.RecognizeText))
- Thumbnail: generates a thumbnail of user-specified size from the image ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/GenerateThumbnails.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.GenerateThumbnails))
- Recognize domain-specific content: recognizes domain-specific content (celebrity, landmark) ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/RecognizeDomainSpecificContent.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.RecognizeDomainSpecificContent))
- Tag: identifies list of words that are relevant to the in0put image ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/TagImage.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.TagImage))

[**Face**](https://azure.microsoft.com/en-us/services/cognitive-services/face/)
- Detect: detects human faces in an image ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/DetectFace.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.DetectFace))
- Verify: verifies whether two faces belong to a same person, or a face belongs to a person ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/VerifyFaces.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.VerifyFaces))
- Identify: finds the closest matches of the specific query person face from a person group ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/IdentifyFaces.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.IdentifyFaces))
- Find similar: finds similar faces to the query face in a face list ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/FindSimilarFace.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.FindSimilarFace))
- Group: divides a group of faces into disjoint groups based on similarity ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/GroupFaces.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.GroupFaces))

### Speech
[**Speech Services**](https://azure.microsoft.com/en-us/services/cognitive-services/speech-services/)
- Speech-to-text: transcribes audio streams ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/SpeechToText.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.SpeechToText))

### Language
[**Text Analytics**](https://azure.microsoft.com/en-us/services/cognitive-services/text-analytics/)
- Language detection: detects language of the input text ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/LanguageDetector.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.LanguageDetector))
- Key phrase extraction: identifies the key talking points in the input text ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/KeyPhraseExtractor.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.KeyPhraseExtractor))
- Named entity recognition: identifies known entities and general named entities in the input text ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/NER.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.NER))
- Sentiment analysis: returns a score betwee 0 and 1 indicating the sentiment in the input text ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/TextSentiment.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.TextSentiment))

### Decision
[**Anomaly Detector**](https://azure.microsoft.com/en-us/services/cognitive-services/anomaly-detector/)
- Anomaly status of latest point: generates a model using preceding points and determines whether the latest point is anomalous ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/DetectLastAnomaly.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.DetectLastAnomaly))
- Find anomalies: generates a model using an entire series and finds anomalies in the series ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/DetectAnomalies.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.DetectAnomalies))

### Web Search
- [Bing Image search](https://azure.microsoft.com/en-us/services/cognitive-services/bing-image-search-api/) ([Scala](https://mmlspark.blob.core.windows.net/docs/0.9.0/scala/com/microsoft/ml/spark/cognitive/BingImageSearch.html), [Python](https://mmlspark.blob.core.windows.net/docs/0.9.0/pyspark/synapse.ml.cognitive.html#module-synapse.ml.cognitive.BingImageSearch))

