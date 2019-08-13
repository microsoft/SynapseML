# Azure Cognitive Services on Apache Spark™

## Azure Cognitive Services
[Azure Cognitive Services](https://azure.microsoft.com/en-us/services/cognitive-services/) are a suite of APIs, SDKs, and services available to help developers build intelligent applications without having direct AI or data science skills or knowledge by enabling developers to easily add cognitive features into their applications. The goal of Azure Cognitive Services is to help developers create applications that can see, hear, speak, understand, and even begin to reason. The catalog of services within Azure Cognitive Services can be categorized into five main pillars - Vision, Speech, Language, Web Search, and Decision.

## Why use Cognitive Services on Spark?
Azure Cognitive Services on Spark enable working with Azure’s Intelligent Services at massive scales with the Apache Spark™ distributed computing ecosystem. Cognitive Services on Spark allows users to embed general purpose and continuously improving intelligent models directly into their Apache Spark™ and SQL computations. This liberates developers from low-level networking details, so they can focus on creating intelligent, distributed applications. Each Cognitive Service acts as a  a SparkML transformer, so users can add services to existing SparkML pipelines.

## Usage
To see an example of Cognitive Services on Spark in action, take a look at [this sample notebook](../notebooks/samples/CognitiveServices%20-%20Celebrity%20Quote%20Analysis.ipynb).

## Cognitive Services on Apache Spark
Currently, the following Cognitive Services are available on Apache Spark™ through MMLSpark:
### Vision
[**Computer Vision**](https://azure.microsoft.com/en-us/services/cognitive-services/computer-vision/)
- Describe: provides description of an image in human readable language
- Analyze (color, image type, face, adult/racy content): analyzes visual features of an image
- OCR: reads text from an image
- Recognize Text: reads text from an image
- Thumbnail: generates a thumbnail of user-specified size from the image
- Recognize domain-specific content: recognizes domain-specific content (celebrity, landmark)

[**Face**](https://azure.microsoft.com/en-us/services/cognitive-services/face/)
- Detect: detects human faces in an image
- Verify: verifies whether two faces belong to a same person, or a face belongs to a person
- Identify: finds the closest matches of the specific query person face from a person group
- Find similar: given a query face, finds similar faces in a face list
- Group: given a number of faces, divides them into disjoint groups based on similarity

### Speech
[**Speech Services**](https://azure.microsoft.com/en-us/services/cognitive-services/speech-services/)
- Speech-to-text: transcribes audio streams

### Language
[**Text Analytics**](https://azure.microsoft.com/en-us/services/cognitive-services/text-analytics/)
- Language detection: detects language of the input text
- Key phrase extraction: identifies the key talking points in the input text 
- Named entity recognition: identifies known entities and general named entities in the input text
- Sentiment analysis: returns a score betwee 0 and 1 indicating the sentiment in the input text

### Decision
[**Anomaly Detector**](https://azure.microsoft.com/en-us/services/cognitive-services/anomaly-detector/)
- Anomaly status of latest point: generates a model using preceding points and determines whether the latest point is anomalous
- Find anomalies: generates a model using an entire series and finds anomalies in the series

### Web Search
- [Bing Image search](https://azure.microsoft.com/en-us/services/cognitive-services/bing-image-search-api/)