
# Other Tutorials

The following tutorials provide complete examples of using Cognitive Services in Synapse Analytics.

- [Sentiment analysis with Cognitive Services](tutorial-cognitive-services-sentiment.md) - Using an example data set of customer comments, you build a Spark table with a column that indicates the sentiment of the comments in each row.

- [Anomaly detection with Cognitive Services](tutorial-cognitive-services-anomaly.md) - Using an example data set of time series data, you build a Spark table with a column that indicates whether the data in each row is an anomaly.

- [Build machine learning applications using Microsoft Machine Learning for Apache Spark](tutorial-build-applications-use-mmlspark.md) - This tutorial demonstrates how to use SynapseML to access several models from Cognitive Services.

- [Form recognizer with Applied AI Service](tutorial-form-recognizer-use-mmlspark.md) demonstrates how to use [Form Recognizer](../../applied-ai-services/form-recognizer/index.yml) to analyze your forms and documents, extracts text and data on Azure Synapse Analytics. 

- [Text Analytics with Cognitive Service](tutorial-text-analytics-use-mmlspark.md) shows how to use [Text Analytics](../../cognitive-services/text-analytics/index.yml) to analyze unstructured text on Azure Synapse Analytics.

- [Translator with Cognitive Service](tutorial-translator-use-mmlspark.md) shows how to use [Translator](../../cognitive-services/Translator/index.yml) to build intelligent, multi-language solutions on Azure Synapse Analytics

- [Computer Vision with Cognitive Service](tutorial-computer-vision-use-mmlspark.md) demonstrates how to use [Computer Vision](../../cognitive-services/computer-vision/index.yml) to analyze images on Azure Synapse Analytics.

# Available Cognitive Services APIs
## Bing Image Search

| API Type                                   | SynapseML APIs                  | Cognitive Service APIs (Versions)                                                                                               | DEP VNet Support |
| ------------------------------------------ | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
|Bing Image Search|BingImageSearch|Images - Visual Search V7.0| Not Supported|

## Anomaly Detector

| API Type                                   | SynapseML APIs                  | Cognitive Service APIs (Versions)                                                                                               | DEP VNet Support |
| ------------------------------------------ | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| Detect Last Anomaly                        | DetectLastAnomaly              | Detect Last Point V1.0                                                                                                          | Supported        |
| Detect Anomalies                           | DetectAnomalies                | Detect Entire Series V1.0                                                                                                       | Supported        |
| Simple DetectAnomalies                     | SimpleDetectAnomalies          | Detect Entire Series V1.0                                                                                                       | Supported        |

## Computer vision

| API Type                                   | SynapseML APIs                  | Cognitive Service APIs (Versions)                                                                                               | DEP VNet Support |
| ------------------------------------------ | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| OCR                                        | OCR                            | Recognize Printed Text V2.0                                                                                                     | Supported        |
| Recognize Text                             | RecognizeText                  | Recognize Text V2.0                                                                                                             | Supported        |
| Read Image                                 | ReadImage                      | Read V3.1                                                                                                                       | Supported        |
| Generate Thumbnails                        | GenerateThumbnails             | Generate Thumbnail V2.0                                                                                                         | Supported        |
| Analyze Image                              | AnalyzeImage                   | Analyze Image V2.0                                                                                                              | Supported        |
| Recognize Domain Specific Content          | RecognizeDomainSpecificContent | Analyze Image By Domain V2.0                                                                                                    | Supported        |
| Tag Image                                  | TagImage                       | Tag Image V2.0                                                                                                                  | Supported        |
| Describe Image                             | DescribeImage                  | Describe Image V2.0                                                                                                             | Supported        |


## Translator

| API Type                                   | SynapseML APIs                  | Cognitive Service APIs (Versions)                                                                                               | DEP VNet Support |
| ------------------------------------------ | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| Translate Text                             | Translate                      | Translate V3.0                                                                                                                  | Not Supported    |
| Transliterate Text                         | Transliterate                  | Transliterate V3.0                                                                                                              | Not Supported    |
| Detect Language                            | Detect                         | Detect V3.0                                                                                                                     | Not Supported    |
| Break Sentence                             | BreakSentence                  | Break Sentence V3.0                                                                                                             | Not Supported    |
| Dictionary lookup (alternate translations) | DictionaryLookup               | Dictionary Lookup V3.0                                                                                                          | Not Supported    |
| Document Translation                       | DocumentTranslator             | Document Translation V1.0                                                                                                       | Not Supported    |


## Face

| API Type                                   | SynapseML APIs                  | Cognitive Service APIs (Versions)                                                                                               | DEP VNet Support |
| ------------------------------------------ | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| Detect Face                                | DetectFace                     | Detect With Url V1.0                                                                                                            | Supported        |
| Find Similar Face                          | FindSimilarFace                | Find Similar V1.0                                                                                                               | Supported        |
| Group Faces                                | GroupFaces                     | Group V1.0                                                                                                                      | Supported        |
| Identify Faces                             | IdentifyFaces                  | Identify V1.0                                                                                                                   | Supported        |
| Verify Faces                               | VerifyFaces                    | Verify Face To Face V1.0                                                                                                        | Supported        |

## Form Recognizer
| API Type                                   | SynapseML APIs                  | Cognitive Service APIs (Versions)                                                                                               | DEP VNet Support |
| ------------------------------------------ | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| Analyze Layout                             | AnalyzeLayout                  | Analyze Layout Async V2.1                                                                                                       | Supported        |
| Analyze Receipts                           | AnalyzeReceipts                | Analyze Receipt Async V2.1                                                                                                      | Supported        |
| Analyze Business Cards                     | AnalyzeBusinessCards           | Analyze Business Card Async V2.1                                                                                                | Supported        |
| Analyze Invoices                           | AnalyzeInvoices                | Analyze Invoice Async V2.1                                                                                                      | Supported        |
| Analyze ID Documents                       | AnalyzeIDDocuments             | identification (ID) document model V2.1                                                                                         | Supported        |
| List Custom Models                         | ListCustomModels               | List Custom Models V2.1                                                                                                         | Supported        |
| Get Custom Model                           | GetCustomModel                 | Get Custom Models V2.1                                                                                                          | Supported        |
| Analyze Custom Model                       | AnalyzeCustomModel             | Analyze With Custom Model V2.1                                                                                                  | Supported        |

## Speech-to-text
| API Type                                   | SynapseML APIs                  | Cognitive Service APIs (Versions)                                                                                               | DEP VNet Support |
| ------------------------------------------ | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| Speech To Text                             | SpeechToText                   | SpeechToText V1.0 |  Not Supported    |
| Speech To Text SDK                         | SpeechToTextSDK                | Using Speech SDK Version 1.14.0                                                                                                 | Not Supported    |


## Text Analytics

| API Type                                   | SynapseML APIs                  | Cognitive Service APIs (Versions)                                                                                               | DEP VNet Support |
| ------------------------------------------ | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| Text Sentiment V2                          | TextSentimentV2                | Sentiment V2.0                                                                                                                  | Supported        |
| Language Detector V2                       | LanguageDetectorV2             | Languages V2.0                                                                                                                  | Supported        |
| Entity Detector V2                         | EntityDetectorV2               | Entities Linking V2.0                                                                                                           | Supported        |
| NER V2                                     | NERV2                          | Entities Recognition General V2.0                                                                                               | Supported        |
| Key Phrase Extractor V2                    | KeyPhraseExtractorV2           | Key Phrases V2.0                                                                                                                | Supported        |
| Text Sentiment                             | TextSentiment                  | Sentiment V3.1                                                                                                                  | Supported        |
| Key Phrase Extractor                       | KeyPhraseExtractor             | Key Phrases V3.1                                                                                                                | Supported        |
| PII                                        | PII                            | Entities Recognition Pii V3.1                                                                                                   | Supported        |
| NER                                        | NER                            | Entities Recognition General V3.1                                                                                               | Supported        |
| Language Detector                          | LanguageDetector               | Languages V3.1                                                                                                                  | Supported        |
| Entity Detector                            | EntityDetector                 | Entities Linking V3.1                                                                                                           | Supported        |


# Next steps

- [Machine Learning capabilities in Azure Synapse Analytics](what-is-machine-learning.md)
- [What are Cognitive Services?](../../cognitive-services/what-are-cognitive-services.md)
- [Use a sample notebook from the Synapse Analytics gallery](quickstart-gallery-sample-notebook.md)
