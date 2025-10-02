---
title: Quickstart - Analyze Text
hide_title: true
status: stable
---
# Analyze Text with SynapseML and Azure AI Language
[Azure AI Language](https://learn.microsoft.com/azure/ai-services/language-service/overview) is a cloud-based service that provides Natural Language Processing (NLP) features for understanding and analyzing text. Use this service to help build intelligent applications using the web-based Language Studio, REST APIs, and client libraries.
You can use SynapseML with Azure AI Language for **named entity recognition**, **language detection**, **entity linking**, **key phrase extraction**, **Pii entity recognition** and **sentiment analysis**.


```python
from synapse.ml.services.language import AnalyzeText
from synapse.ml.core.platform import find_secret

ai_service_key = find_secret(
    secret_name="ai-services-api-key", keyvault="mmlspark-build-keys"
)
ai_service_location = "eastus"
```

## Named Entity Recognition 
[Named Entity Recognition](https://learn.microsoft.com/azure/ai-services/language-service/named-entity-recognition/overview) is one of the features offered by Azure AI Language, a collection of machine learning and AI algorithms in the cloud for developing intelligent applications that involve written language. The NER feature can identify and categorize entities in unstructured text. For example: people, places, organizations, and quantities. Refer to [this article](https://learn.microsoft.com/azure/ai-services/language-service/named-entity-recognition/language-support?tabs=ga-api) for the full list of supported languages.


```python
df = spark.createDataFrame(
    data=[
        ["en", "Dr. Smith has a very modern medical office, and she has great staff."],
        ["en", "I had a wonderful trip to Seattle last week."],
    ],
    schema=["language", "text"],
)

entity_recognition = (
    AnalyzeText()
    .setKind("EntityRecognition")
    .setLocation(ai_service_location)
    .setSubscriptionKey(ai_service_key)
    .setTextCol("text")
    .setOutputCol("entities")
    .setErrorCol("error")
    .setLanguageCol("language")
)

df_results = entity_recognition.transform(df)
display(df_results.select("language", "text", "entities.documents.entities"))
```

This cell should yield a result that looks like:


| language         | text     | entities |
|:--------------|:-----------|:------------|
| en           | Dr. Smith has a very modern medical office, and she has great staff.      | [{"category": "Person", "confidenceScore": 0.98, "length": 5, "offset": 4, "subcategory": null, "text": "Smith"}, {"category": "Location", "confidenceScore": 0.79, "length": 14, "offset": 28, "subcategory": "Structural", "text": "medical office"}, {"category": "PersonType", "confidenceScore": 0.85, "length": 5, "offset": 62, "subcategory": null, "text": "staff"}]        |
| en           | I had a wonderful trip to Seattle last week.  | [{"category": "Event", "confidenceScore": 0.74, "length": 4, "offset": 18, "subcategory": null, "text": "trip"}, {"category": "Location", "confidenceScore": 1, "length": 7, "offset": 26, "subcategory": "GPE", "text": "Seattle"}, {"category": "DateTime", "confidenceScore": 0.8, "length": 9, "offset": 34, "subcategory": "DateRange", "text": "last week"}]       |

## LanguageDetection
[Language detection](https://learn.microsoft.com/azure/ai-services/language-service/language-detection/overview) can detect the language a document is written in. It returns a language code for a wide range of languages, variants, dialects, and some regional/cultural languages. Refer to [this article](https://learn.microsoft.com/azure/ai-services/language-service/language-detection/language-support) for the full list of supported languages.


```python
df = spark.createDataFrame(
    data=[
        ["This is a document written in English."],
        ["这是一份用中文写的文件"],
    ],
    schema=["text"],
)

language_detection = (
    AnalyzeText()
    .setKind("LanguageDetection")
    .setLocation(ai_service_location)
    .setSubscriptionKey(ai_service_key)
    .setTextCol("text")
    .setOutputCol("detected_language")
    .setErrorCol("error")
)

df_results = language_detection.transform(df)
display(df_results.select("text", "detected_language.documents.detectedLanguage"))
```

This cell should yield a result that looks like:


| text     | detectedLanguage |
|:-----------|:------------|
| This is a document written in English.      | {"name": "English", "iso6391Name": "en", "confidenceScore": 0.99}        |
| 这是一份用中文写的文件  | {"name": "Chinese_Simplified", "iso6391Name": "zh_chs", "confidenceScore": 1}       |

## EntityLinking
[Entity linking](https://learn.microsoft.com/azure/ai-services/language-service/entity-linking/overview) identifies and disambiguates the identity of entities found in text. For example, in the sentence "We went to Seattle last week.", the word "Seattle" would be identified, with a link to more information on Wikipedia. [English and Spanish are supported](https://learn.microsoft.com/azure/ai-services/language-service/entity-linking/language-support).


```python
df = spark.createDataFrame(
    data=[
        ["Microsoft was founded by Bill Gates and Paul Allen on April 4, 1975."],
        ["We went to Seattle last week."],
    ],
    schema=["text"],
)

entity_linking = (
    AnalyzeText()
    .setKind("EntityLinking")
    .setLocation(ai_service_location)
    .setSubscriptionKey(ai_service_key)
    .setTextCol("text")
    .setOutputCol("entity_linking")
    .setErrorCol("error")
)

df_results = entity_linking.transform(df)
display(df_results.select("text", "entity_linking.documents.entities"))
```

This cell should yield a result that looks like:


| text     | entities |
|:-----------|:------------|
| Microsoft was founded by Bill Gates and Paul Allen on April 4, 1975.      | [{"bingId": "a093e9b9-90f5-a3d5-c4b8-5855e1b01f85", "dataSource": "Wikipedia", "id": "Microsoft", "language": "en", "matches": [{"confidenceScore": 0.48, "length": 9, "offset": 0, "text": "Microsoft"}], "name": "Microsoft", "url": "https://en.wikipedia.org/wiki/Microsoft"}, {"bingId": "0d47c987-0042-5576-15e8-97af601614fa", "dataSource": "Wikipedia", "id": "Bill Gates", "language": "en", "matches": [{"confidenceScore": 0.52, "length": 10, "offset": 25, "text": "Bill Gates"}], "name": "Bill Gates", "url": "https://en.wikipedia.org/wiki/Bill_Gates"}, {"bingId": "df2c4376-9923-6a54-893f-2ee5a5badbc7", "dataSource": "Wikipedia", "id": "Paul Allen", "language": "en", "matches": [{"confidenceScore": 0.54, "length": 10, "offset": 40, "text": "Paul Allen"}], "name": "Paul Allen", "url": "https://en.wikipedia.org/wiki/Paul_Allen"}, {"bingId": "52535f87-235e-b513-54fe-c03e4233ac6e", "dataSource": "Wikipedia", "id": "April 4", "language": "en", "matches": [{"confidenceScore": 0.38, "length": 7, "offset": 54, "text": "April 4"}], "name": "April 4", "url": "https://en.wikipedia.org/wiki/April_4"}]        |
| We went to Seattle last week.  | [{"bingId": "5fbba6b8-85e1-4d41-9444-d9055436e473", "dataSource": "Wikipedia", "id": "Seattle", "language": "en", "matches": [{"confidenceScore": 0.17, "length": 7, "offset": 11, "text": "Seattle"}], "name": "Seattle", "url": "https://en.wikipedia.org/wiki/Seattle"}]       |

## KeyPhraseExtraction
[Key phrase extraction](https://learn.microsoft.com/en-us/azure/ai-services/language-service/key-phrase-extraction/overview) is one of the features offered by Azure AI Language, a collection of machine learning and AI algorithms in the cloud for developing intelligent applications that involve written language. Use key phrase extraction to quickly identify the main concepts in text. For example, in the text "The food was delicious and the staff were wonderful.", key phrase extraction will return the main topics: "food" and "wonderful staff". Refer to [this article](https://learn.microsoft.com/azure/ai-services/language-service/key-phrase-extraction/language-support) for the full list of supported languages.


```python
df = spark.createDataFrame(
    data=[
        ["Microsoft was founded by Bill Gates and Paul Allen on April 4, 1975."],
        ["Dr. Smith has a very modern medical office, and she has great staff."],
    ],
    schema=["text"],
)

key_phrase_extraction = (
    AnalyzeText()
    .setKind("KeyPhraseExtraction")
    .setLocation(ai_service_location)
    .setSubscriptionKey(ai_service_key)
    .setTextCol("text")
    .setOutputCol("key_phrase_extraction")
    .setErrorCol("error")
)

df_results = key_phrase_extraction.transform(df)
display(df_results.select("text", "key_phrase_extraction.documents.keyPhrases"))
```

This cell should yield a result that looks like:


| text     | keyPhrases |
|:-----------|:------------|
| Microsoft was founded by Bill Gates and Paul Allen on April 4, 1975.      | ["Bill Gates", "Paul Allen", "Microsoft", "April"]        |
| Dr. Smith has a very modern medical office, and she has great staff.  | ["modern medical office", "Dr. Smith", "great staff"]       |

## PiiEntityRecognition
The PII detection feature can identify, categorize, and redact sensitive information in unstructured text. For example: phone numbers, email addresses, and forms of identification. The method for utilizing PII in conversations is different than other use cases, and articles for this use have been separated. Refer to [this article](https://learn.microsoft.com/azure/ai-services/language-service/personally-identifiable-information/language-support?tabs=documents) for the full list of supported languages.


```python
df = spark.createDataFrame(
    data=[
        ["Call our office at 312-555-1234, or send an email to support@contoso.com"],
        ["Dr. Smith has a very modern medical office, and she has great staff."],
    ],
    schema=["text"],
)

pii_entity_recognition = (
    AnalyzeText()
    .setKind("PiiEntityRecognition")
    .setLocation(ai_service_location)
    .setSubscriptionKey(ai_service_key)
    .setTextCol("text")
    .setOutputCol("pii_entity_recognition")
    .setErrorCol("error")
)

df_results = pii_entity_recognition.transform(df)
display(df_results.select("text", "pii_entity_recognition.documents.entities"))
```

This cell should yield a result that looks like:


| text     | entities |
|:-----------|:------------|
| Call our office at 312-555-1234, or send an email to support@contoso.com      | [{"category": "PhoneNumber", "confidenceScore": 0.8, "length": 12, "offset": 19, "subcategory": null, "text": "312-555-1234"}, {"category": "Email", "confidenceScore": 0.8, "length": 19, "offset": 53, "subcategory": null, "text": "support@contoso.com"}]        |
| Dr. Smith has a very modern medical office, and she has great staff.  | [{"category": "Person", "confidenceScore": 0.93, "length": 5, "offset": 4, "subcategory": null, "text": "Smith"}]       |

## SentimentAnalysis
[Sentiment analysis](https://learn.microsoft.com/en-us/azure/ai-services/language-service/sentiment-opinion-mining/overview) and opinion mining are features offered by the Language service, a collection of machine learning and AI algorithms in the cloud for developing intelligent applications that involve written language. These features help you find out what people think of your brand or topic by mining text for clues about positive or negative sentiment, and can associate them with specific aspects of the text. Refer to [this article](https://learn.microsoft.com/azure/ai-services/language-service/sentiment-opinion-mining/language-support) for the full list of supported languages.


```python
df = spark.createDataFrame(
    data=[
        ["The food and service were unacceptable. The concierge was nice, however."],
        ["It taste great."],
    ],
    schema=["text"],
)

sentiment_analysis = (
    AnalyzeText()
    .setKind("SentimentAnalysis")
    .setLocation(ai_service_location)
    .setSubscriptionKey(ai_service_key)
    .setTextCol("text")
    .setOutputCol("sentiment_analysis")
    .setErrorCol("error")
)

df_results = sentiment_analysis.transform(df)
display(df_results.select("text", "sentiment_analysis.documents.sentiment"))
```

This cell should yield a result that looks like:


| text     | sentiment |
|:-----------|:------------|
| The food and service were unacceptable. The concierge was nice, however.      | mixed        |
| It tastes great.  | positive       |

## Analyze Text with TextAnalyze

Text Analyze is Deprecated, please use AnalyzeText instead


```python
df = spark.createDataFrame(
    data=[
        ["en", "Hello Seattle"],
        ["en", "There once was a dog who lived in London and thought she was a human"],
    ],
    schema=["language", "text"],
)
```


```python
from synapse.ml.services import *

text_analyze = (
    TextAnalyze()
    .setLocation(ai_service_location)
    .setSubscriptionKey(ai_service_key)
    .setTextCol("text")
    .setOutputCol("textAnalysis")
    .setErrorCol("error")
    .setLanguageCol("language")
    .setEntityRecognitionParams(
        {"model-version": "latest"}
    )  # Can pass parameters to each model individually
    .setIncludePii(False)  # Users can manually exclude tasks to speed up analysis
    .setIncludeEntityLinking(False)
    .setIncludeSentimentAnalysis(False)
)

df_results = text_analyze.transform(df)
```


```python
display(df_results)
```
