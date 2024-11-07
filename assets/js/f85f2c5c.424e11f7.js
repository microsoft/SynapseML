"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[31018],{3905:(e,t,a)=>{a.d(t,{Zo:()=>p,kt:()=>g});var n=a(67294);function i(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function r(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function l(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?r(Object(a),!0).forEach((function(t){i(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):r(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function o(e,t){if(null==e)return{};var a,n,i=function(e,t){if(null==e)return{};var a,n,i={},r=Object.keys(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||(i[a]=e[a]);return i}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(i[a]=e[a])}return i}var s=n.createContext({}),c=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):l(l({},t),e)),a},p=function(e){var t=c(e.components);return n.createElement(s.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},u=n.forwardRef((function(e,t){var a=e.components,i=e.mdxType,r=e.originalType,s=e.parentName,p=o(e,["components","mdxType","originalType","parentName"]),u=c(a),g=i,f=u["".concat(s,".").concat(g)]||u[g]||d[g]||r;return a?n.createElement(f,l(l({ref:t},p),{},{components:a})):n.createElement(f,l({ref:t},p))}));function g(e,t){var a=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var r=a.length,l=new Array(r);l[0]=u;var o={};for(var s in t)hasOwnProperty.call(t,s)&&(o[s]=t[s]);o.originalType=e,o.mdxType="string"==typeof e?e:i,l[1]=o;for(var c=2;c<r;c++)l[c]=a[c];return n.createElement.apply(null,l)}return n.createElement.apply(null,a)}u.displayName="MDXCreateElement"},28467:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>s,contentTitle:()=>l,default:()=>d,frontMatter:()=>r,metadata:()=>o,toc:()=>c});var n=a(83117),i=(a(67294),a(3905));const r={title:"Quickstart - Analyze Text",hide_title:!0,status:"stable"},l="Analyze Text with SynapseML and Azure AI Language",o={unversionedId:"Explore Algorithms/AI Services/Quickstart - Analyze Text",id:"version-1.0.7/Explore Algorithms/AI Services/Quickstart - Analyze Text",title:"Quickstart - Analyze Text",description:"Azure AI Language is a cloud-based service that provides Natural Language Processing (NLP) features for understanding and analyzing text. Use this service to help build intelligent applications using the web-based Language Studio, REST APIs, and client libraries.",source:"@site/versioned_docs/version-1.0.7/Explore Algorithms/AI Services/Quickstart - Analyze Text.md",sourceDirName:"Explore Algorithms/AI Services",slug:"/Explore Algorithms/AI Services/Quickstart - Analyze Text",permalink:"/SynapseML/docs/1.0.7/Explore Algorithms/AI Services/Quickstart - Analyze Text",draft:!1,tags:[],version:"1.0.7",frontMatter:{title:"Quickstart - Analyze Text",hide_title:!0,status:"stable"},sidebar:"docs",previous:{title:"Quickstart - Analyze Celebrity Quotes",permalink:"/SynapseML/docs/1.0.7/Explore Algorithms/AI Services/Quickstart - Analyze Celebrity Quotes"},next:{title:"Quickstart - Create a Visual Search Engine",permalink:"/SynapseML/docs/1.0.7/Explore Algorithms/AI Services/Quickstart - Create a Visual Search Engine"}},s={},c=[{value:"Named Entity Recognition",id:"named-entity-recognition",level:2},{value:"LanguageDetection",id:"languagedetection",level:2},{value:"EntityLinking",id:"entitylinking",level:2},{value:"KeyPhraseExtraction",id:"keyphraseextraction",level:2},{value:"PiiEntityRecognition",id:"piientityrecognition",level:2},{value:"SentimentAnalysis",id:"sentimentanalysis",level:2},{value:"Analyze Text with TextAnalyze",id:"analyze-text-with-textanalyze",level:2}],p={toc:c};function d(e){let{components:t,...a}=e;return(0,i.kt)("wrapper",(0,n.Z)({},p,a,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"analyze-text-with-synapseml-and-azure-ai-language"},"Analyze Text with SynapseML and Azure AI Language"),(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://learn.microsoft.com/azure/ai-services/language-service/overview"},"Azure AI Language")," is a cloud-based service that provides Natural Language Processing (NLP) features for understanding and analyzing text. Use this service to help build intelligent applications using the web-based Language Studio, REST APIs, and client libraries.\nYou can use SynapseML with Azure AI Language for ",(0,i.kt)("strong",{parentName:"p"},"named entity recognition"),", ",(0,i.kt)("strong",{parentName:"p"},"language detection"),", ",(0,i.kt)("strong",{parentName:"p"},"entity linking"),", ",(0,i.kt)("strong",{parentName:"p"},"key phrase extraction"),", ",(0,i.kt)("strong",{parentName:"p"},"Pii entity recognition")," and ",(0,i.kt)("strong",{parentName:"p"},"sentiment analysis"),"."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.services.language import AnalyzeText\nfrom synapse.ml.core.platform import find_secret\n\nai_service_key = find_secret(\n    secret_name="ai-services-api-key", keyvault="mmlspark-build-keys"\n)\nai_service_location = "eastus"\n')),(0,i.kt)("h2",{id:"named-entity-recognition"},"Named Entity Recognition"),(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://learn.microsoft.com/azure/ai-services/language-service/named-entity-recognition/overview"},"Named Entity Recognition")," is one of the features offered by Azure AI Language, a collection of machine learning and AI algorithms in the cloud for developing intelligent applications that involve written language. The NER feature can identify and categorize entities in unstructured text. For example: people, places, organizations, and quantities. Refer to ",(0,i.kt)("a",{parentName:"p",href:"https://learn.microsoft.com/azure/ai-services/language-service/named-entity-recognition/language-support?tabs=ga-api"},"this article")," for the full list of supported languages."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'df = spark.createDataFrame(\n    data=[\n        ["en", "Dr. Smith has a very modern medical office, and she has great staff."],\n        ["en", "I had a wonderful trip to Seattle last week."],\n    ],\n    schema=["language", "text"],\n)\n\nentity_recognition = (\n    AnalyzeText()\n    .setKind("EntityRecognition")\n    .setLocation(ai_service_location)\n    .setSubscriptionKey(ai_service_key)\n    .setTextCol("text")\n    .setOutputCol("entities")\n    .setErrorCol("error")\n    .setLanguageCol("language")\n)\n\ndf_results = entity_recognition.transform(df)\ndisplay(df_results.select("language", "text", "entities.documents.entities"))\n')),(0,i.kt)("p",null,"This cell should yield a result that looks like:"),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:"left"},"language"),(0,i.kt)("th",{parentName:"tr",align:"left"},"text"),(0,i.kt)("th",{parentName:"tr",align:"left"},"entities"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:"left"},"en"),(0,i.kt)("td",{parentName:"tr",align:"left"},"Dr. Smith has a very modern medical office, and she has great staff."),(0,i.kt)("td",{parentName:"tr",align:"left"},'[{"category": "Person", "confidenceScore": 0.98, "length": 5, "offset": 4, "subcategory": null, "text": "Smith"}, {"category": "Location", "confidenceScore": 0.79, "length": 14, "offset": 28, "subcategory": "Structural", "text": "medical office"}, {"category": "PersonType", "confidenceScore": 0.85, "length": 5, "offset": 62, "subcategory": null, "text": "staff"}]')),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:"left"},"en"),(0,i.kt)("td",{parentName:"tr",align:"left"},"I had a wonderful trip to Seattle last week."),(0,i.kt)("td",{parentName:"tr",align:"left"},'[{"category": "Event", "confidenceScore": 0.74, "length": 4, "offset": 18, "subcategory": null, "text": "trip"}, {"category": "Location", "confidenceScore": 1, "length": 7, "offset": 26, "subcategory": "GPE", "text": "Seattle"}, {"category": "DateTime", "confidenceScore": 0.8, "length": 9, "offset": 34, "subcategory": "DateRange", "text": "last week"}]')))),(0,i.kt)("h2",{id:"languagedetection"},"LanguageDetection"),(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://learn.microsoft.com/azure/ai-services/language-service/language-detection/overview"},"Language detection")," can detect the language a document is written in. It returns a language code for a wide range of languages, variants, dialects, and some regional/cultural languages. Refer to ",(0,i.kt)("a",{parentName:"p",href:"https://learn.microsoft.com/azure/ai-services/language-service/language-detection/language-support"},"this article")," for the full list of supported languages."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'df = spark.createDataFrame(\n    data=[\n        ["This is a document written in English."],\n        ["\u8fd9\u662f\u4e00\u4efd\u7528\u4e2d\u6587\u5199\u7684\u6587\u4ef6"],\n    ],\n    schema=["text"],\n)\n\nlanguage_detection = (\n    AnalyzeText()\n    .setKind("LanguageDetection")\n    .setLocation(ai_service_location)\n    .setSubscriptionKey(ai_service_key)\n    .setTextCol("text")\n    .setOutputCol("detected_language")\n    .setErrorCol("error")\n)\n\ndf_results = language_detection.transform(df)\ndisplay(df_results.select("text", "detected_language.documents.detectedLanguage"))\n')),(0,i.kt)("p",null,"This cell should yield a result that looks like:"),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:"left"},"text"),(0,i.kt)("th",{parentName:"tr",align:"left"},"detectedLanguage"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:"left"},"This is a document written in English."),(0,i.kt)("td",{parentName:"tr",align:"left"},'{"name": "English", "iso6391Name": "en", "confidenceScore": 0.99}')),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:"left"},"\u8fd9\u662f\u4e00\u4efd\u7528\u4e2d\u6587\u5199\u7684\u6587\u4ef6"),(0,i.kt)("td",{parentName:"tr",align:"left"},'{"name": "Chinese_Simplified", "iso6391Name": "zh_chs", "confidenceScore": 1}')))),(0,i.kt)("h2",{id:"entitylinking"},"EntityLinking"),(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://learn.microsoft.com/azure/ai-services/language-service/entity-linking/overview"},"Entity linking"),' identifies and disambiguates the identity of entities found in text. For example, in the sentence "We went to Seattle last week.", the word "Seattle" would be identified, with a link to more information on Wikipedia. ',(0,i.kt)("a",{parentName:"p",href:"https://learn.microsoft.com/azure/ai-services/language-service/entity-linking/language-support"},"English and Spanish are supported"),"."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'df = spark.createDataFrame(\n    data=[\n        ["Microsoft was founded by Bill Gates and Paul Allen on April 4, 1975."],\n        ["We went to Seattle last week."],\n    ],\n    schema=["text"],\n)\n\nentity_linking = (\n    AnalyzeText()\n    .setKind("EntityLinking")\n    .setLocation(ai_service_location)\n    .setSubscriptionKey(ai_service_key)\n    .setTextCol("text")\n    .setOutputCol("entity_linking")\n    .setErrorCol("error")\n)\n\ndf_results = entity_linking.transform(df)\ndisplay(df_results.select("text", "entity_linking.documents.entities"))\n')),(0,i.kt)("p",null,"This cell should yield a result that looks like:"),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:"left"},"text"),(0,i.kt)("th",{parentName:"tr",align:"left"},"entities"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:"left"},"Microsoft was founded by Bill Gates and Paul Allen on April 4, 1975."),(0,i.kt)("td",{parentName:"tr",align:"left"},'[{"bingId": "a093e9b9-90f5-a3d5-c4b8-5855e1b01f85", "dataSource": "Wikipedia", "id": "Microsoft", "language": "en", "matches": ','[{"confidenceScore": 0.48, "length": 9, "offset": 0, "text": "Microsoft"}]',', "name": "Microsoft", "url": "',(0,i.kt)("a",{parentName:"td",href:"https://en.wikipedia.org/wiki/Microsoft%22%7D"},'https://en.wikipedia.org/wiki/Microsoft"}'),', {"bingId": "0d47c987-0042-5576-15e8-97af601614fa", "dataSource": "Wikipedia", "id": "Bill Gates", "language": "en", "matches": ','[{"confidenceScore": 0.52, "length": 10, "offset": 25, "text": "Bill Gates"}]',', "name": "Bill Gates", "url": "',(0,i.kt)("a",{parentName:"td",href:"https://en.wikipedia.org/wiki/Bill_Gates%22%7D"},'https://en.wikipedia.org/wiki/Bill_Gates"}'),', {"bingId": "df2c4376-9923-6a54-893f-2ee5a5badbc7", "dataSource": "Wikipedia", "id": "Paul Allen", "language": "en", "matches": ','[{"confidenceScore": 0.54, "length": 10, "offset": 40, "text": "Paul Allen"}]',', "name": "Paul Allen", "url": "',(0,i.kt)("a",{parentName:"td",href:"https://en.wikipedia.org/wiki/Paul_Allen%22%7D"},'https://en.wikipedia.org/wiki/Paul_Allen"}'),', {"bingId": "52535f87-235e-b513-54fe-c03e4233ac6e", "dataSource": "Wikipedia", "id": "April 4", "language": "en", "matches": ','[{"confidenceScore": 0.38, "length": 7, "offset": 54, "text": "April 4"}]',', "name": "April 4", "url": "',(0,i.kt)("a",{parentName:"td",href:"https://en.wikipedia.org/wiki/April_4%22%7D%5D"},'https://en.wikipedia.org/wiki/April_4"}]'))),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:"left"},"We went to Seattle last week."),(0,i.kt)("td",{parentName:"tr",align:"left"},'[{"bingId": "5fbba6b8-85e1-4d41-9444-d9055436e473", "dataSource": "Wikipedia", "id": "Seattle", "language": "en", "matches": ','[{"confidenceScore": 0.17, "length": 7, "offset": 11, "text": "Seattle"}]',', "name": "Seattle", "url": "',(0,i.kt)("a",{parentName:"td",href:"https://en.wikipedia.org/wiki/Seattle%22%7D%5D"},'https://en.wikipedia.org/wiki/Seattle"}]'))))),(0,i.kt)("h2",{id:"keyphraseextraction"},"KeyPhraseExtraction"),(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://learn.microsoft.com/en-us/azure/ai-services/language-service/key-phrase-extraction/overview"},"Key phrase extraction"),' is one of the features offered by Azure AI Language, a collection of machine learning and AI algorithms in the cloud for developing intelligent applications that involve written language. Use key phrase extraction to quickly identify the main concepts in text. For example, in the text "The food was delicious and the staff were wonderful.", key phrase extraction will return the main topics: "food" and "wonderful staff". Refer to ',(0,i.kt)("a",{parentName:"p",href:"https://learn.microsoft.com/azure/ai-services/language-service/key-phrase-extraction/language-support"},"this article")," for the full list of supported languages."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'df = spark.createDataFrame(\n    data=[\n        ["Microsoft was founded by Bill Gates and Paul Allen on April 4, 1975."],\n        ["Dr. Smith has a very modern medical office, and she has great staff."],\n    ],\n    schema=["text"],\n)\n\nkey_phrase_extraction = (\n    AnalyzeText()\n    .setKind("KeyPhraseExtraction")\n    .setLocation(ai_service_location)\n    .setSubscriptionKey(ai_service_key)\n    .setTextCol("text")\n    .setOutputCol("key_phrase_extraction")\n    .setErrorCol("error")\n)\n\ndf_results = key_phrase_extraction.transform(df)\ndisplay(df_results.select("text", "key_phrase_extraction.documents.keyPhrases"))\n')),(0,i.kt)("p",null,"This cell should yield a result that looks like:"),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:"left"},"text"),(0,i.kt)("th",{parentName:"tr",align:"left"},"keyPhrases"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:"left"},"Microsoft was founded by Bill Gates and Paul Allen on April 4, 1975."),(0,i.kt)("td",{parentName:"tr",align:"left"},'["Bill Gates", "Paul Allen", "Microsoft", "April"]')),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:"left"},"Dr. Smith has a very modern medical office, and she has great staff."),(0,i.kt)("td",{parentName:"tr",align:"left"},'["modern medical office", "Dr. Smith", "great staff"]')))),(0,i.kt)("h2",{id:"piientityrecognition"},"PiiEntityRecognition"),(0,i.kt)("p",null,"The PII detection feature can identify, categorize, and redact sensitive information in unstructured text. For example: phone numbers, email addresses, and forms of identification. The method for utilizing PII in conversations is different than other use cases, and articles for this use have been separated. Refer to ",(0,i.kt)("a",{parentName:"p",href:"https://learn.microsoft.com/azure/ai-services/language-service/personally-identifiable-information/language-support?tabs=documents"},"this article")," for the full list of supported languages."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'df = spark.createDataFrame(\n    data=[\n        ["Call our office at 312-555-1234, or send an email to support@contoso.com"],\n        ["Dr. Smith has a very modern medical office, and she has great staff."],\n    ],\n    schema=["text"],\n)\n\npii_entity_recognition = (\n    AnalyzeText()\n    .setKind("PiiEntityRecognition")\n    .setLocation(ai_service_location)\n    .setSubscriptionKey(ai_service_key)\n    .setTextCol("text")\n    .setOutputCol("pii_entity_recognition")\n    .setErrorCol("error")\n)\n\ndf_results = pii_entity_recognition.transform(df)\ndisplay(df_results.select("text", "pii_entity_recognition.documents.entities"))\n')),(0,i.kt)("p",null,"This cell should yield a result that looks like:"),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:"left"},"text"),(0,i.kt)("th",{parentName:"tr",align:"left"},"entities"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:"left"},"Call our office at 312-555-1234, or send an email to ",(0,i.kt)("a",{parentName:"td",href:"mailto:support@contoso.com"},"support@contoso.com")),(0,i.kt)("td",{parentName:"tr",align:"left"},'[{"category": "PhoneNumber", "confidenceScore": 0.8, "length": 12, "offset": 19, "subcategory": null, "text": "312-555-1234"}, {"category": "Email", "confidenceScore": 0.8, "length": 19, "offset": 53, "subcategory": null, "text": "support@contoso.com"}]')),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:"left"},"Dr. Smith has a very modern medical office, and she has great staff."),(0,i.kt)("td",{parentName:"tr",align:"left"},'[{"category": "Person", "confidenceScore": 0.93, "length": 5, "offset": 4, "subcategory": null, "text": "Smith"}]')))),(0,i.kt)("h2",{id:"sentimentanalysis"},"SentimentAnalysis"),(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://learn.microsoft.com/en-us/azure/ai-services/language-service/sentiment-opinion-mining/overview"},"Sentiment analysis")," and opinion mining are features offered by the Language service, a collection of machine learning and AI algorithms in the cloud for developing intelligent applications that involve written language. These features help you find out what people think of your brand or topic by mining text for clues about positive or negative sentiment, and can associate them with specific aspects of the text. Refer to ",(0,i.kt)("a",{parentName:"p",href:"https://learn.microsoft.com/azure/ai-services/language-service/sentiment-opinion-mining/language-support"},"this article")," for the full list of supported languages."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'df = spark.createDataFrame(\n    data=[\n        ["The food and service were unacceptable. The concierge was nice, however."],\n        ["It taste great."],\n    ],\n    schema=["text"],\n)\n\nsentiment_analysis = (\n    AnalyzeText()\n    .setKind("SentimentAnalysis")\n    .setLocation(ai_service_location)\n    .setSubscriptionKey(ai_service_key)\n    .setTextCol("text")\n    .setOutputCol("sentiment_analysis")\n    .setErrorCol("error")\n)\n\ndf_results = sentiment_analysis.transform(df)\ndisplay(df_results.select("text", "sentiment_analysis.documents.sentiment"))\n')),(0,i.kt)("p",null,"This cell should yield a result that looks like:"),(0,i.kt)("table",null,(0,i.kt)("thead",{parentName:"table"},(0,i.kt)("tr",{parentName:"thead"},(0,i.kt)("th",{parentName:"tr",align:"left"},"text"),(0,i.kt)("th",{parentName:"tr",align:"left"},"sentiment"))),(0,i.kt)("tbody",{parentName:"table"},(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:"left"},"The food and service were unacceptable. The concierge was nice, however."),(0,i.kt)("td",{parentName:"tr",align:"left"},"mixed")),(0,i.kt)("tr",{parentName:"tbody"},(0,i.kt)("td",{parentName:"tr",align:"left"},"It tastes great."),(0,i.kt)("td",{parentName:"tr",align:"left"},"positive")))),(0,i.kt)("h2",{id:"analyze-text-with-textanalyze"},"Analyze Text with TextAnalyze"),(0,i.kt)("p",null,"Text Analyze is Deprecated, please use AnalyzeText instead"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'df = spark.createDataFrame(\n    data=[\n        ["en", "Hello Seattle"],\n        ["en", "There once was a dog who lived in London and thought she was a human"],\n    ],\n    schema=["language", "text"],\n)\n')),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.services import *\n\ntext_analyze = (\n    TextAnalyze()\n    .setLocation(ai_service_location)\n    .setSubscriptionKey(ai_service_key)\n    .setTextCol("text")\n    .setOutputCol("textAnalysis")\n    .setErrorCol("error")\n    .setLanguageCol("language")\n    .setEntityRecognitionParams(\n        {"model-version": "latest"}\n    )  # Can pass parameters to each model individually\n    .setIncludePii(False)  # Users can manually exclude tasks to speed up analysis\n    .setIncludeEntityLinking(False)\n    .setIncludeSentimentAnalysis(False)\n)\n\ndf_results = text_analyze.transform(df)\n')),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},"display(df_results)\n")))}d.isMDXComponent=!0}}]);