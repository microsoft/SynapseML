# Azure Cognitive Services on Apache Spark™

## Azure Cognitive Services
Azure Cognitive Services are a suite of APIs, SDKs, and services available to help developers build intelligent applications without having direct AI or data science skills or knowledge. Azure Cognitive Services enable developers to easily add cognitive features into their applications. The goal of Azure Cognitive Services is to help developers create applications that can see, hear, speak, understand, and even begin to reason. The catalog of services within Azure Cognitive Services can be categorized into five main pillars - Vision, Speech, Language, Web Search, and Decision.

## Why use Cognitive Services on Spark?
Cognitive Services on Spark enable working with Azure’s Intelligent Services at massive scales with the Apache Spark™ distributed computing ecosystem. Cognitive Services on Spark allows users to embed general purpose and continuously improving intelligent models directly into their Apache Spark™ and SQL computations. This liberates developers from low-level networking details, so they can focus on creating intelligent, distributed applications. Each Cognitive Service acts as a  a SparkML transformer, so users can add services to existing SparkML pipelines.

## Usage
To see an example of Cognitive Services on Spark in action, refer to [this sample notebook](../notebooks/samples/CognitiveServices%20-%20Celebrity%20Quote%20Analysis.ipynb).

## Cognitive Services on Apache Spark
Currently, the following Cognitive Services are available on Apach Spark through MMLSpark:
### Vision
**Computer Vision**
- Describe
- Analyze (color, image type, face, adult/racy content)
- OCR
- Recognize Text
- Thumbnail
- Recognize domain-specific content

**Face**
- Detect
- Verify
- Identify
- Find similar
- Group

### Speech
**Speech Services**
- Speech-to-text

### Language
**Text Analytics**
- Language detection
- Key phrace extraction
- Named entity recognition
- Sentiment analysis

### Decision
**Anomaly Detector**
- Anomaly status of latest point
- Find anomalies

### Search
- Bing Image search