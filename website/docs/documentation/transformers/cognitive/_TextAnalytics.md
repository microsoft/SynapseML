import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";

<!-- 
```python
import pyspark
import os
import json
import mmlspark
from IPython.display import display
from pyspark.sql.functions import col

spark = (pyspark.sql.SparkSession.builder.appName("MyApp")
        .config("spark.jars.packages", "com.microsoft.ml.spark:mmlspark:1.0.0-rc4")
        .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven")
        .getOrCreate())

def getSecret(secretName):
        get_secret_cmd = 'az keyvault secret show --vault-name mmlspark-build-keys --name {}'.format(secretName)
        value = json.loads(os.popen(get_secret_cmd).read())["value"]
        return value
``` 
-->

## EntityDetector

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.cognitive import *

textKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
df = spark.createDataFrame([
    ("1", "Microsoft released Windows 10"),
    ("2", "In 1975, Bill Gates III and Paul Allen founded the company.")
], ["id", "text"])

entity = (EntityDetector()
      .setSubscriptionKey(textKey)
      .setLocation("eastus")
      .setLanguage("en")
      .setOutputCol("replies")
      .setErrorCol("error"))

display(entity.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
import spark.implicits._
import org.apache.spark.sql.functions.{col, flatten}

val textKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df = Seq(
    ("1", "Microsoft released Windows 10"),
    ("2", "In 1975, Bill Gates III and Paul Allen founded the company.")
  ).toDF("id", "text")

val entity = (new EntityDetector()
            .setSubscriptionKey(textKey)
            .setLocation("eastus")
            .setLanguage("en")
            .setOutputCol("replies"))

display(entity.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="EntityDetector"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.EntityDetector"
scala="com/microsoft/ml/spark/cognitive/EntityDetector.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/TextAnalytics.scala" />


## KeyPhraseExtractor

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.cognitive import *

textKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
df = spark.createDataFrame([
    ("en", "Hello world. This is some input text that I love."),
    ("fr", "Bonjour tout le monde"),
    ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer.")
], ["lang", "text"])

keyPhrase = (KeyPhraseExtractor()
            .setSubscriptionKey(textKey)
            .setLocation("eastus")
            .setLanguageCol("lang")
            .setOutputCol("replies")
            .setErrorCol("error"))

display(keyPhrase.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
import spark.implicits._

val textKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df = Seq(
    ("en", "Hello world. This is some input text that I love."),
    ("fr", "Bonjour tout le monde"),
    ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
    ("en", null)
  ).toDF("lang", "text")

val keyPhrase = (new KeyPhraseExtractor()
                  .setSubscriptionKey(textKey)
                  .setLocation("eastus")
                  .setLanguageCol("lang")
                  .setOutputCol("replies"))

display(keyPhrase.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="KeyPhraseExtractor"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.KeyPhraseExtractor"
scala="com/microsoft/ml/spark/cognitive/KeyPhraseExtractor.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/TextAnalytics.scala" />


## LanguageDetector

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.cognitive import *

textKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
df = spark.createDataFrame([
  ("Hello World",),
  ("Bonjour tout le monde",),
  ("La carretera estaba atascada. Había mucho tráfico el día de ayer.",),
  ("你好",),
  ("こんにちは",),
  (":) :( :D",)
], ["text",])

language = (LanguageDetector()
            .setSubscriptionKey(textKey)
            .setLocation("eastus")
            .setTextCol("text")
            .setOutputCol("language")
            .setErrorCol("error"))

display(language.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
import spark.implicits._

val textKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df = Seq(
    "Hello World",
    "Bonjour tout le monde",
    "La carretera estaba atascada. Había mucho tráfico el día de ayer.",
    ":) :( :D"
  ).toDF("text")

val language = (new LanguageDetector()
      .setSubscriptionKey(textKey)
      .setLocation("eastus")
      .setOutputCol("replies"))

display(language.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="LanguageDetector"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.LanguageDetector"
scala="com/microsoft/ml/spark/cognitive/LanguageDetector.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/TextAnalytics.scala" />


## NER

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.cognitive import *

textKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
df = spark.createDataFrame([
    ("1", "en", "I had a wonderful trip to Seattle last week."),
    ("2", "en", "I visited Space Needle 2 times.")
], ["id", "language", "text"])

ner = (NER()
      .setSubscriptionKey(textKey)
      .setLocation("eastus")
      .setLanguageCol("language")
      .setOutputCol("replies")
      .setErrorCol("error"))

display(ner.transform(df)
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
import spark.implicits._

val textKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df = Seq(
    ("1", "en", "I had a wonderful trip to Seattle last week."),
    ("2", "en", "I visited Space Needle 2 times.")
  ).toDF("id", "language", "text")

val ner = (new NER()
            .setSubscriptionKey(textKey)
            .setLocation("eastus")
            .setLanguage("en")
            .setOutputCol("response"))

display(ner.transform(df)
```

</TabItem>
</Tabs>

<DocTable className="NER"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.NER"
scala="com/microsoft/ml/spark/cognitive/NER.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/TextAnalytics.scala" />


## PII

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.cognitive import *

textKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
df = spark.createDataFrame([
    ("1", "en", "My SSN is 859-98-0987"),
    ("2", "en",
      "Your ABA number - 111000025 - is the first 9 digits in the lower left hand corner of your personal check."),
    ("3", "en", "Is 998.214.865-68 your Brazilian CPF number?")
], ["id", "language", "text"])

pii = (PII()
      .setSubscriptionKey(textKey)
      .setLocation("eastus")
      .setLanguage("en")
      .setOutputCol("response")

display(pii.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
import spark.implicits._

val textKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df = Seq(
    ("1", "en", "My SSN is 859-98-0987"),
    ("2", "en",
      "Your ABA number - 111000025 - is the first 9 digits in the lower left hand corner of your personal check."),
    ("3", "en", "Is 998.214.865-68 your Brazilian CPF number?")
  ).toDF("id", "language", "text")

val pii = (new PII()
            .setSubscriptionKey(textKey)
            .setLocation("eastus")
            .setLanguage("en")
            .setOutputCol("response"))

display(pii.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="PII"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.TextSentiment"
scala="com/microsoft/ml/spark/cognitive/TextSentiment.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/TextAnalytics.scala" />


## TextSentiment

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from mmlspark.cognitive import *

textKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
df = spark.createDataFrame([
  ("I am so happy today, its sunny!", "en-US"),
  ("I am frustrated by this rush hour traffic", "en-US"),
  ("The cognitive services on spark aint bad", "en-US"),
], ["text", "language"])

sentiment = (TextSentiment()
            .setSubscriptionKey(textKey)
            .setLocation("eastus")
            .setTextCol("text")
            .setOutputCol("sentiment")
            .setErrorCol("error")
            .setLanguageCol("language"))

display(sentiment.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
import spark.implicits._

val textKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df = Seq(
    ("en", "Hello world. This is some input text that I love."),
    ("fr", "Bonjour tout le monde"),
    ("es", "La carretera estaba atascada. Había mucho tráfico el día de ayer."),
    (null, "ich bin ein berliner"),
    (null, null),
    ("en", null)
  ).toDF("lang", "text")

val sentiment = (new TextSentiment()
            .setSubscriptionKey(textKey)
            .setLocation("eastus")
            .setLanguageCol("lang")
            .setModelVersion("latest")
            .setShowStats(true)
            .setOutputCol("replies"))

display(sentiment.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="TextSentiment"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.TextSentiment"
scala="com/microsoft/ml/spark/cognitive/TextSentiment.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/TextAnalytics.scala" />


