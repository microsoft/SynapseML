import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";


## Text Analytics

### EntityDetector

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.cognitive import *

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

entity.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
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

entity.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="EntityDetector"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.EntityDetector"
scala="com/microsoft/azure/synapse/ml/cognitive/EntityDetector.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/TextAnalytics.scala" />


### KeyPhraseExtractor

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">


<!--pytest-codeblocks:cont-->

```python
from synapse.ml.cognitive import *

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

keyPhrase.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
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

keyPhrase.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="KeyPhraseExtractor"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.KeyPhraseExtractor"
scala="com/microsoft/azure/synapse/ml/cognitive/KeyPhraseExtractor.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/TextAnalytics.scala" />


### LanguageDetector

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.cognitive import *

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

language.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
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

language.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="LanguageDetector"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.LanguageDetector"
scala="com/microsoft/azure/synapse/ml/cognitive/LanguageDetector.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/TextAnalytics.scala" />


### NER

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.cognitive import *

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

ner.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
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

ner.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="NER"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.NER"
scala="com/microsoft/azure/synapse/ml/cognitive/NER.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/TextAnalytics.scala" />


### PII

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.cognitive import *

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
      .setOutputCol("response"))

pii.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
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

pii.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="PII"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.TextSentiment"
scala="com/microsoft/azure/synapse/ml/cognitive/TextSentiment.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/TextAnalytics.scala" />


### TextSentiment

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">




<!--pytest-codeblocks:cont-->

```python
from synapse.ml.cognitive import *

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

sentiment.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
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

sentiment.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="TextSentiment"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.TextSentiment"
scala="com/microsoft/azure/synapse/ml/cognitive/TextSentiment.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/TextAnalytics.scala" />
