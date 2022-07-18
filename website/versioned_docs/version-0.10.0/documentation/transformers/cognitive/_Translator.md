import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";


## Translator

### Translate

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

translatorKey = os.environ.get("TRANSLATOR_KEY", getSecret("translator-key"))
df = spark.createDataFrame([
  (["Hello, what is your name?", "Bye"],)
], ["text",])

translate = (Translate()
            .setSubscriptionKey(translatorKey)
            .setLocation("eastus")
            .setTextCol("text")
            .setToLanguage(["zh-Hans", "fr"])
            .setOutputCol("translation")
            .setConcurrency(5))

(translate
      .transform(df)
      .withColumn("translation", flatten(col("translation.translations")))
      .withColumn("translation", col("translation.text"))
      .select("translation")).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._
import org.apache.spark.sql.functions.{col, flatten}

val translatorKey = sys.env.getOrElse("TRANSLATOR_KEY", None)
val df = Seq(List("Hello, what is your name?", "Bye")).toDF("text")

val translate = (new Translate()
                  .setSubscriptionKey(translatorKey)
                  .setLocation("eastus")
                  .setTextCol("text")
                  .setToLanguage(Seq("zh-Hans", "fr"))
                  .setOutputCol("translation")
                  .setConcurrency(5))

(translate
      .transform(df)
      .withColumn("translation", flatten(col("translation.translations")))
      .withColumn("translation", col("translation.text"))
      .select("translation")).show()
```

</TabItem>
</Tabs>

<DocTable className="Translate"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.Translate"
scala="com/microsoft/azure/synapse/ml/cognitive/Translate.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/TextTranslator.scala" />


### Transliterate

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

translatorKey = os.environ.get("TRANSLATOR_KEY", getSecret("translator-key"))
df =  spark.createDataFrame([
  (["こんにちは", "さようなら"],)
], ["text",])

transliterate = (Transliterate()
            .setSubscriptionKey(translatorKey)
            .setLocation("eastus")
            .setLanguage("ja")
            .setFromScript("Jpan")
            .setToScript("Latn")
            .setTextCol("text")
            .setOutputCol("result"))

(transliterate
    .transform(df)
    .withColumn("text", col("result.text"))
    .withColumn("script", col("result.script"))
    .select("text", "script")).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._
import org.apache.spark.sql.functions.col

val translatorKey = sys.env.getOrElse("TRANSLATOR_KEY", None)
val df = Seq(List("こんにちは", "さようなら")).toDF("text")

val transliterate = (new Transliterate()
                        .setSubscriptionKey(translatorKey)
                        .setLocation("eastus")
                        .setLanguage("ja")
                        .setFromScript("Jpan")
                        .setToScript("Latn")
                        .setTextCol("text")
                        .setOutputCol("result"))

(transliterate
    .transform(df)
    .withColumn("text", col("result.text"))
    .withColumn("script", col("result.script"))
    .select("text", "script")).show()
```

</TabItem>
</Tabs>

<DocTable className="Transliterate"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.Transliterate"
scala="com/microsoft/azure/synapse/ml/cognitive/Transliterate.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/TextTranslator.scala" />


### Detect

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

translatorKey = os.environ.get("TRANSLATOR_KEY", getSecret("translator-key"))
df =  spark.createDataFrame([
  (["Hello, what is your name?"],)
], ["text",])

detect = (Detect()
      .setSubscriptionKey(translatorKey)
      .setLocation("eastus")
      .setTextCol("text")
      .setOutputCol("result"))

(detect
    .transform(df)
    .withColumn("language", col("result.language"))
    .select("language")).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._
import org.apache.spark.sql.functions.col

val translatorKey = sys.env.getOrElse("TRANSLATOR_KEY", None)
val df = Seq(List("Hello, what is your name?")).toDF("text")

val detect = (new Detect()
            .setSubscriptionKey(translatorKey)
            .setLocation("eastus")
            .setTextCol("text")
            .setOutputCol("result"))

(detect
    .transform(df)
    .withColumn("language", col("result.language"))
    .select("language")).show()
```

</TabItem>
</Tabs>

<DocTable className="Detect"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.Detect"
scala="com/microsoft/azure/synapse/ml/cognitive/Detect.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/TextTranslator.scala" />


### BreakSentence

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

translatorKey = os.environ.get("TRANSLATOR_KEY", getSecret("translator-key"))
df =  spark.createDataFrame([
  (["Hello, what is your name?"],)
], ["text",])

breakSentence = (BreakSentence()
            .setSubscriptionKey(translatorKey)
            .setLocation("eastus")
            .setTextCol("text")
            .setOutputCol("result"))

(breakSentence
    .transform(df)
    .withColumn("sentLen", flatten(col("result.sentLen")))
    .select("sentLen")).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._
import org.apache.spark.sql.functions.{col, flatten}

val translatorKey = sys.env.getOrElse("TRANSLATOR_KEY", None)
val df = Seq(List("Hello, what is your name?")).toDF("text")

val breakSentence = (new BreakSentence()
                        .setSubscriptionKey(translatorKey)
                        .setLocation("eastus")
                        .setTextCol("text")
                        .setOutputCol("result"))

(breakSentence
    .transform(df)
    .withColumn("sentLen", flatten(col("result.sentLen")))
    .select("sentLen")).show()
```

</TabItem>
</Tabs>

<DocTable className="BreakSentence"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.BreakSentence"
scala="com/microsoft/azure/synapse/ml/cognitive/BreakSentence.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/TextTranslator.scala" />


### DictionaryLookup

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

translatorKey = os.environ.get("TRANSLATOR_KEY", getSecret("translator-key"))
df = spark.createDataFrame([
  (["fly"],)
], ["text",])

dictionaryLookup = (DictionaryLookup()
                  .setSubscriptionKey(translatorKey)
                  .setLocation("eastus")
                  .setFromLanguage("en")
                  .setToLanguage("es")
                  .setTextCol("text")
                  .setOutputCol("result"))

(dictionaryLookup
    .transform(df)
    .withColumn("translations", flatten(col("result.translations")))
    .withColumn("normalizedTarget", col("translations.normalizedTarget"))
    .select("normalizedTarget")).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._
import org.apache.spark.sql.functions.{col, flatten}

val translatorKey = sys.env.getOrElse("TRANSLATOR_KEY", None)
val df = Seq(List("fly")).toDF("text")

val dictionaryLookup = (new DictionaryLookup()
                        .setSubscriptionKey(translatorKey)
                        .setLocation("eastus")
                        .setFromLanguage("en")
                        .setToLanguage("es")
                        .setTextCol("text")
                        .setOutputCol("result"))

(dictionaryLookup
      .transform(df)
      .withColumn("translations", flatten(col("result.translations")))
      .withColumn("normalizedTarget", col("translations.normalizedTarget"))
      .select("normalizedTarget")).show()
```

</TabItem>
</Tabs>

<DocTable className="DictionaryLookup"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.DictionaryLookup"
scala="com/microsoft/azure/synapse/ml/cognitive/DictionaryLookup.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/TextTranslator.scala" />


### DictionaryExamples

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

translatorKey = os.environ.get("TRANSLATOR_KEY", getSecret("translator-key"))
df = (spark.createDataFrame([
  ("fly", "volar")
], ["text", "translation"])
    .withColumn("textAndTranslation", array(struct(col("text"), col("translation")))))

dictionaryExamples = (DictionaryExamples()
                  .setSubscriptionKey(translatorKey)
                  .setLocation("eastus")
                  .setFromLanguage("en")
                  .setToLanguage("es")
                  .setTextAndTranslationCol("textAndTranslation")
                  .setOutputCol("result"))

(dictionaryExamples
    .transform(df)
    .withColumn("examples", flatten(col("result.examples")))
    .select("examples")).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._
import org.apache.spark.sql.functions.{col, flatten}

val translatorKey = sys.env.getOrElse("TRANSLATOR_KEY", None)
val df = Seq(List(TextAndTranslation("fly", "volar"))).toDF("textAndTranslation")

val dictionaryExamples = (new DictionaryExamples()
                        .setSubscriptionKey(translatorKey)
                        .setLocation("eastus")
                        .setFromLanguage("en")
                        .setToLanguage("es")
                        .setTextAndTranslationCol("textAndTranslation")
                        .setOutputCol("result"))

(dictionaryExamples
    .transform(df)
    .withColumn("examples", flatten(col("result.examples")))
    .select("examples")).show()
```

</TabItem>
</Tabs>

<DocTable className="DictionaryExamples"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.DictionaryExamples"
scala="com/microsoft/azure/synapse/ml/cognitive/DictionaryExamples.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/TextTranslator.scala" />


### DocumentTranslator

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

translatorKey = os.environ.get("TRANSLATOR_KEY", getSecret("translator-key"))
translatorName = os.environ.get("TRANSLATOR_NAME", "mmlspark-translator")

documentTranslator = (DocumentTranslator()
                  .setSubscriptionKey(translatorKey)
                  .setServiceName(translatorName)
                  .setSourceUrlCol("sourceUrl")
                  .setTargetsCol("targets")
                  .setOutputCol("translationStatus"))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
import spark.implicits._

val translatorKey = sys.env.getOrElse("TRANSLATOR_KEY", None)
val translatorName = sys.env.getOrElse("TRANSLATOR_NAME", None)

val documentTranslator = (new DocumentTranslator()
                        .setSubscriptionKey(translatorKey)
                        .setServiceName(translatorName)
                        .setSourceUrlCol("sourceUrl")
                        .setTargetsCol("targets")
                        .setOutputCol("translationStatus"))
```

</TabItem>
</Tabs>

<DocTable className="DocumentTranslator"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.DocumentTranslator"
scala="com/microsoft/azure/synapse/ml/cognitive/DocumentTranslator.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/DocumentTranslator.scala" />
