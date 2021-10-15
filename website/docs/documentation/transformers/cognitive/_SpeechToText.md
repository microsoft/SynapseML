import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";

<!-- 
```python
import pyspark
import os
import json
from IPython.display import display
from pyspark.sql.functions import col, collect_list, lit, sort_array, struct

spark = (pyspark.sql.SparkSession.builder.appName("MyApp")
        .config("spark.jars.packages", "com.microsoft.ml.spark:mmlspark:1.0.0-rc4")
        .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven")
        .getOrCreate())

def getSecret(secretName):
        get_secret_cmd = 'az keyvault secret show --vault-name mmlspark-build-keys --name {}'.format(secretName)
        value = json.loads(os.popen(get_secret_cmd).read())["value"]
        return value

import mmlspark
``` 
-->

## SpeechToText

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
import requests

cognitiveKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
link = "https://mmlspark.blob.core.windows.net/datasets/Speech/audio2.wav"
audioBytes = requests.get(link).content
df = spark.createDataFrame([(audioBytes,)
                           ], ["audio"])

stt = (SpeechToText()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setOutputCol("text")
    .setAudioDataCol("audio")
    .setLanguage("en-US")
    .setFormat("simple"))

display(stt.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
import spark.implicits._
import org.apache.commons.compress.utils.IOUtils
import java.net.URL

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val audioBytes = IOUtils.toByteArray(new URL("https://mmlspark.blob.core.windows.net/datasets/Speech/test1.wav").openStream())

val df: DataFrame = Seq(
    Tuple1(audioBytes)
  ).toDF("audio")

val stt = (new SpeechToText()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setOutputCol("text")
    .setAudioDataCol("audio")
    .setLanguage("en-US")
    .setFormat("simple"))

display(stt.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="SpeechToText"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.SpeechToText"
scala="com/microsoft/ml/spark/cognitive/SpeechToText.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/SpeechToText.scala" />


## SpeechToTextSDK

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
import requests

cognitiveKey = os.environ.get("COGNITIVE_API_KEY", getSecret("cognitive-api-key"))
df = spark.createDataFrame([("https://mmlspark.blob.core.windows.net/datasets/Speech/audio2.wav",)
                           ], ["url"])

speech_to_text = (SpeechToTextSDK()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setOutputCol("text")
    .setAudioDataCol("url")
    .setLanguage("en-US")
    .setProfanity("Masked"))

display(speech_to_text.transform(df))
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.ml.spark.cognitive._
import spark.implicits._
import org.apache.commons.compress.utils.IOUtils
import java.net.URL

val cognitiveKey = sys.env.getOrElse("COGNITIVE_API_KEY", None)
val df: DataFrame = Seq(
    "https://mmlspark.blob.core.windows.net/datasets/Speech/audio2.wav"
  ).toDF("url")

val speech_to_text = (new SpeechToTextSDK()
    .setSubscriptionKey(cognitiveKey)
    .setLocation("eastus")
    .setOutputCol("text")
    .setAudioDataCol("url")
    .setLanguage("en-US")
    .setProfanity("Masked"))

display(speech_to_text.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="SpeechToTextSDK"
py="mmlspark.cognitive.html#module-mmlspark.cognitive.SpeechToTextSDK"
scala="com/microsoft/ml/spark/cognitive/SpeechToTextSDK.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/ml/spark/cognitive/SpeechToTextSDK.scala" />

