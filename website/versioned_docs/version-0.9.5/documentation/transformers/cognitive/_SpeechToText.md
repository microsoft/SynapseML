import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";


## Speech To Text

### SpeechToText

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

stt.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
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

stt.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="SpeechToText"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.SpeechToText"
scala="com/microsoft/azure/synapse/ml/cognitive/SpeechToText.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/SpeechToText.scala" />


### SpeechToTextSDK

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

speech_to_text.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.cognitive._
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

speech_to_text.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="SpeechToTextSDK"
py="synapse.ml.cognitive.html#module-synapse.ml.cognitive.SpeechToTextSDK"
scala="com/microsoft/azure/synapse/ml/cognitive/SpeechToTextSDK.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/cognitive/src/main/scala/com/microsoft/azure/synapse/ml/cognitive/SpeechToTextSDK.scala" />
