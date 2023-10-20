---
title: Quickstart - Create Audiobooks
hide_title: true
status: stable
---
# Create audiobooks using neural Text to speech

## Step 1: Load libraries and add service information


```python
from synapse.ml.core.platform import *

if running_on_synapse():
    from notebookutils import mssparkutils

# Fill this in with your cognitive service information
service_key = find_secret(
    "cognitive-api-key"
)  # Replace this line with a string like service_key = "dddjnbdkw9329"
service_loc = "eastus"

storage_container = "audiobooks"
storage_key = find_secret("madtest-storage-key")
storage_account = "anomalydetectiontest"
```

## Step 2: Attach the storage account to hold the audio files


```python
spark_key_setting = f"fs.azure.account.key.{storage_account}.blob.core.windows.net"
spark.sparkContext._jsc.hadoopConfiguration().set(spark_key_setting, storage_key)
```


```python
import os
from os.path import exists, join

mount_path = f"wasbs://{storage_container}@{storage_account}.blob.core.windows.net/"
if running_on_synapse():
    mount_dir = join("/synfs", mssparkutils.env.getJobId(), storage_container)
    if not exists(mount_dir):
        mssparkutils.fs.mount(
            mount_path, f"/{storage_container}", {"accountKey": storage_key}
        )
elif running_on_databricks():
    if not exists(f"/dbfs/mnt/{storage_container}"):
        dbutils.fs.mount(
            source=mount_path,
            mount_point=f"/mnt/{storage_container}",
            extra_configs={spark_key_setting: storage_key},
        )
```

## Step 3: Read in text data


```python
from pyspark.sql.functions import udf


@udf
def make_audio_filename(part):
    return f"wasbs://{storage_container}@{storage_account}.blob.core.windows.net/alice_in_wonderland/part_{part}.wav"


df = (
    spark.read.parquet(
        "wasbs://publicwasb@mmlspark.blob.core.windows.net/alice_in_wonderland.parquet"
    )
    .repartition(10)
    .withColumn("filename", make_audio_filename("part"))
)

display(df)
```

## Step 4: Synthesize audio from text

<div>
<img src="https://marhamilresearch4.blob.core.windows.net/gutenberg-public/Notebook/NeuralTTS_hero.jpeg" width="500" />
</div>


```python
from synapse.ml.cognitive import TextToSpeech

tts = (
    TextToSpeech()
    .setSubscriptionKey(service_key)
    .setTextCol("text")
    .setLocation(service_loc)
    .setErrorCol("error")
    .setVoiceName("en-US-SteffanNeural")
    .setOutputFileCol("filename")
)

audio = tts.transform(df).cache()
display(audio)
```

## Step 5: Listen to an audio file


```python
from IPython.display import Audio


def get_audio_file(num):
    if running_on_databricks():
        return f"/dbfs/mnt/{storage_container}/alice_in_wonderland/part_{num}.wav"
    else:
        return join(mount_dir, f"alice_in_wonderland/part_{num}.wav")


Audio(filename=get_audio_file(1))
```
