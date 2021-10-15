---
title: HttpOnSpark - Working with Arbitrary Web APIs
hide_title: true
status: stable
---
### Use "dogs as a service" in a distributed fashion with HTTP on Spark

In this example we will use the simple HTTP Transformer to call a public webAPI that returns random images of dogs. The service does not use the json payload, but this is for example purposes. 

A call to the dog service returns json objects structured like:

`{"status":"success","message":"https:\/\/images.dog.ceo\/breeds\/lhasa\/n02098413_2536.jpg"}`

If you visit the link you can download the image:

<img src="https://images.dog.ceo//breeds//lhasa//n02098413_2536.jpg"
      />





```python
import os

if os.environ.get("AZURE_SERVICE", None) == "Microsoft.ProjectArcadia":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

from pyspark.sql.functions import struct
from pyspark.sql.types import *
from synapse.ml.io.http import *

df = spark.createDataFrame([("foo",) for x in range(20)], ["data"]) \
      .withColumn("inputs", struct("data"))

response_schema = StructType().add("status", StringType()).add("message", StringType())

client = SimpleHTTPTransformer() \
  .setInputCol("inputs") \
  .setInputParser(JSONInputParser()) \
  .setOutputParser(JSONOutputParser().setDataType(response_schema)) \
  .setOutputCol("results") \
  .setUrl("https://dog.ceo/api/breeds/image/random")

responses = client.transform(df)
responses.select("results").show(truncate = False)
```
