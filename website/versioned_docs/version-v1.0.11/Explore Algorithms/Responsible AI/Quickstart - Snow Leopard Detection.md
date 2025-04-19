---
title: Quickstart - Snow Leopard Detection
hide_title: true
status: stable
---
## Automated Snow Leopard Detection with Synapse Machine Learning

<img src="https://mmlspark.blob.core.windows.net/graphics/SnowLeopardAD/SLTrust.PNG" width="900" />


```python
from synapse.ml.core.platform import *

bing_search_key = find_secret(
    secret_name="bing-search-key", keyvault="mmlspark-build-keys"
)

# WARNING this notebook requires a lot of memory.
# If you get a heap space error, try dropping the number of images bing returns
# or by writing out the images to parquet first
```


```python
from synapse.ml.services import *
from synapse.ml.core.spark import FluentAPI
from pyspark.sql.functions import lit


def bingPhotoSearch(name, queries, pages):
    offsets = [offset * 10 for offset in range(0, pages)]
    parameters = [(query, offset) for offset in offsets for query in queries]

    return (
        spark.createDataFrame(parameters, ("queries", "offsets"))
        .mlTransform(
            BingImageSearch()  # Apply Bing Image Search
            .setSubscriptionKey(bing_search_key)  # Set the API Key
            .setOffsetCol("offsets")  # Specify a column containing the offsets
            .setQueryCol("queries")  # Specify a column containing the query words
            .setCount(10)  # Specify the number of images to return per offset
            .setImageType("photo")  # Specify a filter to ensure we get photos
            .setOutputCol("images")
        )
        .mlTransform(BingImageSearch.getUrlTransformer("images", "urls"))
        .withColumn("labels", lit(name))
        .limit(400)
    )
```

<img src="https://mmlspark.blob.core.windows.net/graphics/SparkSummit2/cog_services.png" width="900" />


```python
def displayDF(df, n=5, image_cols=set(["urls"])):
    rows = df.take(n)
    cols = df.columns
    header = "".join(["<th>" + c + "</th>" for c in cols])

    style = """
<!DOCTYPE html>
<html>
<head>

</head>"""

    table = []
    for row in rows:
        table.append("<tr>")
        for col in cols:
            if col in image_cols:
                rep = '<img src="{}",  width="100">'.format(row[col])
            else:
                rep = row[col]
            table.append("<td>{}</td>".format(rep))
        table.append("</tr>")
    tableHTML = "".join(table)

    body = """
<body>
<table>
  <tr>
    {} 
  </tr>
  {}
</table>
</body>
</html>
  """.format(
        header, tableHTML
    )
    try:
        if running_on_databricks():
            displayHTML(style + body)
        else:
            import IPython

            IPython.display.HTML(style + body)
    except:
        pass
```


```python
snowLeopardQueries = ["snow leopard"]
snowLeopardUrls = bingPhotoSearch("snow leopard", snowLeopardQueries, pages=100)
displayDF(snowLeopardUrls)
```


```python
randomWords = spark.read.parquet(
    "wasbs://publicwasb@mmlspark.blob.core.windows.net/random_words.parquet"
).cache()
randomWords.show()
```


```python
randomLinks = (
    randomWords.mlTransform(
        BingImageSearch()
        .setSubscriptionKey(bing_search_key)
        .setCount(10)
        .setQueryCol("words")
        .setOutputCol("images")
    )
    .mlTransform(BingImageSearch.getUrlTransformer("images", "urls"))
    .withColumn("label", lit("other"))
    .limit(400)
)

displayDF(randomLinks)
```


```python
images = (
    snowLeopardUrls.union(randomLinks)
    .distinct()
    .repartition(100)
    .mlTransform(
        BingImageSearch.downloadFromUrls("urls", "image", concurrency=5, timeout=5000)
    )
    .dropna()
)

train, test = images.randomSplit([0.7, 0.3], seed=1)
```


```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import udf
from synapse.ml.onnx import ImageFeaturizer
from synapse.ml.stages import UDFTransformer
from pyspark.sql.types import *


def getIndex(row):
    return float(row[1])


model = Pipeline(
    stages=[
        StringIndexer(inputCol="labels", outputCol="index"),
        ImageFeaturizer(
            inputCol="image",
            outputCol="features",
            autoConvertToColor=True,
            ignoreDecodingErrors=True,
        ).setModel("ResNet50"),
        LogisticRegression(maxIter=5, labelCol="index", regParam=10.0),
        UDFTransformer()
        .setUDF(udf(getIndex, DoubleType()))
        .setInputCol("probability")
        .setOutputCol("leopard_prob"),
    ]
)

fitModel = model.fit(train)
```

<img src="https://mmlspark.blob.core.windows.net/graphics/SnowLeopardAD/SLPipeline.PNG" width="900" />


```python
def plotConfusionMatrix(df, label, prediction, classLabels):
    from synapse.ml.plot import confusionMatrix
    import matplotlib.pyplot as plt

    fig = plt.figure(figsize=(4.5, 4.5))
    confusionMatrix(df, label, prediction, classLabels)
    display(fig)


if not running_on_synapse():
    plotConfusionMatrix(
        fitModel.transform(test), "index", "prediction", fitModel.stages[0].labels
    )
```


```python
import urllib.request
from synapse.ml.explainers import ImageLIME

test_image_url = (
    "https://mmlspark.blob.core.windows.net/graphics/SnowLeopardAD/snow_leopard1.jpg"
)
with urllib.request.urlopen(test_image_url) as url:
    barr = url.read()
test_subsample = spark.createDataFrame([(bytearray(barr),)], ["image"])

lime = (
    ImageLIME()
    .setModel(fitModel)
    .setTargetCol("leopard_prob")
    .setOutputCol("weights")
    .setInputCol("image")
    .setCellSize(100.0)
    .setModifier(50.0)
    .setNumSamples(300)
)

result = lime.transform(test_subsample)
```


```python
import matplotlib.pyplot as plt
import PIL, io, numpy as np


def plot_superpixels(row):
    image_bytes = row["image"]
    superpixels = row["superpixels"]["clusters"]
    weights = list(row["weights"][0])
    mean_weight = np.percentile(weights, 90)
    img = (PIL.Image.open(io.BytesIO(image_bytes))).convert("RGBA")
    image_array = np.asarray(img).copy()
    for (sp, w) in zip(superpixels, weights):
        if w > mean_weight:
            for (x, y) in sp:
                image_array[y, x, 1] = 255
                image_array[y, x, 3] = 200
    plt.clf()
    plt.imshow(image_array)
    display()


# Gets first row from the LIME-transformed data frame
if not running_on_synapse():
    plot_superpixels(result.take(1)[0])
```

### Your results will look like:
<img src="https://mmlspark.blob.core.windows.net/graphics/SnowLeopardAD/lime_results.png" width="900" />
