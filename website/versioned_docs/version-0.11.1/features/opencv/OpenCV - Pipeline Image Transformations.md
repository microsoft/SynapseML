---
title: OpenCV - Pipeline Image Transformations
hide_title: true
status: stable
---
## OpenCV - Pipeline Image Transformations

This example shows how to manipulate the collection of images.
First, the images are downloaded to the local directory.
Second, they are copied to your cluster's attached HDFS.

The images are loaded from the directory (for fast prototyping, consider loading a fraction of
images). Inside the dataframe, each image is a single field in the image column. The image has
sub-fields (path, height, width, OpenCV type and OpenCV bytes).


```python
from pyspark.sql import SparkSession

# Bootstrap Spark Session
spark = SparkSession.builder.getOrCreate()

from synapse.ml.core.platform import running_on_synapse

if running_on_synapse():
    from notebookutils.visualization import display

import synapse.ml
import numpy as np
from synapse.ml.opencv import toNDArray
from synapse.ml.io import *

imageDir = "wasbs://publicwasb@mmlspark.blob.core.windows.net/sampleImages"
images = spark.read.image().load(imageDir).cache()
images.printSchema()
print(images.count())
```

We can also alternatively stream the images with a similar api.
Check the [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
for more details on streaming.


```python
import time

imageStream = spark.readStream.image().load(imageDir)
query = (
    imageStream.select("image.height")
    .writeStream.format("memory")
    .queryName("heights")
    .start()
)
time.sleep(3)
print("Streaming query activity: {}".format(query.isActive))
```

Wait a few seconds and then try querying for the images below.
Note that when streaming a directory of images that already exists it will
consume all images in a single batch. If one were to move images into the
directory, the streaming engine would pick up on them and send them as
another batch.


```python
heights = spark.sql("select * from heights")
print("Streamed {} heights".format(heights.count()))
```

After we have streamed the images we can stop the query:


```python
from py4j.protocol import Py4JJavaError

try:
    query.stop()
except Py4JJavaError as e:
    print(e)
```

When collected from the *DataFrame*, the image data are stored in a *Row*, which is Spark's way
to represent structures (in the current example, each dataframe row has a single Image, which
itself is a Row).  It is possible to address image fields by name and use `toNDArray()` helper
function to convert the image into numpy array for further manipulations.


```python
from synapse.ml.core.platform import running_on_binder

if running_on_binder():
    from IPython import get_ipython
from PIL import Image
import matplotlib.pyplot as plt

data = images.take(3)  # take first three rows of the dataframe
im = data[2][0]  # the image is in the first column of a given row

print("image type: {}, number of fields: {}".format(type(im), len(im)))
print("image path: {}".format(im.origin))
print("height: {}, width: {}, OpenCV type: {}".format(im.height, im.width, im.mode))

arr = toNDArray(im)  # convert to numpy array
print(images.count())
plt.imshow(Image.fromarray(arr, "RGB"))  # display the image inside notebook
```

Use `ImageTransformer` for the basic image manipulation: resizing, cropping, etc.
Internally, operations are pipelined and backed by OpenCV implementation.


```python
from synapse.ml.opencv import ImageTransformer

tr = (
    ImageTransformer()  # images are resized and then cropped
    .setOutputCol("transformed")
    .resize(size=(200, 200))
    .crop(0, 0, height=180, width=180)
)

small = tr.transform(images).select("transformed")

im = small.take(3)[2][0]  # take third image
plt.imshow(Image.fromarray(toNDArray(im), "RGB"))  # display the image inside notebook
```

For the advanced image manipulations, use Spark UDFs.
The SynapseML package provides conversion function between *Spark Row* and
*ndarray* image representations.


```python
from pyspark.sql.functions import udf
from synapse.ml.opencv import ImageSchema, toNDArray, toImage


def u(row):
    array = toNDArray(row)  # convert Image to numpy ndarray[height, width, 3]
    array[:, :, 2] = 0
    return toImage(array)  # numpy array back to Spark Row structure


noBlueUDF = udf(u, ImageSchema)

noblue = small.withColumn("noblue", noBlueUDF(small["transformed"])).select("noblue")

im = noblue.take(3)[2][0]  # take second image
plt.imshow(Image.fromarray(toNDArray(im), "RGB"))  # display the image inside notebook
```

Images could be unrolled into the dense 1D vectors suitable for CNTK evaluation.


```python
from synapse.ml.image import UnrollImage

unroller = UnrollImage().setInputCol("noblue").setOutputCol("unrolled")

unrolled = unroller.transform(noblue).select("unrolled")

vector = unrolled.take(1)[0][0]
print(type(vector))
len(vector.toArray())
```


```python

```
