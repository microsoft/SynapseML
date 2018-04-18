# <img src="https://mmlspark.blob.core.windows.net/graphics/SparkServing3.svg" width="90" align="left"> MMLSpark Serving

### An engine for deploying Spark jobs as distributed web services

- **Distributed**: Takes full advantage of Node, JVM, and thread level
  parallelism that Spark is famous for.
- **Fast**: No single node bottlenecks, no round trips to Python.
  Requests can be routed directly to and from worker JVMs through
  network switches.  Spin up a web service in a matter of seconds.
- **Deployable Anywhere**: Works anywhere that runs Spark such as
  Databricks, HDInsight, AZTK, DSVMs, local, or on your own
  cluster.  Usable from Spark, PySpark, and SparklyR.
- **Lightweight**: No dependence on additional (costly) Kafka or
  Kubernetes clusters.
- **Idiomatic**: Uses the same API as batch and structured streaming.
- **Flexible**: Spin up and manage several services on a single Spark
  cluster.  Synchronous and Asynchronous service management and
  extensibility.  Deploy any spark job that is expressible as a
  structured streaming query.  Use serving sources/sinks with other
  Spark data sources/sinks for more complex deployments.

## Usage

### MMLSpark Serving Hello World

   ```python
   import mmlspark
   import pyspark
   from pyspark.sql.functions import udf, col, length

   serving_source = "org.apache.spark.sql.execution.streaming.HTTPSourceProvider"
   serving_sink = "org.apache.spark.sql.execution.streaming.HTTPSinkProvider"

   df = spark.readStream.format(serving_source) \
       .option("host", "localhost") \
       .option("port", 8888) \
       .option("name", "my_api") \
       .load()

   server = df.withColumn("newCol", length(col("value"))) \
       .writeStream \
       .format(serving_sink) \
       .option("name", "my_api") \
       .queryName("my_query") \
       .option("replyCol", "newCol") \
       .option("checkpointLocation", "file:///path/to/checkpoints") \
       .start()
   ```

### Deploying a Deep Network with the CNTKModel

   ```python
   import mmlspark
   from mmlspark import CNTKModel
   import pyspark
   from pyspark.sql.functions import udf, col

   serving_source = "org.apache.spark.sql.execution.streaming.HTTPSourceProvider"
   serving_sink = "org.apache.spark.sql.execution.streaming.HTTPSinkProvider"

   df = spark.readStream.format(serving_source) \
          .option("host", "localhost") \
          .option("port", 8888) \
          .option("name", "my_api") \
          .load()

   # See notebook examples for how to create and save several
   # examples of CNTK models
   network = CNTKModel.load("file:///path/to/my_cntkmodel.mml")

   transformed_df = network.transform(df)

   server = transformed_df \
              .writeStream \
              .format(serving_sink) \
              .option("name", "my_api") \
              .queryName("my_query") \
              .option("replyCol", network.getOutputCol()) \
              .option("checkpointLocation", "file:///path/to/checkpoints") \
              .start()
   ```

## Architecture

MMLSpark serving adds special streaming sources sinks to turn any
structured streaming job into a web service.  MMLSpark serving comes
with two deployment options that vary based on the load balancing used:

### Head Node Load Balanced

You can deploy head node load balancing with the `HTTPSource` and
`HTTPSink` classes.  This mode spins up a queue on the head node,
distributes work across partitions, then collects response data back to
the head node.  All HTTP requests are kept and replied to on the head
node.  This mode allows for more complex windowing, repartitioning, and
SQL operations.  This option is also idea for rapid setup and testing,
as it doesn't require any additional load balancing or network
switches.A diagram of this configuration can be seen below:

<p align="center">
  <img src="https://mmlspark.blob.core.windows.net/graphics/HeadNodeDistributed2.png" width="600">
</p>

### Fully Distributed (Custom Load Balancer)

You can configure MMLSpark serving for a custom load balancer using the
`DistributedHTTPSource` and `DistributedHTTPSink` classes.  This mode
spins up servers on each executor JVM.  Each server will feed its
executor's partitions in parallel.  This mode is key for high throughput
and low latency as data does not need to be transferred to and from the
head node.  This deployment results in several web services that all
route into the same spark computation.  You can deploy an external load
balancer to unify the executor's services under a single IP address.
Support for automatic load balancer management and deployment is
targeted for the next release of MMLSpark.  A diagram of this
configuration can be seen below:

<p align="center">
  <img src="https://mmlspark.blob.core.windows.net/graphics/FullyDistributed2.png" width="600">
</p>

Queries that involve data movement across workers, such as a nontrivial
SQL join, need special consideration.  The user must ensure that the
right machine replies to each request.  One can route data back to the
originating partition with a broadcast join.  In the future, request
routing will be automatically handled by the sink.
