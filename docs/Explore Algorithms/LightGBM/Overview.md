---
title: Overview
hide_title: true
sidebar_label: Overview
---

# LightGBM on Apache Spark

### LightGBM

[LightGBM](https://github.com/lightgbm-org/LightGBM) is an open-source,
distributed, high-performance gradient boosting (GBDT, GBRT, GBM, or
MART) framework. This framework specializes in creating high-quality and
GPU enabled decision tree algorithms for ranking, classification, and
many other machine learning tasks. LightGBM is part of Microsoft's
[DMTK](http://github.com/microsoft/dmtk) project.

### Advantages of LightGBM through SynapseML

-   **Composability**: LightGBM models can be incorporated into existing
    SparkML Pipelines, and used for batch, streaming, and serving
    workloads.
-   **Performance**: LightGBM on Spark is 10-30% faster than SparkML on
    the Higgs dataset, and achieves a 15% increase in AUC.  [Parallel
    experiments](https://github.com/lightgbm-org/LightGBM/blob/master/docs/Experiments.rst#parallel-experiment)
    have verified that LightGBM can achieve a linear speed-up by using
    multiple machines for training in specific settings.
-   **Functionality**: LightGBM offers a wide array of [tunable
    parameters](https://github.com/lightgbm-org/LightGBM/blob/master/docs/Parameters.rst),
    that one can use to customize their decision tree system. LightGBM on
    Spark also supports new types of problems such as quantile regression.
-   **Cross platform** LightGBM on Spark is available on Spark, PySpark, and SparklyR

### Usage

In PySpark, you can run the `LightGBMClassifier` via:

```python
from synapse.ml.lightgbm import LightGBMClassifier
model = LightGBMClassifier(learningRate=0.3,
                           numIterations=100,
                           numLeaves=31).fit(train)
```

Similarly, you can run the `LightGBMRegressor` by setting the
`application` and `alpha` parameters:

```python
from synapse.ml.lightgbm import LightGBMRegressor
model = LightGBMRegressor(application='quantile',
                          alpha=0.3,
                          learningRate=0.3,
                          numIterations=100,
                          numLeaves=31).fit(train)
```

For an end to end application, check out the LightGBM [notebook
example](../Quickstart%20-%20Classification,%20Ranking,%20and%20Regression).

### Arguments/Parameters

SynapseML exposes getters/setters for many common LightGBM parameters.
In python, you can use property-value pairs, or in Scala use
fluent setters. Examples of both are shown in this section.

```scala
import com.microsoft.azure.synapse.ml.lightgbm.LightGBMClassifier
val classifier = new LightGBMClassifier()
                       .setLearningRate(0.2)
                       .setNumLeaves(50)
```

LightGBM has far more parameters than SynapseML exposes. For cases where you
need to set some parameters that SynapseML doesn't expose a setter for, use
passThroughArgs. This argument is just a free string that you can use to add extra parameters
to the command SynapseML sends to configure LightGBM.

In python:
```python
from synapse.ml.lightgbm import LightGBMClassifier
model = LightGBMClassifier(passThroughArgs="force_row_wise=true min_sum_hessian_in_leaf=2e-3",
                           numIterations=100,
                           numLeaves=31).fit(train)
```

In Scala:
```scala
import com.microsoft.azure.synapse.ml.lightgbm.LightGBMClassifier
val classifier = new LightGBMClassifier()
                      .setPassThroughArgs("force_row_wise=true min_sum_hessian_in_leaf=2e-3")
                      .setLearningRate(0.2)
                      .setNumLeaves(50)
```

For formatting options and specific argument documentation, see
[LightGBM docs](https://lightgbm.readthedocs.io/en/v3.3.2/Parameters.html). SynapseML sets some
parameters specifically for the Spark distributed environment and
shouldn't be changed. Some parameters are for CLI mode only, and don't work within
Spark. 

You can mix *passThroughArgs* and explicit args, as shown in the example. SynapseML
merges them to create one argument string to send to LightGBM. If you set a parameter in
both places, *passThroughArgs* takes precedence.

#### GPU training with a custom OpenCL native library

SynapseML's published `lightgbmlib` artifact contains CPU-only native libraries. Only
`deviceType="gpu"` selects an accelerator: it selects LightGBM's OpenCL learner and
requires a compatible custom native library. All `cuda` requests are rejected before
native training because LightGBM 3.3.510 CUDA is incompatible with SynapseML streaming
Datasets.

Accelerator training is intended for users who provide their own compatible LightGBM
native build. Put both `lib_lightgbm` and `lib_lightgbm_swig` on `java.library.path` for
the Spark driver and every executor before LightGBM is initialized. `NativeLoader` checks
that path first and falls back to the CPU-only libraries packaged in the SynapseML JAR.
For example, a Spark deployment can set both `spark.driver.extraLibraryPath` and
`spark.executor.extraLibraryPath` to the directory containing the custom libraries.
The custom SWIG library must be ABI-compatible with the Java classes shipped by the
SynapseML version in use; supplying only one library can accidentally mix incompatible
custom and bundled binaries.

SynapseML does not support `deviceType="cuda"` with `lightgbmlib` 3.3.510. Its CUDA
objective expects CUDA metadata that is not created by the serialized streaming Dataset
path and can segfault the Spark executor during booster creation. SynapseML rejects CUDA
before native training. Use `deviceType="gpu"` with an OpenCL-enabled native build; this
path supports classifier, regressor, and ranker training on NVIDIA GPUs such as T4.

`deviceType` exposes only the accelerator backends implemented by LightGBM:

| Value | Backend | Hardware |
| --- | --- | --- |
| `cpu` | Native CPU learner | Supported by the bundled SynapseML native library |
| `gpu` | OpenCL learner (`USE_GPU=1`) | AMD, Intel, or NVIDIA devices with a working OpenCL runtime |
| `cuda` | Unsupported with SynapseML's LightGBM 3.3.510 streaming Dataset path | Do not use |

Apple Metal Performance Shaders (`mps`) and Habana HPU are not LightGBM tree-learning
backends and are therefore not accepted values. Apple Silicon can only be evaluated
through LightGBM's OpenCL learner and a custom macOS ARM64 native build; this is not MPS
support, Apple has deprecated OpenCL, and LightGBM documents a macOS Boost.Compute cache
workaround. Do not claim Apple Silicon GPU support without testing that exact native build,
Spark/JVM architecture, dataset correctness, and performance on supported macOS hardware.

After installing the custom native libraries, select the learner explicitly:

```python
model = LightGBMClassifier(deviceType="gpu").fit(train)
```

The default is `cpu` and does not add a `device_type` native parameter. If
*passThroughArgs* contains `device_type`, that canonical value takes precedence over both
the `device` alias and `deviceType`, regardless of argument order. If only `device` is
present, it takes precedence over `deviceType`. A native-effective value of `cuda` is
always rejected. If a requested OpenCL accelerator is unavailable, SynapseML reports that
the bundled native is CPU-only and identifies the custom-library configuration required.

### Architecture

LightGBM on Spark uses the Simple Wrapper and Interface Generator (SWIG)
to add Java support for LightGBM. These Java Binding use the Java Native
Interface call into the [distributed C++
API](https://github.com/lightgbm-org/LightGBM/blob/master/include/LightGBM/c_api.h).

We initialize LightGBM by calling
[`LGBM_NetworkInit`](https://github.com/lightgbm-org/LightGBM/blob/master/include/LightGBM/c_api.h)
with the Spark executors within a MapPartitions call. We then pass each
workers partitions into LightGBM to create the in-memory distributed
dataset for LightGBM.  We can then train LightGBM to produce a model
that can then be used for inference.

The `LightGBMClassifier` and `LightGBMRegressor` use the SparkML API,
inherit from the same base classes, integrate with SparkML pipelines,
and can be tuned with [SparkML's cross
validators](https://spark.apache.org/docs/latest/ml-tuning.html).

Models built can be saved as SparkML pipeline with native LightGBM model
using `saveNativeModel()`. Additionally, they're fully compatible with [PMML](https://en.wikipedia.org/wiki/Predictive_Model_Markup_Language) and
can be converted to PMML format through the
[JPMML-SparkML-LightGBM](https://github.com/alipay/jpmml-sparkml-lightgbm) plugin.

#### Dynamic Allocation Limitations
The native LightGBM library has a *distributed mode* that allows the algorithm to work over multiple *machines*. SynapseML
uses this mode to call LightGBM from Spark. SynapseML first gathers all the Spark executor networking information, passes that to LightGBM, and then
waits for LightGBM to complete its work. However, the native LightGBM algorithm implementation assumes all networking is constant over the time period of a single
training or scoring session. The native LightGBM distributed mode was designed this way and isn't a limitation of SynapseML by itself.

Dynamic compute changes can cause LightGBM problems if the Spark executors change during data processing. Spark can naturally
take advantage of cluster autoscaling and can also dynamically replace any failed executor with another, but LightGBM can't
handle these networking changes. Large datasets are affected in particular since they're more likely to cause executor scaling
or have a single executor fail during a single processing pass.

If you're experiencing problems with LightGBM as exposed through SynapseML due to executor changes (for example, occasional Task failures or networking hangs),
there are several options.
1. In the Spark platform, turn off any autoscaling on the cluster you have provisioned.
2. Set *numTasks* manually to be smaller so that fewer executors are used (reducing probability of single executor failure).
3. Turn off dynamic executor scaling with configuration in a notebook cell. In Synapse and Fabric, you can use:

```python
   %%configure
   {
       "conf":
       {
           "spark.dynamicAllocation.enabled": "false"
       }
   }
```
Note: setting any custom configuration can affect cluster startup time if your compute platform takes advantage of "live pools"
to improve notebook performance.

If you still have problems, you can consider splitting your data into smaller segments using *numBatches*. Splitting into multiple
batches increases total processing time, but can potentially be used to increase reliability.

### Data Transfer Mode

SynapseML must pass data from Spark partitions to LightGBM native Datasets before turning over control to
the actual LightGBM execution code for training and inference. SynapseML has two modes
that control how this data is transferred: *streaming* and *bulk*.
This mode doesn't affect training but can affect memory usage and overall fit/transform time.
By default, SynapseML uses "streaming" mode.

#### Bulk Execution mode
The "Bulk" mode is older and requires accumulating all data in executor memory before creating Datasets. This mode can cause
OOM errors for large data, especially since the data must be accumulated in its original uncompressed double-format size.

For bulk mode, native LightGBM Datasets can either be created per partition (useSingleDatasetMode=false), or
per executor (useSingleDatasetMode=true). Generally, one Dataset per executor is more efficient since it reduces LightGBM network size and complexity during training or fitting. It also avoids using slow network protocols on partitions
that are actually on the same executor node.

#### Streaming Execution Mode
The "streaming" execution mode uses new native LightGBM APIs created just for SynapseML that don't require loading extra copies of the data into memory. In particular, data is passed directly
from partitions to Datasets in small "micro-batches", similar to Spark streaming. The `microBatchSize` parameter controls the size of these micro-batches.
Smaller micro-batch sizes reduce memory overhead, but larger sizes avoid overhead from repeatedly transferring data to the native layer. The default
100, uses far less memory than bulk mode since only 100 rows of data will be loaded at a time. If your dataset has
few columns, you can increase the batch size. Alternatively, if
your dataset has a large number of columns you can decrease the micro-batch size to avoid OOM issues.

These new streaming APIs in LightGBM are thread-safe, and allow all partitions in the same executor
to push data into a shared Dataset in parallel. Because of this, streaming mode always uses the more efficient
"useSingleDatasetMode=true", creating only one Dataset per executor.

You can explicitly specify Execution Mode and MicroBatch size as parameters.

    val lgbm = new LightGBMClassifier()
        .setExecutionMode("streaming")
        .setMicroBatchSize(100)
        .setLabelCol(labelColumn)
        .setObjective("binary")
    ...
    <train classifier>

For streaming mode, only one Dataset is created per partition, so *useSingleDataMode* has no effect. It's effectively always true.

### Data Sampling

In order for LightGBM algorithm to work, it must first create a set of bin boundaries for optimization. It does this calculation by
first sampling the data before any training or inferencing starts. ([LightGBM docs](https://github.com/lightgbm-org/LightGBM)). The number of
samples to use is set using *binSampleCount*, which must be a minimal percent of the data or LightGBM rejects it.

For *bulk* mode, this sampling is automatically done over the entire data, and each executor uses its own partitions to calculate samples for only
a subset of the features. This distributed sampling can have subtle effects since partitioning can affect the calculated bins.
Also, all data is sampled no matter what.

For *streaming* mode, there are more explicit user controls for this sampling, and it's all done from the driver.
The *samplingMode* property controls the behavior. The efficiency of these methods increases from first to last.
- *global* - Like bulk mode, the random sample is calculated by iterating over entire data (hence data is traversed twice)
- *subset* - (default) Samples only from the first *samplingSubsetSize* elements. Assumes this subset is representative.
- *fixed* - There's no random sample. The first *binSampleSize* rows are used. Assumes randomized data.
For large row counts, *subset* and *fixed* modes can save a first iteration over the entire data.

#### Reference Dataset
The sampling of the data to calculate bin boundaries happens every *fit* call.
If repeating a fit many times (for example, hyperparameter tuning), this calculation is duplicated effort.

For *streaming* mode, there's an optimization that a client can set to use the previously calculated bin boundaries. The
sampling calculation results in a *reference dataset*, which can be reused. After a fit, there will be a *referenceDataset* property
on the estimator that was calculated and used for that fit. If that is set on the next estimator (or you reuse the same one),
it will use that instead of resampling the data.

```python
from synapse.ml.lightgbm import LightGBMClassifier
classifier = LightGBMClassifier(learningRate=0.3,
                                numIterations=100,
                                numLeaves=31)
model1 = classifier.fit(train)

classifier.learningRate = 0.4
model2 = classifier.fit(train)
```
The 'model2' call to 'fit' doesn't resample the data and uses the same bin boundaries as 'model1'.

*Caution*: Some parameters actually affect the bin boundary calculation and require the use of a new reference dataset every time.
These parameters include *isEnableSparse*, *useMissing*, and *zeroAsMissing* that you can set from SynapseML. If you manually set
some parameters with *passThroughArgs*, you should look at LightGBM docs to see if they affect bin boundaries. If you're setting
any parameter that affects bin boundaries and reusing the same estimator, you should set referenceDataset to an empty array between calls.

### Barrier Execution Mode

By default LightGBM uses the regular spark paradigm for launching tasks and communicates with the driver to coordinate task execution.
The driver thread aggregates all task host:port information and then communicates the full list back to the workers in order for NetworkInit to be called.
This procedure requires the driver to know how many tasks there are, and a mismatch between the expected number of tasks and the actual number causes
the initialization to deadlock.

If you're experiencing network issues, you can try using Spark's *barrier* execution mode. SynapseML provides a `UseBarrierExecutionMode` flag,
to use Apache Spark's `barrier()` stage to ensure all tasks execute at the same time.
Barrier execution mode changes the logic to aggregate `host:port` information across all tasks in a synchronized way.
To use it in scala, you can call setUseBarrierExecutionMode(true), for example:

    val lgbm = new LightGBMClassifier()
        .setLabelCol(labelColumn)
        .setObjective(binaryObjective)
        .setUseBarrierExecutionMode(true)
    ...
    <train classifier>
Note: barrier execution mode can also cause complicated issues, so use it only if needed.

Barrier execution mode is also the only mode that can recover from a task failure that happens after the
network topology has been negotiated. A LightGBM network is negotiated once and then fixed, so an individual
Spark task retry can never rejoin it. Spark restarts a barrier stage in its entirety, and the driver serves a
fresh topology round for each stage attempt, so training can survive a failure that would otherwise abort the
job. Failures that happen before a task joins the network are still retried normally in either mode.

### Diagnosing "Connection refused" during training

Distributed training first exchanges `host:port` information with the driver, which serves that exchange
once per stage attempt. If a task fails after that exchange, the regular (non-barrier) retry of that task
reconnects to a driver endpoint that is no longer accepting connections. Because Spark only reports the
most recent attempt, this can hide the failure that actually caused the retry.

When this happens, the reported error explains that it's a retry that could not rejoin the network, and
names the partition to investigate. Look for the **first** failed attempt of that partition in the executor
logs — that attempt holds the real cause.

### IPv6 clusters

Distributed training works on clusters whose executors only have IPv6 addresses.

The native LightGBM library is IPv4-only in every released version: it splits each machine list entry on
`:` and keeps the entry only when that yields exactly two parts, and it builds every socket with `AF_INET`
and `inet_pton(AF_INET, ...)`. An IPv6 endpoint is therefore dropped or misread by the native parser, and
could not be dialed or accepted even if it survived parsing.

SynapseML handles this itself. The topology exchange publishes IPv6 endpoints in the unambiguous
`[address]:port` form, and each task then bridges the transport for the native library:

- the port a task advertised to its peers is owned by SynapseML, which accepts peer connections over
  either address family and forwards them to the native listener over IPv4 loopback;
- each IPv6 peer gets an IPv4 loopback relay that forwards what the native library sends to that peer's
  real IPv6 address;
- the machine list handed to the native library has the same entries in the same order, with every bridged
  endpoint rewritten to `127.0.0.1:port` and an explicit rank, so LightGBM ranks are unchanged.

An IPv4 machine list is passed to the native library exactly as before, with no relay and no rewriting, so
IPv4 clusters see no behavior or performance change. On an IPv6 cluster, peer traffic takes one extra
loopback hop on each side, and IPv4 loopback must be available on the executors. Measured on a 16 core
developer machine over loopback, a bridged link sustains roughly a third of the throughput of a direct one
(about 0.6 GB/s per direction) for about four times the CPU per transferred byte, and adds roughly 200
microseconds to a small message round trip per hop. Links faster than a few Gb/s can therefore be limited
by the bridge rather than by the network. The relays never buffer in the JVM heap: a reader that stops
reading stops the sender, with only the socket buffers in between.

The advertised port is the one address peers know, so SynapseML binds it on every interface, exactly as the
native listener did before this change, and performs the LightGBM link handshake there itself. A machine
opens a LightGBM link by sending its rank, so a connection has to produce a valid, unused, lower rank within
a timeout before any of its bytes reach the native library; connections that stall, repeat a rank, or claim
a rank the topology does not have are closed by the bridge. That matters because the native accept loop has
no timeout and treats the first four bytes of any connection as a rank.

The native listener itself is the one thing this library cannot rebind: `TcpSocket::Bind` hardcodes
`0.0.0.0`, so while it is open it is reachable on every IPv4 interface. The bridge therefore claims each of
that listener's link slots itself, over IPv4 loopback, as soon as the port is bound — one slot per lower
rank, which is exactly how many the native library accepts before closing the listener. In practice the
listener is open for milliseconds on an unadvertised ephemeral port rather than for the whole handshake
phase, and every byte the native library reads from it comes from the bridge. Closing that window entirely
requires an upstream change to LightGBM: `TcpSocket::Bind` would have to take a bind address so that
`Linkers::TryBind` can pass a loopback one. Until then, a LightGBM training port must only be reachable
from the cluster's own executors, which was already true before this change.

The per peer relays are bound to IPv4 loopback and are not reachable from outside the machine, the number of
links a bridge will relay is capped at twice the machine count, and each lower rank may link only once. One
event loop thread serves every listener and every link, so a bridge runs exactly one thread whatever the
machine count is, with two fixed size buffers per link.

A connection the topology cannot need — an unsolicited one, one past the cap, or one that fails its
handshake — is refused without consuming any of that budget, so it cannot starve the links the native
library itself opens. Transport failures are retried while they can be: a dial that fails, immediately or
later, backs off and retries until its deadline, and a failure while handling one connection never closes
the listener that accepted it. A failure that cannot be retried away is recorded and fails the Spark task
with that cause, both before and after the native initialization call, rather than leaving the task waiting
on a transport that will not recover.

Link-local addresses (`fe80::/10`) are supported only when a peer advertises a zone identifier
(`fe80::1%eth0`) that names an interface on every other machine, since a link-local address is scoped to a
single interface. A task normalizes a numeric scope (an interface index, which only means something on the
machine that produced it) to the interface name before publishing its endpoint, and a peer that still
advertises a numeric scope is rejected. A link-local peer without a zone, or with a zone this machine does
not have, fails immediately with an error naming the address instead of hanging. Use a globally routable or
unique-local IPv6 address for distributed training.
