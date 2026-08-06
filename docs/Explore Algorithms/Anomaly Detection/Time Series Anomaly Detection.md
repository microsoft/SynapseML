---
title: Time Series Anomaly Detection
hide_title: true
sidebar_label: Time Series Anomaly Detection
---

## Time Series Anomaly Detection on Apache Spark

`UnivariateAnomalyDetector` finds anomalies in univariate time series and scales
across many series at once. Each series is scored independently and in parallel,
so a table holding thousands of separate signals is detected in a single pass.

It replaces the `DetectAnomalies`, `DetectLastAnomaly` and `SimpleDetectAnomalies`
transformers, which were removed when the Azure AI Anomaly Detector service was
retired. Parameter names and output field names are unchanged, so migrating is
normally just a change of import and constructor.

### Installation

The detector is built on the open source
[`microsoft/anomaly-detector`](https://github.com/microsoft/anomaly-detector)
library, which open sources the same algorithms that used to back the retired
service. It's an optional dependency, so install it separately:

```bash
pip install time-series-anomaly-detector
```

The install name and the import name differ: the package installs as
`time-series-anomaly-detector` but imports as `anomaly_detector`.

### Usage

```python
from synapse.ml.timeseries import UnivariateAnomalyDetector

detector = (
    UnivariateAnomalyDetector()
    .setGroupByCols(["deviceId"])
    .setTimestampCol("timestamp")
    .setValueCol("value")
    .setGranularity("hourly")
    .setSensitivity(95)
    .setOutputCol("anomalies")
)

scored = detector.transform(df)
scored.select("deviceId", "timestamp", "value", "anomalies.isAnomaly").show()
```

The output column is a struct with the fields `isAnomaly`, `isPositiveAnomaly`,
`isNegativeAnomaly`, `expectedValue`, `upperMargin`, `lowerMargin` and `period`.

Set `detectMode` to `latest` to score only the most recent point of each series,
which is the streaming style check the old `DetectLastAnomaly` performed. In that
mode every row except the last one in each series has a null result.

### Scaling

`setGroupByCols` is what makes this scale. The underlying library is single node
pandas code, so parallelism comes from detecting many series concurrently rather
than splitting one series across executors. Give each independent signal its own
group key and Spark spreads the work across the cluster.

Leaving `groupByCols` empty treats the whole DataFrame as one series. That's
correct, but it runs on a single executor and won't benefit from a larger
cluster.

Note that a series is materialized in memory on one executor while it's scored,
so extremely long individual series are the practical limit, not the number of
series.

### Error handling

Time series data is uneven in practice: some groups are too short to model, and
some carry a granularity the detector can't infer. Rather than failing the job,
those series are returned with a null result and a message in the error column.

```python
scored.filter("error is not null").select("deviceId", "error").show(truncate=False)
```

This means a single malformed group cannot take down a run over thousands of
series. Check the error column when a series unexpectedly has no result.

### Parameters

| Parameter | Description |
| --- | --- |
| `timestampCol` | Column holding the timestamp of each point. Defaults to `timestamp`. |
| `valueCol` | Column holding the measured value. Defaults to `value`. |
| `groupByCols` | Columns identifying an individual series. Empty means a single series. |
| `outputCol` | Output struct column. Defaults to `anomalies`. |
| `errorCol` | Output column holding per series error messages. Defaults to `error`. |
| `detectMode` | `entire` to score every point, or `latest` for only the most recent one. |
| `granularity` | `yearly`, `monthly`, `weekly`, `daily`, `hourly`, `minutely`, `secondly`, `microsecond` or `none`. |
| `customInterval` | Used with `granularity`, for example `5` with `minutely` for a five minute series. |
| `period` | Known period of the series. Omit to have it inferred. |
| `maxAnomalyRatio` | Maximum proportion of points that may be flagged, in (0, 0.49]. |
| `sensitivity` | 0 to 99. Lower values widen the margins and flag fewer points. |
| `imputeMode` | `auto`, `previous`, `linear`, `fixed`, `zero` or `notFill`. |
| `imputeFixedValue` | Value used to fill gaps when `imputeMode` is `fixed`. |

### Migrating from the retired transformers

Parameters and output fields carry over unchanged, so most pipelines only need
the import and constructor swapped:

```python
# Before, against the retired Azure AI Anomaly Detector service
from synapse.ml.services.anomaly import SimpleDetectAnomalies

detector = (
    SimpleDetectAnomalies()
    .setSubscriptionKey(key)
    .setLocation("westus2")
    .setGroupbyCol("deviceId")
    .setGranularity("hourly")
    .setOutputCol("anomalies")
)

# After, running locally with no service call
from synapse.ml.timeseries import UnivariateAnomalyDetector

detector = (
    UnivariateAnomalyDetector()
    .setGroupByCols(["deviceId"])
    .setGranularity("hourly")
    .setOutputCol("anomalies")
)
```

Two differences are worth knowing about:

- No subscription key, endpoint or location is needed, because detection runs on
  the cluster instead of calling a REST API. Throughput is bounded by the cluster
  rather than by service quota.
- The `severity` output field is gone. The retired service returned it, but the
  open source library doesn't compute it. Use `expectedValue` together with
  `upperMargin` and `lowerMargin` to gauge how far a point deviates.

### Multivariate series

The library also implements the multivariate model that backed the retired
`FitMultivariateAnomaly` and `DetectMultivariateAnomaly` transformers, but it
trains a PyTorch model over a sliding window rather than scoring rows
independently. That doesn't decompose into a per row Spark transformer, so it
isn't wrapped here. Train it on the driver and apply it with a pandas UDF, or
featurize your signals and use [Isolation Forests](../Quickstart%20-%20Isolation%20Forests).
