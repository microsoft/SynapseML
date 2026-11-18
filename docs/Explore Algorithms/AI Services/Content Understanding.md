---
title: Content Understanding
hide_title: true
sidebar_label: Content Understanding
description: Analyze documents from Spark using AI defaults and save resumable Content Understanding results.
---

# Azure Content Understanding

`ContentUnderstanding` analyzes one document per DataFrame row using a prebuilt
or custom analyzer. Configure connection details once with `AIDefaults`, then
use `transform` in a Spark pipeline or write resumable results to a lakehouse.
The implementation calls REST from the JVM; no Content Understanding Python
SDK is required.

Install matching SynapseML Python and JVM artifacts containing this class.
See the [installation guide](../../Get%20Started/Install%20SynapseML.md).
The [Foundry invoice notebook](./Quickstart%20-%20Content%20Understanding.ipynb)
and [keyless Fabric notebook](./Quickstart%20-%20Content%20Understanding%20in%20Fabric.ipynb)
provide complete examples using synthetic PDFs.

## Configure once, analyze from code

For your own Foundry resource, configure its endpoint and key through your
secret manager. For example, in a Fabric notebook:

```python
import notebookutils
from synapse.ml.services.ai import AIDefaults
from synapse.ml.services.contentunderstanding import ContentUnderstanding

defaults = AIDefaults().for_api("content_understanding")
defaults.set_endpoint("https://<your-resource>.services.ai.azure.com")
defaults.set_api_key(
    notebookutils.credentials.getSecret(
        "https://<your-vault>.vault.azure.net/", "<your-secret-name>"
    )
)
defaults.set_analyzer_id("prebuilt-read")

analyzer = ContentUnderstanding(
    documentUrlCol="documentUrl",
    outputCol="analysis",
    errorCol="requestError",
)

documents = spark.createDataFrame(
    [("invoice-v1", "https://<storage>/<container>/invoice.pdf")],
    ["documentId", "documentUrl"],
)

latest = analyzer.writeToPath(
    documents, idCol="documentId", path="Files/content-understanding/operations"
)
latest.select("documentId", "status", "rawResponse", "error").show(truncate=False)
```

The default storage format is Delta. Use `format="parquet"` where Delta is
unavailable. The URL must be accessible to the service, not just the notebook.
For private lakehouse files, supply a binary column instead:

```python
files = spark.read.format("binaryFile").load("Files/documents/")
binaryAnalyzer = ContentUnderstanding(
    documentBytesCol="content", documentNameCol="path", outputCol="analysis"
)
results = binaryAnalyzer.transform(files)
```

Configure exactly one of `documentUrl` and `documentBytes`. Binary data is
base64-encoded in the request, so prefer accessible URLs for large inputs.

### Defaults and overrides

The stage reads only the `content_understanding` scope. It does not inherit
`OpenAIDefaults` or other `AIDefaults` API scopes.

| Python scope setter | Content Understanding setting |
| --- | --- |
| `set_endpoint(value)` | Foundry resource root, not an OpenAI deployment URL |
| `set_api_key(value)` | Resource subscription key; requires an endpoint in this scope |
| `set_api_version(value)` | Service API version |
| `set_analyzer_id(value)` | Prebuilt or custom analyzer ID |
| `set_model_deployments(mapping)` | Analyzer model names/aliases mapped to deployment names |
| `set_string_encoding(value)` | `codePoint`, `utf16`, or `utf8` |
| `set_processing_location(value)` | `geography`, `dataZone`, or `global` |
| `set_allow_input_truncation(value)` | Preview Boolean; requires a supported API version |

Defaults are shared by Spark sessions in the same driver JVM. Configure them
before starting work. `transform`, `transformSchema`, durable writes, and
analyzer management each resolve a snapshot onto a copy; they do not mutate
the configured stage. An already-created transform DataFrame retains its
snapshot even if defaults change before its Spark action.

Explicit stage values and column bindings win over defaults. For example:

```python
invoiceAnalyzer = analyzer.copy({}).setAnalyzerId("prebuilt-invoice")
invoiceAnalyzer.setModelDeployments(
    {
        "prebuilt-analyzer-completion": "<completion-deployment>",
        "prebuilt-analyzer-embedding": "<embedding-deployment>",
    }
)
```

Use `defaults.set_model_deployments(mapping)` to share that mapping across
stages. Request-level mappings do not change the Foundry resource's model
defaults. The resource must already have supported deployments.

Use `setEndpoint` and `setSubscriptionKey` for a self-contained stage.
`setAADToken` and `setAADTokenCol` support Microsoft Entra ID tokens for
`https://cognitiveservices.azure.com/.default`. Explicit credentials take
precedence over the scoped key. A scoped key is never forwarded to an explicit
endpoint on a different origin.

### Fabric-managed analysis

Where Fabric-managed Content Understanding is enabled, omit the endpoint and
credentials from both the stage and its defaults scope:

```python
analyzer = ContentUnderstanding(
    analyzerId="prebuilt-read",
    documentBytesCol="content",
    outputCol="analysis",
)
```

SynapseML uses the runtime's workspace endpoint and MWC identity for submission
and polling. It refreshes implicit authentication once after a handle-free
HTTP 401; explicit credentials are never replaced. A BYO endpoint takes
precedence and never inherits Fabric credentials.

The managed route supports analysis and result polling, not analyzer
administration. Availability of analyzers, models, and the managed service
depends on the Fabric capacity and region. Use your own Foundry resource for
custom analyzer management or capabilities unavailable on that route.

### Save and load stage configuration

SparkML save/load preserves explicit parameters separately from defaults:

```python
analyzer.write().overwrite().save("Files/models/document-analyzer")
loaded = ContentUnderstanding.load("Files/models/document-analyzer")
```

An unset endpoint, key, or analyzer remains unset after loading and resolves
the current `AIDefaults` at execution. Ambient defaults are not copied into
the saved stage. To deliberately freeze defaults, use
`snapshot = defaults.apply_to(analyzer)` before saving or running it.
**Explicit credentials, including those captured by `apply_to`, are saved
with the stage.** Keep credentials in the defaults scope or inject them after
loading when the saved stage must not contain secrets.

## Choose an execution API

| Python method | Behavior |
| --- | --- |
| `transform(dataset)` | Lazy; returns input columns plus `outputCol` and `errorCol`. |
| `writeToTable(dataset, idCol, tableName, format="delta", batchSize=1)` | Eager; journals accepted operations and results, returning the latest state per ID. |
| `writeToPath(dataset, idCol, path, format="delta", batchSize=1)` | Same journal behavior at a filesystem/lakehouse path. |
| `readTable(spark, tableName)` | Reads saved latest states without calling the service. |
| `readPath(spark, path, format="delta")` | Reads saved latest states from a path. |
| `createAnalyzer(definition, allowReplace=False)` | Explicit driver call; accepts a dictionary or JSON string, returning definition JSON. |
| `getAnalyzer()` | Explicit driver call returning the configured analyzer's definition JSON. |

`transform` adds a struct with `operationLocation`, `id`, `status`,
`httpStatus`, `rawResponse`, and `error`. It rejects output-column collisions.
`rawResponse` retains results, usage, warnings, and unknown service fields.
Only `status == "Succeeded"` means successful completion; an HTTP 200 or an
empty `result` does not.

Repeated Spark actions can repeat billable submissions. Materialize a
transform's output once and query the saved data. An ordinary DataFrame write
does not checkpoint individual operations; use the durable methods for that.

### Resume durable results

Use one dedicated journal destination per workload and one writer at a time:

```python
latest = analyzer.writeToTable(
    documents, idCol="documentId", tableName="document_analysis_operations"
)
saved = analyzer.readTable(spark, "document_analysis_operations")
completed = saved.where("status = 'Succeeded'")
```

Rerun the same write with the same input, IDs, options, and destination to
resume. The writer commits accepted operation handles before polling,
commits results individually, skips terminal records, and polls unfinished
handles without submitting those documents again.

This two-phase operation journal is separate from `AIPrompt`'s durable-output
store: saving only a final inference result would lose CU's accepted handle
when polling fails. Use `writeToPath`/`writeToTable`, not AIPrompt output-path
parameters, for Content Understanding recovery.

IDs must be unique, non-null strings identifying immutable document versions
and selected ranges. Changed bytes, URLs, analyzers, or request options for an
existing ID are rejected. Credential rotation does not change the fingerprint.
Keep the same scoped defaults when resuming, or explicitly snapshot them
before the first write. Content changes behind an unchanged URL and changes
to remote model/analyzer definitions cannot be detected; version those inputs.

To return after accepting and saving handles, then resume later:

```python
analyzer.setOperationMode("submit").writeToTable(
    documents, "documentId", "document_analysis_operations"
)
latest = analyzer.setOperationMode("analyze").writeToTable(
    documents, "documentId", "document_analysis_operations"
)
```

`readTable`/`readPath` and writer return values expose latest states as
top-level response fields plus `documentId`, `requestHash`, and `sequence`.
Reading the physical journal directly returns its append-only history.
Input document bytes are not copied into it.

| State or failure | Recovery |
| --- | --- |
| `Running` / `NotStarted` | Rerun the writer to poll the saved handle. |
| `Succeeded`, `Failed`, `Canceled` | Terminal record; inspect the result/error. Use a new versioned ID only for an intentional new analysis. |
| `Rejected` without a handle | Definite rejection; the writer stops before recording the ID. Correct credentials/options and rerun. |
| `Unknown` without a handle | Acceptance is uncertain; the journal blocks automatic resubmission. Investigate before choosing a new ID. |
| `ResultUnavailable` | Poll returned 404/410. The handle is retained; later IDs can continue. |
| Polling authentication/transport failure | The call fails, preserving the last committed handle for resumption. |

There is a crash window between service acceptance and committing the handle.
Neither Spark retries nor the journal guarantees exactly-once billing.
Resume promptly: service results have limited retention, while committed
`rawResponse` remains in your storage.

The writer is sequential on the driver. `batchSize` bounds collected input
rows, not commit size or concurrency; each document and response must fit in
memory. It may rescan and sort the manifest between batches. Supply a static,
deterministic DataFrame; use controlled batches or `foreachBatch` with stable
IDs for large workloads, not a streaming DataFrame directly.

## Request and polling options

Service parameters support scalar and column setters, for example
`setRange("1-5")` or `setRangeCol("pageRange")`. Python `setParams` rejects
simultaneous scalar/column bindings for the same parameter. `None` is ignored;
use `clear` to remove a value.

| Parameter | Default / purpose |
| --- | --- |
| `analyzerId` | `prebuilt-read`, unless scoped or explicit |
| `apiVersion` | `2025-11-01`, unless scoped or explicit |
| `documentUrl`, `documentBytes` | Exactly one input source |
| `documentName`, `mimeType` | Optional name and content type |
| `range` | Original 1-based PDF pages, or milliseconds for audio/video |
| `modelDeployments` | Per-request model/alias-to-deployment map |
| `operationMode` | `analyze`; also `submit` or `poll` |
| `operationLocation` | Accepted URL for poll-only mode; its API version is retained |
| `maxPollAttempts` | 120 GET attempts, excluding automatically retried 429s |
| `pollingIntervalSeconds` | 6 seconds; fractional values supported |
| `pollingDelay` | Same setting in milliseconds, retained for compatibility |
| `retryOnThrottling` | `True`; honors `Retry-After`, including waits longer than a minute |
| `maxResponseBytes` | 33554432; bounds each response before JSON parsing |
| `timeout` | 60 seconds for HTTP connection/read timeouts |
| `concurrency` | 1 operation per transform partition; does not parallelize durable writers |
| `concurrentTimeout` | Optional per-operation wait in seconds; expiration fails the Spark task |

Each transform concurrency slot covers submission and polling. Four partitions
at concurrency two can run eight operations. Spark cancellation and
`concurrentTimeout` cancel workers and abort active requests. Driver writers
and management calls observe JVM interruption; stopping only Python's wait
does not necessarily interrupt the JVM call.

Only definite throttling rejections without an accepted handle are retried as
submissions. Polling retries remain GETs. With throttling retries enabled, an
operation can wait indefinitely; `maxPollAttempts` is not a total deadline.
Set `retryOnThrottling=False` to return submission 429s as `Rejected` and
count polling 429s against the attempt budget.

Preview options require explicit opt-in:

```python
preview = analyzer.copy({}).setApiVersion("2026-06-01-preview")
preview.setAnalyzerId("prebuilt-layout").setAllowInputTruncation(False)
```

`allowInputTruncation` accepts `True` or `False` only on supported API versions;
a null Boolean column omits it. Offsets can use `setStringEncoding("utf16")`.

For range-level durability, create one row and versioned ID per range and use
`setRangeCol`. Ranges retain original numbering. Splitting changes extraction
context; SynapseML does not split automatically or guarantee page-by-page
streaming results. DOCX may return Markdown without pages and may ignore page
ranges; use whole-document IDs for DOCX. Follow the
[service limits](https://learn.microsoft.com/azure/ai-services/content-understanding/service-limits).

## Create a custom analyzer

Extraction fields, layout, and segmentation belong in an analyzer definition,
not an analyze-request `options` object:

```python
custom = analyzer.copy({}).setAnalyzerId("purchase-order-v1")
definition = {
    "baseAnalyzerId": "prebuilt-document",
    "config": {"returnDetails": True},
    "fieldSchema": {
        "name": "PurchaseOrder",
        "fields": {
            "Supplier": {
                "type": "string",
                "description": "The supplier's legal business name.",
            }
        },
    },
}
created = custom.createAnalyzer(definition, allowReplace=False)
current = custom.getAnalyzer()
```

Creation is driver-only and waits within the polling budget. It never runs
implicitly during analysis. `allowReplace=False` protects existing analyzers.
The resource administrator must configure required model defaults first;
SynapseML does not alter them to recover from `DefaultsNotSet`.

## Scala

Scala uses the same stage, journal, and defaults:

```scala
import com.microsoft.azure.synapse.ml.services.ai.AIDefaults
import com.microsoft.azure.synapse.ml.services.contentunderstanding.ContentUnderstanding

AIDefaults.forApi("content_understanding")
  .setEndpoint(endpoint)
  .setApiKey(key)
  .setAnalyzerId("prebuilt-read")

val analyzer = new ContentUnderstanding().setDocumentUrlCol("documentUrl")
val latest = analyzer.writeToTable(
  documents, idCol = "documentId", tableName = "document_analysis_operations")
val saved = analyzer.readTable(spark, "document_analysis_operations")
```

Scala writer/read signatures match the Python table above, using
`Dataset[_]`, `SparkSession`, and `DataFrame`. Scala analyzer creation takes
`createAnalyzer(definitionJson: String, allowReplace: Boolean): String`.
Use `AIDefaults.forApi("content_understanding").applyTo(analyzer)` for an
explicit snapshot.
