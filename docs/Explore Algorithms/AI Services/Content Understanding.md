---
title: Content Understanding
hide_title: true
sidebar_label: Content Understanding
description: Analyze content with Azure Content Understanding and save resumable results to a Fabric lakehouse or Spark table.
---

# Azure Content Understanding

`ContentUnderstanding` calls Azure Content Understanding from a Spark DataFrame.
It supports prebuilt and custom analyzers, document URLs or bytes, page and time
ranges, and per-request model deployment mappings. The default API version is
the latest generally available version, `2025-11-01`. Preview versions require
an explicit opt-in.

Use `transform` for ordinary Spark pipelines. Use `writeToTable` or `writeToPath`
when completed documents must survive a later failure. The durable methods save
each accepted operation handle before polling, then commit each result separately.
They do not wait for the entire input DataFrame to finish before writing results.

## API at a glance

Import `ContentUnderstanding` from `synapse.ml.services.contentunderstanding`.
These are instance methods on a configured analyzer:

| Python method | Behavior and return value |
| --- | --- |
| `transform(dataset)` | Lazy Spark transformation. Returns the input columns plus `outputCol` and `errorCol`. |
| `writeToTable(dataset, idCol, tableName, format="delta", batchSize=1)` | Eagerly processes input and persists each operation. Returns a DataFrame with the latest state of every ID in the journal. |
| `writeToPath(dataset, idCol, path, format="delta", batchSize=1)` | Same persistence and return schema, using a lakehouse or filesystem path. |
| `readTable(spark, tableName)` | Reads the latest persisted state per ID without contacting the service. |
| `readPath(spark, path, format="delta")` | Reads the latest persisted state from a path without contacting the service. |
| `createAnalyzer(definition, allowReplace=False)` | Explicit driver call. Accepts a dictionary or JSON string and returns the analyzer definition as a JSON string. |
| `getAnalyzer()` | Explicit driver call that returns the configured analyzer's definition as a JSON string. |

`transform` adds a response struct containing `operationLocation`, `id`,
`status`, `httpStatus`, `rawResponse`, and `error`. The durable methods return
those fields as top-level columns, with `documentId`, `requestHash`, and
`sequence`. They do not copy the input document bytes into the journal.

### REST calls under the hood

The Scala implementation uses the same SynapseML HTTP and Spark pipeline
infrastructure as other cognitive-service stages. It sends REST requests
directly, without a Content Understanding Python SDK dependency:

| Method | REST operation |
| --- | --- |
| Analyze or submit | `POST {endpoint}/contentunderstanding/analyzers/{analyzerId}:analyze?api-version={version}` |
| Poll an accepted operation | `GET {Operation-Location}` |
| `createAnalyzer` | `PUT {endpoint}/contentunderstanding/analyzers/{analyzerId}?api-version={version}&allowReplace={bool}`, then poll its management operation |
| `getAnalyzer` | `GET {endpoint}/contentunderstanding/analyzers/{analyzerId}?api-version={version}` |

The analyze body contains one `inputs` entry with either `url` or base64 `data`,
optional `name`, `mimeType`, and `range`, and an optional top-level
`modelDeployments` map. `stringEncoding` and `processingLocation` are query
parameters. Analyzer configuration is sent only by `createAnalyzer`.

## Configure an analyzer

Install the SynapseML Python package and matching JVM artifacts that include this
class. Fabric's preinstalled SynapseML version may not include a newly released
class. Follow the [installation guide](../../Get%20Started/Install%20SynapseML.md)
for your runtime rather than mixing Python and JVM versions.

In a Fabric notebook, retrieve the key from Key Vault at runtime:

```python
import notebookutils
from synapse.ml.services.contentunderstanding import ContentUnderstanding

key = notebookutils.credentials.getSecret(
    "https://<your-vault>.vault.azure.net/", "<your-secret-name>"
)

analyzer = (
    ContentUnderstanding()
    .setEndpoint("https://<your-resource>.cognitiveservices.azure.com")
    .setSubscriptionKey(key)
    .setAnalyzerId("prebuilt-read")
    .setDocumentUrlCol("documentUrl")
    .setOutputCol("analysis")
    .setErrorCol("requestError")
)
```

Outside Fabric, obtain the key from your secret manager. Microsoft Entra ID
authentication is also supported through `setAADToken` or `setAADTokenCol`.
Acquire a token for `https://cognitiveservices.azure.com/.default` using an
identity authorized on your Content Understanding resource. Refresh it for
long-running work. No implicit Fabric service endpoint or identity is used.

Do not put credentials in notebook arguments, Spark configuration, source code,
or displayed output. SparkML persistence includes configured scalar parameters:
save an unauthenticated stage and inject credentials after loading it.

```python
documents = spark.createDataFrame(
    [("invoice-v1", "https://<storage>/<container>/invoice.pdf")],
    ["documentId", "documentUrl"],
)

results = analyzer.transform(documents)
results.selectExpr(
    "documentId",
    "analysis.status",
    "get_json_object(analysis.rawResponse, '$.result.contents[0].markdown') AS markdown",
    "requestError",
).show(truncate=False)
```

A URL must be accessible to the Content Understanding service, not just the
notebook. For private lakehouse files, use a Spark binary column instead:

```python
files = spark.read.format("binaryFile").load("Files/documents/")
binaryAnalyzer = (
    ContentUnderstanding()
    .setEndpoint("https://<your-resource>.cognitiveservices.azure.com")
    .setSubscriptionKey(key)
    .setDocumentBytesCol("content")
    .setDocumentNameCol("path")
)
```

Configure exactly one source, `documentUrl` or `documentBytes`. Do not set both
on the same stage. Binary input is base64-encoded in the request's `inputs[].data`
property. Prefer URLs for large files when access requirements permit it, since
base64 encoding increases memory and request size.

PDF and DOCX files can use the same bytes API. For DOCX, supply a `.docx` name
and, when setting `mimeType`, use
`application/vnd.openxmlformats-officedocument.wordprocessingml.document`.
Use `setMimeTypeCol` for mixed-format input.

Do not assume every document response contains a `pages` array. In live
`prebuilt-read` tests, PDFs returned page-level output and DOCX returned text and
tables as Markdown with `documentPagesMinimal` usage. A DOCX request with
`range="2"` still returned the whole document. Use whole-document IDs for DOCX;
do not use its page range as a checkpoint boundary. The PDF range examples below
are not a DOCX pagination guarantee.

### Request options

Options backed by service parameters accept either a scalar setter or a column
setter, such as `setRange("1-5")` and `setRangeCol("pageRange")`.

| Option | Purpose |
| --- | --- |
| `analyzerId` | Prebuilt analyzer or an existing custom analyzer. Defaults to `prebuilt-read`. |
| `documentUrl`, `documentBytes` | Mutually exclusive input source. |
| `documentName`, `mimeType` | Optional name and content type. |
| `range` | Original 1-based pages for documents, or integer milliseconds for audio/video. |
| `modelDeployments` | Map the analyzer's model names or aliases to deployments in your resource. |
| `stringEncoding` | `codePoint`, `utf16`, or `utf8` for response offsets. |
| `processingLocation` | Service-supported processing-location policy. |
| `apiVersion` | Defaults to `2025-11-01`; use `setApiVersion` to override. |
| `operationMode` | `analyze`, `submit`, or `poll`. Defaults to `analyze`. |
| `operationLocation` | Accepted operation URL used by poll-only mode. |

Polling and memory controls use ordinary scalar setters:

| Setter | Default | Purpose |
| --- | --- | --- |
| `setMaxPollAttempts` | 120 | Bound GET attempts, including retries. |
| `setPollingDelay` | 1000 | Milliseconds between polls when `Retry-After` is absent. |
| `setMaxResponseBytes` | 33554432 | Bound each response before parsing JSON. |
| `setTimeout` | 60 | HTTP connection and read timeout in seconds. |
| `setConcurrency` | 1 | Concurrent operations per Spark partition for `transform`. |

The client honors `Retry-After`, capped at one minute per delay. A response-size
failure includes `ResponseTooLarge`; increase the bound or choose smaller ranges
rather than treating an unread response as an empty result.

For example, a prebuilt invoice analyzer can use request-level model aliases:

```python
invoiceAnalyzer = analyzer.copy({}).setAnalyzerId("prebuilt-invoice")
invoiceAnalyzer.setModelDeployments(
    {
        "prebuilt-analyzer-completion": "<your-completion-deployment>",
        "prebuilt-analyzer-embedding": "<your-embedding-deployment>",
    }
)
```

The selected model must be supported by the analyzer. A deployment name alone
does not identify the alias the analyzer expects. See
[models and deployments](https://learn.microsoft.com/azure/ai-services/content-understanding/concepts/models-deployments).
Request-level mappings do not modify the resource's shared defaults.

To opt into the `2026-06-01-preview` API and its layout behavior:

```python
previewAnalyzer = (
    analyzer.copy({})
    .setApiVersion("2026-06-01-preview")
    .setAnalyzerId("prebuilt-layout")
    .setStringEncoding("utf16")
)
```

### Custom extraction and analyzer configuration

Layout details, segmentation, and extraction fields belong in an analyzer
definition, not an `options` object in an analyze request. `createAnalyzer`
accepts a Python dictionary or JSON string. It is an explicit driver operation
and never runs automatically during `transform`.

```python
custom = analyzer.copy({}).setAnalyzerId("purchase-order-v1")
definition = {
    "baseAnalyzerId": "prebuilt-document",
    "description": "Extract the supplier from a purchase order.",
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

The service creates analyzers asynchronously. `createAnalyzer` waits within the
configured polling budget and returns the final analyzer definition. A service
failure or exhausted creation budget raises an exception with the operation
response. Use `getAnalyzer` to inspect the analyzer afterward.
`allowReplace=False` protects an existing analyzer. Replacing an analyzer is an
explicit administrative choice.

Your resource must have its required default model deployments configured
before creating a custom analyzer. If the service reports `DefaultsNotSet`,
have the resource administrator configure those defaults. SynapseML does not
silently change them. The complete definition is forwarded to the
[create-analyzer API](https://learn.microsoft.com/rest/api/contentunderstanding/content-analyzers/create-or-replace?view=rest-contentunderstanding-2025-11-01),
so new service configuration fields do not require a new Python wrapper.

## Save completed work to a lakehouse or table

Use a stable, unique string ID for each immutable document version and selected
range. `writeToTable` and `writeToPath` are eager actions, unlike `transform`.
The examples below use Delta, which is the default and the recommended format
for lakehouse use.

```python
latest = analyzer.writeToTable(
    documents,
    idCol="documentId",
    tableName="content_understanding_operations",
)

# An alternative destination using a lakehouse path:
latest = analyzer.writeToPath(
    documents,
    idCol="documentId",
    path="Files/content-understanding/operations",
    format="delta",
)
```

Choose one destination for a given workload. These two calls are alternatives:
calling both starts separate analyses in separate journals.

If an action fails, read the already-committed work without contacting the
service:

```python
partial = analyzer.readTable(spark, "content_understanding_operations")
completed = partial.where("status = 'Succeeded'")
failures = partial.where("status IN ('Failed', 'Canceled', 'ResultUnavailable')")
```

Rerun `writeToTable` with the same input, IDs, options, and destination to resume.
It skips terminal records and polls saved handles for unfinished operations.
It does not POST those documents again. Changing the bytes, URL, analyzer, or
analysis options for an existing ID raises an error instead of silently mixing
different analyses. Rotating the resource's authentication credential does not
change the request fingerprint.

Content changes behind an unchanged URL cannot be detected. Use immutable URLs
or versioned IDs. A new SAS URL changes the request fingerprint, so retain the
original manifest when resuming an operation that was already submitted.
Likewise, version custom analyzers and model deployments: the fingerprint does
not fetch remote analyzer definitions or model revisions.

To persist accepted handles without waiting for analysis to complete, use
submit-only mode. Resume against the same destination in analyze mode:

```python
analyzer.setOperationMode("submit").writeToTable(
    documents, "documentId", "content_understanding_operations"
)
latest = analyzer.setOperationMode("analyze").writeToTable(
    documents, "documentId", "content_understanding_operations"
)
```

Repeated submit-only writes skip IDs that already have saved handles.
Poll-only `transform` can also consume operation handles through
`setOperationLocationCol`. It uses the API version in each saved URL, so the
original document and API-version columns are not required. The durable writers
require the original document manifest and use `analyze` or `submit` mode.

For a path destination, use
`analyzer.readPath(spark, "Files/content-understanding/operations")`.
The read helpers and write return values contain the latest state per ID.
Reading the physical table or path directly returns the append-only operation
history, usually multiple rows per document:

| Column | Meaning |
| --- | --- |
| `documentId` | Your stable document/range ID. |
| `requestHash` | SHA-256 fingerprint of the analysis request, excluding resource authentication. |
| `sequence` | Increasing journal sequence within that ID. |
| `operationLocation`, `id` | Service operation handle and service-generated ID. |
| `status`, `httpStatus` | Service operation status or SDK recovery status, and last HTTP status. |
| `rawResponse` | Complete operation JSON, including results, usage, warnings, and unknown fields. |
| `error` | Service error or client diagnostic JSON when present. |

Only `Succeeded` means successful completion. A `Running` response can already
contain an empty `result`, and an HTTP 200 response can report `Failed`. Inspect
the status and error columns before downstream processing. Terminal failures
are retained rather than retried indefinitely. Correct the cause and use a new
versioned ID for an intentional retry.

Submission HTTP 401, 403, or 429 without an operation handle is a definite
rejection. The writer stops with `Rejected` before recording that ID. Fix the
credential or throttling condition and rerun against the same journal. Completed
IDs remain untouched, and the rejected ID can be submitted again.

A submission transport failure, HTTP 408, HTTP 5xx, or unreadable response can
leave acceptance or completion uncertain. An accepted response with a missing
or invalid handle is also `Unknown`. The writer records `Unknown` and stops if
no valid handle was received. It will not automatically submit that ID again.
Resolve the service outcome before choosing a new ID for an intentional retry.
To process unrelated documents while investigating, exclude the unresolved IDs
from the input manifest and keep the same journal.

Polling HTTP 404 or 410 produces a terminal `ResultUnavailable` record with the
original handle and error. The result may be missing or past the service's
retention period. The writer continues with later IDs without resubmitting the
unavailable operation. Only the affected document or range needs a new versioned
ID if you intentionally analyze it again.

### Large documents and explicit ranges

For range-by-range durability, create one input row and ID per range:

```python
ranges = spark.createDataFrame(
    [
        ("report-v1/pages/1-2", "https://<storage>/report.pdf", "1-2"),
        ("report-v1/pages/3-4", "https://<storage>/report.pdf", "3-4"),
    ],
    ["documentId", "documentUrl", "pageRange"],
)

rangeAnalyzer = analyzer.copy({}).setRangeCol("pageRange")
latest = rangeAnalyzer.writeToTable(
    ranges, idCol="documentId", tableName="report_analysis_operations"
)
```

Ranges refer to the original input. They do not restart numbering at page 1.
Splitting a document changes the context available for cross-page extraction
and table reasoning, so SynapseML never splits it automatically.

This saves complete documents or selected ranges. The service does not promise
usable page-by-page output while an operation is still running. Follow the
[service limits](https://learn.microsoft.com/azure/ai-services/content-understanding/service-limits);
selecting a range does not remove input-byte or response-size limits.

### Durability and resource limits

Only one writer may own a destination at a time. Use a dedicated output table
or path, not an existing business table. Delta and Parquet are supported; Delta
provides the stronger transactional storage behavior.

The writer processes requests sequentially on the driver. `batchSize` controls
how many input rows it collects at a time, not how many results share a commit.
It defaults to 1 to limit retained document bytes. It projects only the ID and
configured parameter columns, but a single binary document or response must
still fit in memory. `setConcurrency` applies to `transform`, not to the durable
writer.

The input must be a static, deterministic manifest that does not change during
the call. Bounded keyset batches can rescan and sort that manifest. For very
large manifests, call the writer from controlled input batches or a structured
streaming `foreachBatch` callback using the same journal and globally stable
IDs. Do not pass a streaming DataFrame directly.

Polling has a finite attempt budget. Exhausting it leaves a pending record that
can be resumed with a fresh credential or a larger budget. Transport errors and
malformed responses fail the action instead of being reported as success.
Already-committed handles and results remain available.
Other polling HTTP errors fail the action without marking the accepted service
operation as failed. For an expired credential, set a fresh key or token and
rerun against the same journal. Read helpers expose the last committed state;
the exception contains the unsuccessful polling response.

There is an unavoidable crash window between the service accepting a POST and
the journal committing its handle. A crash in that window can cause another
submission on retry. Neither the writer nor Spark task retries provide
exactly-once external service calls. POST requests are never automatically
retried within an invocation.

Service results are retained for
[up to 24 hours](https://learn.microsoft.com/azure/foundry/responsible-ai/content-understanding/data-privacy).
Resume pending operations promptly. A saved handle is not permanent result
storage; the journal's committed `rawResponse` is.

## Scala

Scala uses the same implementation and persistence behavior:

```scala
def transform(dataset: Dataset[_]): DataFrame
def writeToTable(dataset: Dataset[_], idCol: String, tableName: String,
                 format: String = "delta", batchSize: Int = 1): DataFrame
def writeToPath(dataset: Dataset[_], idCol: String, path: String,
                format: String = "delta", batchSize: Int = 1): DataFrame
def readTable(spark: SparkSession, tableName: String): DataFrame
def readPath(spark: SparkSession, path: String, format: String = "delta"): DataFrame
def createAnalyzer(definitionJson: String, allowReplace: Boolean): String
def getAnalyzer(): String
```

```scala
import com.microsoft.azure.synapse.ml.services.contentunderstanding.ContentUnderstanding

val analyzer = new ContentUnderstanding()
  .setEndpoint(endpoint)
  .setSubscriptionKey(key)
  .setDocumentUrlCol("documentUrl")
  .setAnalyzerId("prebuilt-read")

val latest = analyzer.writeToTable(
  documents, idCol = "documentId", tableName = "content_understanding_operations")
val resumed = analyzer.readTable(spark, "content_understanding_operations")
```

No Content Understanding Python SDK is required. The public transformer,
request handling, and journal logic live in the JVM implementation.

## Content Understanding tests

Offline Scala suites are under
`cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/form/contentunderstanding`.
They remain in the existing document-service CI group, with separate files for
the public API and protocol, journal writes, recovery, session filesystem
configuration, and framework fuzzing. Run only this feature with:

```bash
sbt "cognitive/testOnly *ContentUnderstanding*Suite"
```

`test_ContentUnderstanding.py` exercises the generated Python wrapper against a
loopback REST fixture. `test_ContentUnderstandingE2E.py` is a separate, opt-in
Azure suite that generates synthetic PDF and DOCX files. It covers PDF ranges,
DOCX text and tables, optional preview metadata, partial table results, and
submit-only path resumption. Its module docstring lists the environment
variables and the scratch resources it creates and removes. Without explicitly
configured live-service credentials, the live suite is skipped.
