---
title: Content Understanding
hide_title: true
sidebar_label: Content Understanding
description: Analyze content at Spark scale using native Azure Content Understanding analyzers.
---

# Azure Content Understanding

`ContentUnderstanding` is a SynapseML transformer that calls Azure Content
Understanding (CU) for each input row. Use document URLs or binary columns,
prebuilt or custom analyzers, and native request options in an ordinary Spark
DataFrame or ML pipeline. The implementation is Scala-first with a generated
Python wrapper; no separate CU Python SDK is required.

## Connector scope

SynapseML owns distributed request execution, polling, row association, and
request/result mapping. CU owns analyzer behavior, field extraction, reasoning,
and model execution. Selecting a richer CU analyzer does not require a separate
SynapseML semantic-processing layer. The connector retains native results rather
than interpreting them or imposing a field schema.

This contribution includes `analyze`, `submit`, and `poll`, plus explicit
driver-side `createAnalyzer` and `getAnalyzer` calls. Analyzer management is never
an implicit part of `transform`. It does not include a durable operation journal,
automatic job restart, Fabric-managed authentication or billing, runtime
packaging, or the Fabric LLM/backend integration. Those are separate workstreams.
Existing native options and raw-result preservation support service evolution;
new endpoints or incompatible request contracts may still require connector
changes.

## Setup and first analysis

Install matching SynapseML Python and JVM artifacts that include this class.
A runtime's preinstalled SynapseML version may not include a newly released
connector. Follow the [installation guide](../../Get%20Started/Install%20SynapseML.md)
for your runtime. You also need an existing CU resource with access to the
selected analyzer and any required model deployments.

The common configuration needs only an endpoint, a key obtained from your
secret manager, and an input column:

```python
from synapse.ml.services.contentunderstanding import ContentUnderstanding

analyzer = (
    ContentUnderstanding()
    .setEndpoint(endpoint)
    .setSubscriptionKey(key)
    .setDocumentUrlCol("documentUrl")
    .setOutputCol("analysis")
    .setErrorCol("requestError")
)

results = analyzer.transform(documents)
results.write.mode("overwrite").parquet(output_path)
```

`outputCol` and `errorCol` are optional; defaults are unique to the stage.
The default analyzer is `prebuilt-read`, the default mode is `analyze`, and the
stable API default is `2025-11-01`. Setters do not contact CU or provision
anything. `transform` is lazy: Spark actions execute requests on executors.
Duplicate input rows are processed independently; no unique-ID column is required.

A URL must be accessible to CU, not merely to the notebook. For files Spark
can access, use its existing binary-file reader:

```python
files = spark.read.format("binaryFile").load(input_path)
binary_analyzer = (
    ContentUnderstanding()
    .setEndpoint(endpoint)
    .setSubscriptionKey(key)
    .setDocumentBytesCol("content")
    .setDocumentNameCol("path")
    .setOutputCol("analysis")
    .setErrorCol("requestError")
)
results = binary_analyzer.transform(files)
```

Configure exactly one source: `documentUrl` or `documentBytes`. Binary data is
base64-encoded into `inputs[].data`, not sent to a separate Analyze Binary API.
Base64 and JSON increase memory and request size; prefer service-readable URLs
for large inputs when access requirements permit. A selected range does not
reduce the bytes encoded from a binary column.

### Authentication and saved stages

Use your secret manager to obtain the key. On Fabric, for example:

```python
import notebookutils

key = notebookutils.credentials.getSecret(
    "https://<your-vault>.vault.azure.net/", "<your-secret-name>"
)
```

Explicit Entra authentication is available through `setAADToken` or
`setAADTokenCol`. Acquire a token for
`https://cognitiveservices.azure.com/.default` with an identity authorized on
your CU resource. The connector does not refresh tokens, provision identities,
or borrow Fabric or global OpenAI credentials.

Credential and header values must fit Latin-1 and cannot contain control
characters other than tab.
Custom header names must be valid HTTP tokens and cannot be `Host`,
`Content-Length`, `Content-Type`, or `Transfer-Encoding`. Invalid scalar values
fail before dispatch; invalid column values produce `InvalidInput` for that row.

**Standard SynapseML save/load behavior applies:** configured scalar keys,
tokens, and headers can be saved in model metadata. The serializer does not
encrypt them. Protect access to model artifacts and replace stale or
environment-specific credentials after loading. Never commit or display keys.
Saving a stage is different from saving its analysis results.

Python `explainParam` and `explainParams` redact scalar authentication parameters
and custom/telemetry-header maps while retaining column-binding names. Explicit getters,
parameter maps, Scala inspection, and saved metadata still contain configured
values; redaction is not encryption or a secret-free persistence guarantee.

Python generic parameter inspection exposes a scalar view: it includes JVM
scalar defaults and returns map-valued options as dictionaries. Column bindings
are excluded from `isSet`, `hasDefault`, `isDefined`, and `extractParamMap`,
including when a binding overrides a scalar default. Explicit `getOrDefault`
on a column binding raises `KeyError`. Use `explainParam` to inspect the binding;
copy/save/load preserve it on the JVM, and clearing it restores any scalar default.

Automatic parameter collection, including MLflow Spark ML autologging for
pipeline stages, can record scalar keys, tokens, custom headers, and signed
URLs. Description redaction does not protect that path. Disable autologging
around credential-bearing pipelines, or avoid storing credentials in scalar
parameters. Disabling model logging alone does not disable parameter logging.

To keep credentials out of an artifact, save an unauthenticated stage before
configuring its execution credentials:

```python
portable = (
    ContentUnderstanding()
    .setEndpoint(endpoint)
    .setDocumentUrlCol("documentUrl")
)
portable.write().save(model_path)

loaded = ContentUnderstanding.load(model_path)
loaded.setSubscriptionKey(key)
```

Scalar document URLs may themselves contain SAS tokens; saved input data,
URLs, and CU result content also require appropriate access controls.

## Native request options

Service parameters support scalar and column setters, for example
`setAnalyzerId("custom-invoice-v1")` or `setAnalyzerIdCol("analyzer")`.

| Parameter | Meaning |
| --- | --- |
| `analyzerId` | Existing prebuilt or custom analyzer; defaults to `prebuilt-read`. |
| `documentUrl`, `documentBytes` | Mutually exclusive document source. |
| `documentName`, `mimeType` | Optional name and MIME type. |
| `range` | Service-native page or time range, subject to the selected analyzer and format. |
| `modelDeployments` | Per-request model-name/alias to deployment-name mapping. |
| `stringEncoding` | `codePoint`, `utf16`, or `utf8` for response offsets. |
| `processingLocation` | `geography`, `dataZone`, or `global`, where supported by the service. |
| `apiVersion` | Stable default `2025-11-01`; explicit version overrides are permitted. |
| `operationLocation` | Validated analysis operation URL for poll mode. |

Custom headers cannot override connector-managed framing headers or
`Content-Type`; connector-generated request bodies use JSON.

```python
custom_requests = (
    analyzer.copy({})
    .setAnalyzerId("custom-invoice-v1")
    .setRangeCol("pageRange")
    .setModelDeployments({"prebuilt-analyzer-completion": "<deployment-name>"})
)
preview = analyzer.copy({}).setApiVersion("2026-06-01-preview")
```

Version overrides are forwarded, not checked against a version allowlist.
The connector does not discover "latest" at runtime. New service versions,
modalities, endpoints, or mandatory request fields can still require connector
changes. Raw JSON preservation is not a promise of complete REST API parity.

For PDF ranges, create one row per selected range. Page numbering refers to the
original input. Splitting changes the context available for cross-page
extraction; the connector never splits documents automatically. Do not assume
every format supports page ranges or returns a `pages` array. Use the
[service limits](https://learn.microsoft.com/azure/ai-services/content-understanding/service-limits)
and the selected analyzer's contract.

## Results and failures

The transformer preserves input columns and adds `outputCol` and `errorCol`.
Those names must not overwrite input columns. The response envelope contains:

| Field | Meaning |
| --- | --- |
| `operationLocation` | Accepted, validated operation handle, when available. |
| `id` | Service operation ID, when returned. |
| `status` | Service status or connector diagnostic state. |
| `httpStatus` | Last HTTP status; zero for local validation/transport failures without a response. |
| `rawResponse` | Complete returned operation JSON, including unknown fields. |
| `error` | Service error or connector diagnostic JSON. |

Only `Succeeded` indicates success. HTTP 200 can contain a failed operation;
`Running` can contain an empty result. Extract fields explicitly rather than
assuming every analyzer returns the same schema:

```python
completed = results.where("analysis.status = 'Succeeded'")
markdown = completed.selectExpr(
    "get_json_object(analysis.rawResponse, '$.result.contents[0].markdown') AS markdown"
)
```

Missing, ambiguous, or wrongly typed columns and invalid scalar configuration fail before
dispatch. Invalid row inputs, including null/empty documents, produce
`InvalidInput` in the response and error column without a service request.
Document-specific service failures remain row results.

As with other SynapseML service transformers, `set...Col` bindings must match
top-level input column names exactly, including case, and the selected name
must be unambiguous under the dataset's resolver. Exact duplicate names are
rejected; case-variant names such as `document` and `DOCUMENT` can be used
only in a case-sensitive dataset. Output/error collision
checks instead follow Spark's column resolver. Direct `transform(dataset)` uses
the dataset's session throughout internal planning and execution, without
changing the active session. CU's internal schema callbacks derive field types
without selecting input columns in a temporary DataFrame. Public schema-only
calls and external Spark ML pipeline planning use the
active session because a `StructType` carries no session. When using multiple
Spark sessions, make the dataset's session active before pipeline `fit`/`transform` or
schema-only validation.

Shared authentication rejection (HTTP 401/403 when the effective scalar
credential was sent or all authentication-capable parameters are scalar or unset),
pre-acceptance HTTP 404 for a scalar analyzer/API version,
and recognized shared analyzer/deployment failures produce a
`SharedConfigurationFailure` row diagnostic and stop new submissions in that
partition when the response is definitive. Ambiguous `Unknown` submissions retain
their original diagnostic, do not stop the partition, and are not safe to resubmit
automatically. The triggering definitive row retains its status and raw service response;
subsequent unsent rows return `Rejected` with HTTP status zero. Authentication
source precedence is honored even when a lower-priority row credential column
is configured but unused. Column-configured credentials, including missing
credentials in a `customHeaders` column, produce row-level failures when they
are the credentials selected for that request. Missing authentication and
nonstandard scalar authentication headers also stop later submissions when
`subscriptionKey`, `AADToken`, `CustomAuthHeader`, `customHeaders`, and
`telemHeaders` are all scalar or unset. Without a known scalar credential being
sent, any column binding among those parameters keeps the rejection row-level,
even when that row's credential is empty. The stop itself is
partition-wide: if a row falls back to a rejected scalar credential, later
unsent rows are skipped even if they would use their own column credentials.
This intentionally conservative policy does not maintain separate circuits
for individual credentials. Once an
operation handle has been accepted, later authentication or analyzer/deployment
failures are returned as row results with the handle retained. Already-started
sibling requests finish and their results are collected; a shared failure does
not itself fail the Spark task or discard those handles. Other partitions are
independent. In poll mode, analyzer/deployment
settings are not used by the GET request, so those failures remain row-level.
This is executor-side detection, not a driver preflight. It is not a
transactional or exactly-once guarantee: work already dispatched may continue,
unrelated Spark task failures can replay submissions, and charges are not rolled back.
Service error payloads remain in results or exception response objects; treat
them as potentially sensitive.

## Submit now and poll later

`setOperationMode` selects one of three modes:

| Mode | Behavior |
| --- | --- |
| `analyze` | Submit once, then poll within configured bounds. Default. |
| `submit` | Submit once and return the response/handle without polling. |
| `poll` | Poll an existing analysis handle without resubmitting the document. |

Callers own handle persistence:

```python
submitted = analyzer.copy({}).setOperationMode("submit").transform(documents)
submitted.write.parquet(handles_path)

saved = spark.read.parquet(handles_path)
pending = saved.where("""
    analysis.operationLocation IS NOT NULL AND
    analysis.status IN ('NotStarted', 'Running', 'Unknown')
""").selectExpr(
    "*", "analysis.operationLocation AS operationHandle"
)
poller = (
    ContentUnderstanding()
    .setEndpoint(endpoint)
    .setSubscriptionKey(key)
    .setOperationMode("poll")
    .setOperationLocationCol("operationHandle")
    .setOutputCol("polledAnalysis")
    .setErrorCol("pollError")
)
polled = poller.transform(pending)
polled.write.parquet(results_path)
```

Poll mode uses the API version in the saved handle; it does not require the
original document or API-version columns. Handles must match the configured
endpoint's scheme, host, port, and CU operation path. Redirects are not followed.
Analyzer-creation management handles cannot be passed to this mode; use
`getAnalyzer` or service tooling to inspect analyzer creation instead.
HTTP 404/410 while polling produces `ResultUnavailable`, retaining the handle.
Other non-transient GET errors retain the previous operation status (or use
`Unknown` when polling a handle without prior state), include the poll error,
and keep the handle for a later retry; they do not prove that the remote
operation failed.

POST/PUT requests are not automatically replayed by the connector. A transport
failure or ambiguous submission may return `Unknown`; inspect the outcome before
intentionally resubmitting. Only HTTP 200, 201, or 202 submissions are followed
automatically; an `Operation-Location` on another status is not followed. Safe
GET retries consume the polling budget.

`Rejected` means the request was not accepted and no operation handle was
returned: HTTP 429 throttling, HTTP 401/403 with scalar or per-row authentication
(including a missing credential in a `customHeaders` column), or a DNS,
connection, or deadline failure before HTTP request writing began. Failures after
writing starts remain ambiguous when no definitive response is available,
including partial writes. Complete HTTP 401/403/429 status and headers without
an operation handle establish admission rejection even if reading the error
body fails; an I/O failure reports `ResponseBodyReadFailed`, not a warning that
the rejected request may have been accepted. An oversized body retains
`ResponseTooLarge`. For scalar authentication
failures it is nested under `SharedConfigurationFailure`, and the partition
still stops. A contradictory operation handle keeps the outcome `Unknown`.
An unreadable or oversized HTTP 404 also remains `Unknown` and does not trigger
the shared analyzer stop.
Missing or invalid handles on accepted-status responses do not replace an
existing transport/response-size diagnostic. Unsafe handles are never followed.
Rows skipped after a shared
configuration failure also return `Rejected` because they were never sent.
The connector does not retry rejected submissions. Because the service did not
accept an operation, resubmitting a rejected row does not repeat accepted work.
For throttling, reduce `setConcurrency` or the number of active partitions; for
authentication, correct the credentials. DNS admission waits within the operation
budget and reports `DnsCapacityExceeded` if the executor's bounded worker pool
remains occupied; reduce concurrent load before retrying those rows.
For shared configuration failures, fix the configuration before retrying.
The triggering row remains `Rejected` for definitive scalar HTTP 401/403
admission failures, with a `SharedConfigurationFailure` diagnostic. Shared analyzer
or deployment failures can instead retain `Failed`; skipped rows are `Rejected`.
Include those definite shared failures without a handle in the retry selection;
do not include `Unknown` rows or operations with saved handles. Retry only from
saved results, because filtering the lazy `results` DataFrame would run every
analysis again:

```python
saved = spark.read.parquet(output_path)
retryable = saved.where("""
    analysis.operationLocation IS NULL AND (
        analysis.status = 'Rejected' OR (
            analysis.status = 'Failed' AND
            get_json_object(analysis.error, '$.code') = 'SharedConfigurationFailure'
        )
    )
""").drop("analysis", "requestError")
analyzer.transform(retryable).write.mode("overwrite").parquet(retry_output_path)
```

**There is no durable journal or exactly-once guarantee in this contribution.**
Ordinary Spark writes do not atomically commit each accepted CU handle.
Repeated actions, task retries, speculation, or failure between acceptance and
storage can repeat billable submissions. Cache/materialize results appropriately,
and resume saved handles using poll mode rather than re-running analyze mode.
Service handles can expire; save completed raw results for durable retention.

## Bounds and concurrency

| Setter | Default | Meaning |
| --- | --- | --- |
| `setConcurrency` | 1 | Concurrent operations per Spark partition. |
| `setTimeout` | 60 | HTTP connection/read timeout in seconds. |
| `setConcurrentTimeout` | Unset | Not supported; a configured value is rejected. Use the per-operation budget instead. |
| `setMaxPollAttempts` | 120 | Maximum polling GET attempts, including retries. |
| `setPollingDelay` | 1000 | Milliseconds between operation polls, before the first poll after `analyze` or `createAnalyzer` submission, and between management GET retries when `Retry-After` is absent/invalid. Poll mode sends its first GET immediately. |
| `setMaxOperationDuration` | 600000 | Best-effort total budget in milliseconds per analysis or management call. |
| `setMaxDocumentBytes` | 33554432 | Maximum binary input bytes before base64 encoding. |
| `setMaxResponseBytes` | 33554432 | Maximum bytes per response before JSON parsing. |

Management-call log events include the outcome, elapsed time, and exception type
on failure, without exception messages or service response bodies. Cleanup
failures and the first shared-stop transition in a partition emit sanitized
warnings. Shared-stop warnings include a connector-derived failure category and
Spark task identifiers when available; asynchronous HTTP workers lack Spark task
context and explicitly report it as unavailable. Detailed diagnostics remain in
the output row or exception response.

Valid `Retry-After` is not shortened to poll early. Numeric values must contain
at most 19 decimal digits; larger or malformed values use `pollingDelay`.
HTTP dates remain supported. If the delay cannot fit in the
remaining budget, the call retains any known handle and remote state. It reports
`OperationTimedOut` unless the last polling or management GET already supplied
an error; that original diagnostic takes precedence. This also applies to the
final analyzer GET after creation succeeds. Expiration does not cancel the remote operation.
If the budget expires during a retry wait or before the next GET is dispatched,
the last GET error and its HTTP status are retained, not replaced by the unsent
attempt's timeout. Management retrieval also retains that response body.
This includes expiration while waiting for DNS capacity. With a retained clean
poll/submission state, its HTTP status and raw response remain intact and the
diagnostic becomes `OperationTimedOut`. Without any preceding response, a DNS
capacity failure still reports `DnsCapacityExceeded`. DNS resolution failures
and immediate worker-submission failures are not treated as deadline expiration.
A newer failure from a dispatched GET still takes precedence over an earlier error.
If `maxPollAttempts` is reached while the operation is still `NotStarted` or
`Running`, the response keeps that status and handle and reports
`PollAttemptsExhausted` unless the last poll already supplied an error.
A transient last poll can therefore retain its service/transport diagnostic
instead of either limit code. For analysis operations, select resumable saved rows by a non-null
`operationLocation` and status `NotStarted`, `Running`, or `Unknown`, as above,
not by diagnostic code. Resume them with poll mode instead of resubmitting.
An oversized response reports `ResponseTooLarge`; it is not an empty success.
The response envelope is decoded without Spark's JSON parser, preserving
`rawResponse` values up to `maxResponseBytes`. Raise the response bound or use
smaller explicit ranges, and poll a saved analysis handle instead of resubmitting.

Additional connector-local parsing bounds apply without changing the shared
JSON dependency: JSON nesting is limited to 1,000 levels and numeric tokens to
100 characters. Quoted strings do not count as numeric tokens or nesting.
Malformed or out-of-bound JSON uses the normal invalid-response recovery path,
not a task exception; submissions retain their raw response and validated
handle, while polling retains the previous remote state and handle.
Explicit analyzer definitions use the same JSON bounds and fail before PUT
when invalid. HTTP status/header lines are limited to 64 KiB, with at most
200 headers per response; `maxResponseBytes` separately limits the decoded
entity. A header-parsing failure after sending a request is ambiguous and does
not cause automatic resubmission.

`concurrentTimeout` is rejected because a shared asynchronous wait can expire
before a queued operation starts, losing its handle. The async client wait is
unbounded; `maxOperationDuration` limits new requests and polling waits and caps
each HTTP connection/read timeout at the remaining budget. Deadline-triggered
aborts are best-effort, not a hard wall-clock guarantee: stalled cancellation or
transport cleanup can delay completion. There is no executor-wide operation cap;
concurrency follows the shared SynapseML per-partition model.
Each operation uses a fresh HTTP client with no cross-row connection pooling:
polls within that operation can reuse its connection, but separate rows incur
new connection setup and may incur additional TLS and DNS work.
DNS lookup capacity is bounded separately. The operation budget is checked
again after acquiring DNS capacity, before submitting a lookup. Already-submitted
lookups retain their capacity until the underlying resolver finishes, even if
the caller times out.

Total pressure also depends on active Spark partitions and service quotas.
These controls do not establish a cluster-wide rate limit; throttled
submissions return `Rejected`. Input bytes already
read by Spark and output JSON still need to fit available executor memory.
`maxResponseBytes` is a response-size bound, not a peak-memory cap: decoding,
JSON parsing, escaping/re-encoding `rawResponse` into the output envelope, and
parsing that envelope into a row create multiple in-memory representations.
Peak memory for large responses can be several times their byte size. Size
executor memory for this overhead across `concurrency` operations per partition
and concurrent tasks per executor, or lower the response bound and concurrency.

## Explicit analyzer management

`createAnalyzer` and `getAnalyzer` are eager driver calls, never implicit parts
of `transform`. They require scalar configuration and return raw analyzer JSON.
`createAnalyzer` additionally requires an explicitly set `analyzerId`; an unset
or cleared ID is rejected before any HTTP request. `getAnalyzer` can still read
the default `prebuilt-read` analyzer.

New CU saves preserve Spark's distinction between explicit settings and defaults,
so copying, saving, and loading an unset or cleared ID does not authorize creation.
Explicitly selecting `prebuilt-read` is allowed; there is no analyzer-ID blacklist.
Older saved stages keep their stored settings unchanged. Those files may have
materialized defaults as explicit values, so their original selection intent
cannot be recovered. Before administrative calls on an older stage, explicitly
select the intended analyzer ID (or clear it to disable creation).

If a creation deadline expires before the first poll is sent, the exception
retains the accepted PUT's HTTP status, raw response and handle. If creation has
already succeeded but the first final analyzer GET expires before dispatch, it
retains the successful operation state and adds `OperationTimedOut`; the analyzer
definition has not been retrieved. A dispatched retrieval failure instead retains
that GET's diagnostic and response. Neither case automatically repeats the PUT.
Creation-poll attempt exhaustion or transport failure also retains the management
response and any accepted handle. Do not pass a management handle to analysis
poll mode or repeat creation to recover. Use `getAnalyzer` or service tooling
to inspect the analyzer. The exception message gives neutral guidance and may
include an allowlisted diagnostic code; inspect its response for full details.

The initial PUT returns an analyzer resource, not a polling operation. Its
[`ContentAnalyzerStatus`](https://learn.microsoft.com/en-us/rest/api/contentunderstanding/content-analyzers/create-or-replace?view=rest-contentunderstanding-2025-11-01)
is retained as `Running` for `creating`, `Succeeded` for `ready`, `Unknown` for
`deleting`, and `Failed` for `failed`. The raw resource body remains unchanged;
`analyzerId` is not used as an operation ID. These mappings apply only to the
initial creation response, not analysis submissions or subsequent operation
polls. Unknown or missing statuses and malformed bodies retain their
`InvalidResponse` diagnostic rather than being assumed to be running.

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

The complete definition is forwarded to CU. `createAnalyzer` waits for its
management operation within the budget; failure/exhaustion raises an exception
carrying the response. `getAnalyzer` can inspect the analyzer afterward.
The connector never changes shared model defaults. A resource administrator must
resolve `DefaultsNotSet`. Replacing an analyzer is an explicit choice.

## Scala and validation

Scala uses the same implementation:

```scala
import com.microsoft.azure.synapse.ml.services.contentunderstanding.ContentUnderstanding

val analyzer = new ContentUnderstanding()
  .setEndpoint(endpoint)
  .setSubscriptionKey(key)
  .setDocumentUrlCol("documentUrl")
val results = analyzer.transform(documents)
results.write.parquet(outputPath)
```

Offline Scala suites run in the dedicated `contentunderstanding` UnitTests CI leg:

```bash
sbt "cognitive/testOnly *ContentUnderstanding*Suite"
```

`test_ContentUnderstanding.py` covers the generated wrapper against a loopback
REST fixture. `test_ContentUnderstandingE2E.py` is an opt-in live suite using
synthetic PDF/DOCX inputs, including ranges, preview metadata, and submit/poll.
Without explicitly configured live-service credentials, it is skipped.

Offline tests establish request forwarding, row association, failure handling,
and preservation of complete responses, including fields, confidence, grounding,
multiple content entries, and unknown properties. They do not establish analyzer
accuracy or a live service connection. A custom semantic analyzer is not required
to run these connector tests; live tests consume existing analyzers and never
provision them.
