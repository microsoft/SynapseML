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

For a complete runnable example, use the
[semantic extraction notebook](../Quickstart%20-%20Semantic%20Extraction%20with%20Content%20Understanding).
It includes runtime/resource setup, a generated sample PDF, explicit endpoint
and storage configuration, a custom semantic analyzer, extracted fields with
confidence and source grounding, and saved-handle submit/poll recovery. It reads
saved results before displaying them and uses no CU SDK.
The [GA Analyze API](https://learn.microsoft.com/en-us/rest/api/contentunderstanding/content-analyzers/analyze?view=rest-contentunderstanding-2025-11-01)
and [Python SDK reference](https://learn.microsoft.com/en-us/python/api/azure-ai-contentunderstanding/azure.ai.contentunderstanding.contentunderstandingclient?view=azure-python)
describe the service contract; the SDK is not a dependency of this connector.

The common configuration needs only an endpoint, a key obtained from your
secret manager, and an input column. The short snippets below assume that
configuration and an input DataFrame already exist; the notebook supplies the
complete setup and sample input:

Choose a fresh output directory for each write. The examples use `errorifexists`
so rerunning a cell cannot silently overwrite saved results or recovery handles.
This is not an exactly-once submission guarantee.

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

analyzer.transform(documents).write.mode("errorifexists").parquet(output_path)
results = spark.read.parquet(output_path)
```

`outputCol` and `errorCol` are optional; defaults are unique to the stage.
The default analyzer is `prebuilt-read`, the default mode is `analyze`, and the
stable API default is `2025-11-01`. Setters do not contact CU or provision
anything. `transform` is lazy: Spark actions execute requests on executors.
Duplicate input rows are processed independently; no unique-ID column is required.

After writing, use the reloaded `results` for extraction, display, and other
downstream actions. Reusing the original transformed DataFrame for another
action executes the analysis again and can submit the same documents twice.

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
binary_analyzer.transform(files).write.mode("errorifexists").parquet(binary_output_path)
results = spark.read.parquet(binary_output_path)
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
JVM parameter maps, Scala inspection, and saved metadata still contain configured
values; redaction is not encryption or a secret-free persistence guarantee.

Python generic parameter inspection reads Python parameter maps, not JVM-backed
service settings or their defaults. Use explicit getters such as
`getSubscriptionKey`, `getAnalyzerId`, and `getModelDeployments` for service
scalars, or the corresponding `get...Col` methods for column bindings.
Copy/save/load preserve those settings on the JVM, and clearing a setting
restores any JVM default.

MLflow Spark ML **parameter logging** therefore does not collect JVM-backed
credentials configured through `setSubscriptionKey` or `setAADToken`.
This does not make all autologging secret-free: `log_models=True` can persist
those scalar credentials in saved model metadata. Use
`mlflow.pyspark.ml.autolog(log_models=False)` when fitting authenticated service
stages if model artifacts must not retain their credentials. Pending values placed
in Python parameter maps through generic `set` remain visible to parameter
collectors; use the named credential setters instead. Description redaction
does not sanitize those maps or saved model metadata. Disabling model logging
alone does not disable parameter logging.

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
| `allowInputTruncation` | Optional preview boolean query parameter overriding the analyzer's truncation setting. Unset means omitted, not `false`. |
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
preview = (
    analyzer.copy({})
    .setApiVersion("2026-06-01-preview")
    .setAllowInputTruncation(False)
)
```

Version overrides are forwarded, not checked against a version allowlist.
The connector does not discover "latest" at runtime. New service versions,
modalities, endpoints, or mandatory request fields can still require connector
changes. Raw JSON preservation is not a promise of complete REST API parity.
For the [preview Analyze API](https://learn.microsoft.com/en-us/rest/api/contentunderstanding/content-analyzers/analyze?view=rest-contentunderstanding-2026-06-01-preview),
use `setAllowInputTruncation(True/False)` or
`setAllowInputTruncationCol("allowTruncation")` with a boolean column. An unset
parameter or a null column value omits the query option; explicit `False` sends
`allowInputTruncation=false`. It is never sent on GA requests by default.
Explicit settings are forwarded without a version allowlist, so select an API
version that supports the option. Poll mode ignores this analysis-only setting.

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
`Running` can contain an empty result. In either input example above, `results`
reads the saved Parquet data, not the live transformer. Extract and display
fields from those saved results rather than assuming every analyzer returns the
same schema:

```python
completed = results.where("analysis.status = 'Succeeded'")
markdown = completed.selectExpr(
    "get_json_object(analysis.rawResponse, '$.result.contents[0].markdown') AS markdown"
)
markdown.show(truncate=False)
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
submitted.write.mode("errorifexists").parquet(handles_path)

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
polled.write.mode("errorifexists").parquet(results_path)
polled_results = spark.read.parquet(results_path)
```

Poll mode uses the API version in the saved handle; it does not require the
original document or API-version columns. Handles must match the configured
endpoint's scheme, host, port, and CU operation path. Redirects are not followed.
Analyzer-creation management handles cannot be passed to this mode; use
`getAnalyzer` or service tooling to inspect analyzer creation instead.
HTTP 404/410 while polling analysis results produces `ResultUnavailable`,
retaining the handle, when the response body can be read within the configured
limit. Unreadable or oversized responses retain the prior operation state (or
`Unknown` without prior state) and a diagnostic instead. A 404/410 from an
analyzer-creation operation lookup does not prove creation failed: the accepted
state is retained, with guidance to inspect the analyzer through `getAnalyzer`.
Other non-transient GET errors retain the previous operation status (or use
`Unknown` when polling a handle without prior state), include the poll error,
and keep the handle for a later retry; they do not prove that the remote
operation failed.

By default, POST/PUT requests are not automatically replayed by the connector. A transport
failure or ambiguous submission may return `Unknown`; inspect the outcome before
intentionally resubmitting. Only HTTP 200, 201, or 202 submissions are followed
automatically; an `Operation-Location` on another status is not followed. Safe
GET retries consume the polling budget.

### Opt-in unlimited throttling recovery

Set `analyzer.setRetryThrottledRequests(True)` to retry analysis HTTP 429s
without a retry-count or elapsed-time cutoff. The default is `False`, preserving
the behavior described above. This applies to `analyze`, `submit`, and `poll`,
not analyzer-management PUT/GET calls.

A POST is retried only after complete HTTP 429 headers without an
`Operation-Location`, which establishes that the submission was rejected.
A contradictory handle, transport failure without a definitive rejection,
or other uncertain acceptance is never replayed. Polling retries the GET against
the same saved handle; it never resubmits the document.
A shared configuration failure in a sibling row also stops rejected POST
retries in that partition, including their waits. Polling accepted handles
continues; it is not blocked by that submission-stop signal.
A suppressed POST retry reports `SharedConfigurationFailure`, retains HTTP 429
and its raw response, and nests any response-read diagnostic as `innererror`.
With the retry opt-in disabled, in-flight 429s retain their original diagnostics.

Throttled attempts and their waits do not consume `maxPollAttempts` or
`maxOperationDuration`, without refunding earlier non-throttled work.
Valid `Retry-After` seconds or HTTP dates are honored exactly, without a cap:
zero and past dates intentionally permit immediate retries. Only missing/invalid
headers use exponential fallback backoff with up to 10% downward jitter,
starting at `max(1000, pollingDelay)` milliseconds and growing to
`max(60000, pollingDelay)` milliseconds. Jitter never lowers the delay below its
starting value: the first fallback is exact, and a `pollingDelay` of at least
60000 gives a constant fallback without jitter. A fresh request starts a new backoff;
a valid header resets fallback growth. This delay cap is not an elapsed-time
or retry-count limit. Other requests and ordinary
polling still use their configured budgets, and each HTTP request retains its
connection/read timeout. The wait remains cancellable by thread interruption or
the owning Spark task, even when `Retry-After` exceeds the operation budget.

Cancelling the owning Spark task stops local polling and queued submissions,
including concurrent workers, and attempts to abort in-flight HTTP requests.
It does not cancel an operation already accepted by CU; retain any saved handles
for recovery. Blocking native DNS work can finish in its bounded worker pool
after the cancelled caller stops waiting.

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
With the default settings, the connector does not retry rejected submissions.
Opt-in recovery retries only the definitive analysis 429 rejections described above.
Because the service did not
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
saved results. Filtering a live `analyzer.transform(...)` DataFrame and taking
another action can run analysis again; filtering the `results` reloaded from
Parquet above performs no service requests:

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
analyzer.transform(retryable).write.mode("errorifexists").parquet(retry_output_path)
```

**There is no durable journal or exactly-once guarantee in this contribution.**
Ordinary Spark writes do not atomically commit each accepted CU handle.
Repeated actions, task retries, speculation, or failure between acceptance and
storage can repeat billable submissions. Write results once and read the saved
data for subsequent actions; an evicted cache can be recomputed and is not
durable handle storage. Resume saved handles using poll mode rather than
re-running analyze mode.
Service handles can expire; save completed raw results for durable retention.

## Bounds and concurrency

**Polling runs inside the Spark tasks executing an action**, not in a detached
background service. In `analyze` mode, each row submits and polls before returning
its result. `submit` returns the accepted response without polling; a later
`poll` transformation performs GET requests during its own Spark action.
Neither constructing a DataFrame nor saving a transformer starts this work.

`maxPollAttempts` bounds the number of polling GET attempts per operation,
including retries except opted-in analysis 429 recovery. `maxOperationDuration`
is a separate, best-effort elapsed-time budget, shared by submission and polling
in `analyze` mode; waiting also consumes that budget, except for opted-in
throttled attempts and their retry waits. Whichever limit prevents
further work first ends local polling, preserving the handle and latest useful
state for recovery. Reaching a limit does not establish that CU failed and does
not cancel the remote operation.

Do not set `concurrentTimeout` on CU. That shared wait can expire while an
operation is still queued or in progress and lose its handle, so CU rejects it
rather than silently abandoning work. Use the per-operation controls below and
resume from saved handles when their limits are reached.

| Setter | Default | Meaning |
| --- | --- | --- |
| `setConcurrency` | 1 | Concurrent operations per Spark partition. |
| `setTimeout` | 60 | HTTP connection/read timeout in seconds. |
| `setConcurrentTimeout` | Unset | Not supported; a configured value is rejected. Use the per-operation budget instead. |
| `setRetryThrottledRequests` | `False` | Unlimited cancellable analysis POST/GET 429 recovery; excludes throttled attempts and waits from the two operation budgets. |
| `setMaxPollAttempts` | 120 | Maximum polling GET attempts, excluding opted-in analysis 429 retries. |
| `setPollingDelay` | 1000 | Milliseconds between operation polls, before the first poll after `analyze` or `createAnalyzer` submission, and between management GET retries when `Retry-After` is absent/invalid. Also seeds opted-in analysis 429 fallback backoff, with a 1000 ms floor and downward jitter above the starting value. The first fallback is exact; values of at least 60000 give constant fallback. Poll mode sends its first GET immediately. |
| `setMaxOperationDuration` | 600000 | Best-effort budget in milliseconds per analysis or management call; excludes opted-in analysis 429 attempts and their waits. |
| `setMaxDocumentBytes` | 33554432 | Maximum binary input bytes before base64 encoding. |
| `setMaxResponseBytes` | 33554432 | Maximum bytes per response before JSON parsing. |

Management-call log events include the outcome, elapsed time, and exception type
on failure, without exception messages or service response bodies. Cleanup
failures and the first shared-stop transition in a partition emit sanitized
warnings. Shared-stop warnings include a connector-derived failure category and
Spark task identifiers when available, including the owning task on asynchronous
HTTP workers. Calls made outside Spark tasks report context as unavailable. Detailed diagnostics remain in
the output row or exception response.
Opted-in throttling emits a warning on the first retry and then at most once per
minute per partition, shared across concurrent rows, submissions and polls.
Long retry waits also check this cadence, so a continuing wait is not silent for
hours. Warnings contain only the HTTP method, cumulative partition retry count,
the current retry's configured wait, and available task IDs. Independent
protocol calls outside a partition have their own reporting scope.
There is no per-exchange completion log, avoiding a warning or summary for
every document in a large batch. Retry backoff still resets per exchange;
sharing diagnostics does not create shared throttling or admission control.
These warnings exclude URLs, credentials, headers, and response bodies.

Opted-in analysis 429 recovery honors valid `Retry-After` values, including
zero-padded decimal seconds and HTTP dates (IMF-fixdate, obsolete RFC850, and
asctime, interpreted as UTC). RFC850's two-digit year uses the HTTP 50-year
future boundary. Numeric delays beyond the millisecond
range saturate at `Long.MaxValue` instead of triggering an early retry; waits
remain cancellable. Missing or malformed values use the adaptive fallback.
Outside opted-in analysis 429 recovery, the existing numeric limit of 19 digits
remains: larger or malformed values use `pollingDelay`, and the existing
RFC1123 date parser is unchanged. If the delay cannot fit in the
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
submissions return `Rejected` by default, or wait and retry with opt-in recovery.
Input bytes already
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
    "config": {
        "returnDetails": True,
        "estimateFieldSourceAndConfidence": True,
    },
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

custom.transform(documents).write.mode("errorifexists").parquet(custom_output_path)
custom_results = spark.read.parquet(custom_output_path)
custom_results.where("analysis.status = 'Succeeded'").selectExpr(
    "get_json_object(analysis.rawResponse, '$.result.contents[0].fields.Supplier.valueString') AS supplier",
    "get_json_object(analysis.rawResponse, '$.result.contents[0].fields.Supplier.confidence') AS confidence",
    "get_json_object(analysis.rawResponse, '$.result.contents[0].fields.Supplier.source') AS source"
).show(truncate=False)
```

The complete definition is forwarded to CU. `createAnalyzer` waits for its
management operation within the budget; failure/exhaustion raises an exception
carrying the response. `getAnalyzer` can inspect the analyzer afterward.
The connector never changes shared model defaults. A resource administrator must
resolve `DefaultsNotSet`. Replacing an analyzer is an explicit choice.
The [semantic extraction notebook](../Quickstart%20-%20Semantic%20Extraction%20with%20Content%20Understanding)
expands this into a runnable example, including input creation, all returned
content entries, supporting text spans, and recovery without resubmission.

## Scala and validation

Scala uses the same implementation:

```scala
import com.microsoft.azure.synapse.ml.services.contentunderstanding.ContentUnderstanding

val analyzer = new ContentUnderstanding()
  .setEndpoint(endpoint)
  .setSubscriptionKey(key)
  .setDocumentUrlCol("documentUrl")
analyzer.transform(documents).write.mode("errorifexists").parquet(outputPath)
val results = spark.read.parquet(outputPath)
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

### Shared Foundry resource in CI

The cognitive lane of the `PythonTests` build job runs the live GA suite against
`synapseml-openai-3`, the same resource used for OpenAI validation. It uses
`https://synapseml-openai-3.services.ai.azure.com` and reads only
`openai-api-key-3` from the existing `mmlspark-build-keys` vault through the
`SynapseML Build` service connection. No separate CU Foundry resource or copied
credential is created.

The shared account must expose the CU endpoint and support `prebuilt-read` with
any required resource configuration already in place. OpenAI model access alone
does not prove CU is enabled. An authorized resource administrator must verify
that setup; the tests do not create accounts, deploy models, or change shared
model defaults. The build identity needs permission to read the named vault
secret.

The live step runs only in the cognitive lane, after the ordinary Python tests
have prepared matching generated wrappers and JVM artifacts. Fork PR execution
uses the build definition's existing comment-approval and secret-access policy,
just like the OpenAI tests; this change does not alter that policy. Missing
credentials fail the step instead of silently skipping the suite. The step has
no automatic test rerun because analysis requests are billable. Its separate
`python-test-content-understanding-live.xml` report is published alongside the
ordinary Python results. The preview case remains opt-in via
`CONTENT_UNDERSTANDING_TEST_PREVIEW=1`; normal CI validates GA.
