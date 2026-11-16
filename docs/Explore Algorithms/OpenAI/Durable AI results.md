---
title: Durable AI results
hide_title: true
sidebar_label: Durable AI results
description: Opt-in Delta snapshots, committed outcomes, and explicit recovery of uncommitted AI requests.
---

# Durable AI results

**Opt-in recovery, with bounded runtime qualification:** this builds on
[the focused native APIs](Unified%20AI%20prompting.md). The checks below cover
specific local and Fabric scenarios, including controlled recovery after stopping
a Spark application; they do not establish arbitrary-runtime support or
exactly-once inference. See [issue #2800](https://github.com/microsoft/SynapseML/issues/2800).

The [opt-in recovery notebook](https://github.com/microsoft/SynapseML/blob/master/docs/Explore%20Algorithms/OpenAI/Quickstart%20-%20Durable%20AI%20results.ipynb) starts
with all execution flags disabled. The examples below assume `spark`, an ordinary
configured `prompt`, its input DataFrame `tickets`, and the `AIPrompt` import from
the native guide. Storage and model operations require explicit caller authorization.

## Durable Delta results


Set `outputPath` only when you intend to persist **both inputs and outcomes**.
Without it, no Delta dependency is needed and normal Spark replay applies:
`show`, `collect`, another action, recomputation or task retries can repeat paid
requests. Provider `store=true` is unrelated to Spark durability.
Output paths must not contain SQL substitution markers such as `${team}`, even
when Spark variable substitution is disabled. These paths are rejected before
storage is created because SQL control operations and DataFrame reads must
address the same literal directory. Resolve application variables before calling
`setOutputPath`.

```python
durable = prompt.copy({}).setOutputPath("Files/aiprompt/ticket-run")
durable.setOutputBatchSize(1000)
saved = durable.transform(tickets)  # Eager, bounded Delta commits.
# These actions read storage, not inference lineage.
saved.count()
saved.show()
status = AIPrompt.getOutputStatus(spark, "Files/aiprompt/ticket-run")
reopened = AIPrompt.loadOutput(spark, "Files/aiprompt/ticket-run")
remaining = durable.resume(spark)  # Same configuration; newly bound credentials.
```

```scala
import org.apache.spark.ml.param.ParamMap

val path = "Files/aiprompt/ticket-run"
val durable = prompt.copy(ParamMap.empty).setOutputPath(path).setOutputBatchSize(1000)
val saved = durable.transform(tickets) // Eager; subsequent actions read Delta.
val reopened = AIPrompt.loadOutput(spark, path)
val status = AIPrompt.getOutputStatus(spark, path)
val remaining = durable.resume(spark) // Requires matching configuration and no active owner.
```

**Durable stages are one-shot jobs, not reusable Pipeline stages.** A run directory
can be initialized only once. Keep a stage with `outputPath` outside `Pipeline`,
`PipelineModel`, cross-validation and other tuning wrappers. `Pipeline.fit` may
eagerly run a preceding transformer before fitting an estimator, spending inference
requests during fitting; later scoring, folds or repeated calls then fail because
the run directory already exists. Saving or copying that stage does not create a
fresh run.

Use the standalone `durable.transform(tickets)` call above, reopen its committed
results with `AIPrompt.loadOutput`, handle terminal row errors, and fit or apply a
downstream pipeline to that stored DataFrame. The downstream pipeline must not
include the durable inference stage. Use a new `outputPath` for each new inference
job; use `resume` only to continue the original saved input snapshot.

`AIPrompt.load(stagePath)` restores a saved SparkML stage and its Params;
`AIPrompt.loadOutput(spark, runPath)` reads committed outcomes without model
credentials. These paths contain different artifacts and are not interchangeable.
Scala run-management methods are on the `AIPrompt` companion for all three stage
types; the generated Python classes also inherit those convenience methods.
Use the saved native stage's reader (or `AIPrompt.load`) for SparkML persistence;
loading new native artifacts through the legacy `OpenAIPrompt` reader is outside this feature.

The caller must supply a Delta-capable runtime and a fresh run directory.
The local test profile uses the following caller-provided JVM artifact.
It is not a mandatory inference dependency and does not qualify other runtime combinations:

| Spark / Scala | Caller-provided Delta artifact |
| --- | --- |
| 3.5 / 2.12 | `io.delta:delta-spark_2.12:3.2.0` |

Other Spark/Scala ports require separate source adaptation and qualification;
this table does not establish their compatibility. Durable mode currently requires
the `org.apache.spark.sql.delta.sources.DeltaDataSource` class,
`DeltaSparkSessionExtension` in `spark.sql.extensions`, and `DeltaCatalog` in
`spark.sql.catalog.spark_catalog`. The extension and catalog must be configured
before the Spark session is created.

The opt-in `aiprompt_fabric_recovery_e2e.py` probe
[passed](https://github.com/microsoft/SynapseML/pull/2813#issuecomment-6765779154)
Fabric OneLake interruption, owner release and resume within one managed Spark
application, using bundled Delta and synthetic HTTP responses. A separate
[three-application check](https://github.com/microsoft/SynapseML/pull/2813#issuecomment-6767060133)
tested build `0eef1a46` on Fabric Spark `3.5.5.5.4.20260921.11` with OneLake
and the implicit Fabric Responses endpoint. It interrupted after a committed
batch, verified the writer's Spark context had stopped, released the exact owner
from a second application, resumed pending rows, then reopened the completed
results from a third application. Duplicate rows, raw responses, usage and the
public schema were preserved, including one intentional terminal row error.
This was a controlled post-commit interruption, not abrupt process loss or a
guarantee against replaying uncommitted paid requests.

These are opt-in checks, not continuous CI coverage. Requalify the exact build
and runtime combination before deployment; neither check establishes general
provider availability or compatibility with other Spark ports. Bundled Delta
alone does not establish compatibility:
runtimes with different provider packages or catalog/extension arrangements are
not supported by these checks. Do not replace managed-runtime jars to bypass
them. Accepted path schemes are `hdfs`, `abfs` and `abfss`, plus `file` only
in Spark local mode with a local directory on the same host. Qualified `file`
URIs must have no authority: host-qualified paths (including `localhost`) and
UNC paths are rejected before creating run storage. Non-local Spark
clusters reject `file` paths before creating run storage: node-local data files
and ownership locks cannot coordinate across machines. Local-mode recovery also
requires access to that same host and directory; it does not protect against
loss of that disk. Shared FUSE/NFS mounts are not a substitute for supported
cross-host storage. The `dbfs`, `s3`, `s3a` and `gs` schemes are not qualified.
This is not a claim of Databricks or Synapse runtime qualification. Lazy
inference does not require these durable storage capabilities.

Durable schemas widen nullability and remove Spark-internal alias/file metadata
and Delta generated-column/identity metadata, including in nested fields. Those
source-column behaviors do not belong to a materialized snapshot and Delta may
remove them on read. Ordinary user field metadata and input values are retained;
lazy inference does not apply this storage normalization.

`_control/`, `_input/` and `results/` belong to this run. There is no implicit
overwrite, cleanup, schema evolution, append of a second input, or background
job. Only bounded pending keys reach the driver; input rows remain distributed.
Repeated source rows get distinct persisted identities. Committed row errors
are terminal; retry selected errors by starting an explicit new run.
Internal row identities are excluded from prompt-template expressions, including
wildcards such as `{to_json(struct(*))}`, during both initial inference and resume.
Attachment filename interpolation does not add fields to wildcard expansion:
`{asset}` uses the attachment filename, while `{to_json(struct(*))}` retains only
the public input columns and their original values.
Durable prompt templates additionally require stable built-in expressions over input
columns and literals. Unsupported templates are rejected before run storage is created.
SQL subqueries, UDFs (even deterministic ones), nondeterministic
expressions and runtime-dependent leaves such as `current_timestamp()` are
rejected: external tables, registered functions and session/runtime values could
change between batches or resumes. Materialize those values into input columns
before `transform` and reference those columns in the template; the input
snapshot then preserves them. These additional stability restrictions do not apply
to lazy mode.
Materialize `aes_encrypt` results as well: Spark can mark encryption deterministic
even when it generates a random initialization vector. Durable templates reject all
`aes_encrypt` forms, including explicit-IV and ECB forms; materializing ciphertext
avoids recomputing it during recovery without requiring a fixed IV.
For every API, durable initialization evaluates the template over the committed
input snapshot before sending any inference requests. Before validation, the
snapshot must still have its original schema and data: only intervening `OPTIMIZE`
compaction commits are accepted. The validated input version then remains pinned
through initialization; detected out-of-band changes stop inference rather than
adopt an unchecked snapshot. A row-specific SQL error
(for example, a failing date conversion or ANSI cast) leaves the run in
`initializing`, with no resumable results and no requests sent. Correct the input
or template and use a new output directory; do not resume that incomplete run.
Malformed per-row body extensions also become committed terminal errors without
aborting initialization. Resuming a completed run checks the saved configuration
and ledger integrity, then reads committed output without re-evaluating prompt
templates or parsing input JSON again. It does not acquire an owner or modify the
control table.

**Keep the private `_input` table immutable after initialization.** Exclude it
from manual and automatic maintenance: do not `OPTIMIZE`, `VACUUM`, append,
update, or change its schema. Reads, status checks, and resume require its latest
version to equal the pinned version, even for a completed run. A later compaction
or logged `VACUUM` commit is rejected even when the pinned files remain readable;
vacuuming can also remove files required by the pinned snapshot. There is no
fallback to a different snapshot. The pre-validation compaction allowance above
does not permit maintenance after the input version is pinned.

**Defer `results/` maintenance until the run is complete and has no owner.**
Exclude it from manual and automatic `OPTIMIZE` and `VACUUM` while the run is
unfinished or owned. Between batches, the writer requires the results version
to remain unchanged; even data-preserving compaction can stop inference with
"committed outcomes changed outside the current owner" and retain ownership.
If that happens, stop the previous Spark application, establish that its requests
and tasks have quiesced, then use the stop-confirmed administrative release and
resume procedure below. After completion, maintenance must still preserve the
results schema, row identities and outcomes, and respect Delta retention and
readers of older snapshots. Never append, update or delete result rows, and leave
the private `_control/` table managed by the run APIs. This allowance for completed
results does not relax the `_input/` restrictions.

Recognized credential values are not written into the manifest or used in its
fingerprint. For these credentials, the fingerprint records only whether
authentication is configured, not its value or source. Scalar keys, tokens and
recognized custom authentication headers may rotate or replace each
other on resume. Implicit Fabric authentication also counts as configured
without fetching a token during fingerprinting. Removing all authentication from
an authenticated run fails before ownership acquisition or inference; blank
credentials and auth-named telemetry do not satisfy this check. The same
configuration check applies to completed resume; `loadOutput` remains available
without inference credentials. This does not validate a key's validity or scope:
provider authentication failures still become terminal row errors.
Authentication-named entries in scalar custom and telemetry headers
are excluded from the fingerprint; other header settings remain significant.
The excluded names are the stage's subscription-key and AAD header names
(`api-key` and `Authorization` by default), compared without regard to casing.
Subclasses that override these names use their own names for credential rotation;
headers no longer used for authentication remain part of the fingerprint.
Credentials under unrecognized header names, such as a gateway's
`Ocp-Apim-Subscription-Key` in `customHeaders`, are treated as ordinary settings:
their values contribute to the persisted SHA-256 fingerprint, do not count as
configured authentication, and cannot change on resume. Hashing is not a
substitute for protecting low-entropy secrets. Use recognized authentication
bindings or override the stage's authentication header names when rotation is
required.
Recreate the original stage class as well as its settings: the fingerprint records
the concrete class independently of telemetry, even when telemetry is disabled
or customized. `AISystemOne` and `AIPrompt(apiType="systemone")`
(likewise `OpenAIDecisions` and the Decisions facade) are inference-equivalent,
not interchangeable recovery constructors.
Use the same SynapseML build and Spark runtime versions to resume. The manifest
records both version strings, and recovery rejects a different version before
acquiring ownership or sending requests, including for completed runs.
Library-owned defaults (including prompt, output, telemetry and dated API-version
defaults) also participate in recovery identity. The manifest stores their hash,
not the original configuration; the hash cannot identify which default changed.
Finish pending runs before upgrading, or retain the original environment for
recovery. If it cannot be restored, `loadOutput` still reads committed results;
do not replay the full input.
Other effective configuration must match its fingerprint. This includes the
resolved URL of an automatically selected Fabric endpoint: its workload host,
workspace and capacity are part of the run's identity. Finish durable runs before
moving the workspace to another capacity or changing its workload routing.
If that endpoint changes, `resume` rejects the configuration; automatic migration
of a pending run is not supported. `loadOutput` can still read committed results.
To continue elsewhere, start a new run with deliberately selected unprocessed
source rows rather than replaying the entire original input.

Object
order in `responseFormat`, `extraBody` and `questions` is fingerprint-significant, including
structured-output property order. Their JSON numbers are compared by exact value
within the rendered integer or decimal kind; equivalent
decimal spellings match only when serialization retains that kind. Array order
and string values remain significant. Fingerprinting preserves UTF-16 code units:
an invalid string setting is not equivalent to a question mark, a replacement
character or literal JSON escape text. For example, changing a malformed
`systemPrompt` to `"?"` cannot resume a run whose committed rows contain
`invalid_request` errors for that original setting. SQL
expression settings also participate: time zone, ANSI and legacy semantics,
parser and function options, Base64 line wrapping, JSON/CSV parsing (including JSON partial results),
the corrupt-record field name, JSON generation, decimal precision, case sensitivity,
timestamp type, map-key deduplication and assignment policy. Changing these can
change pending prompt values even when the public schema stays the same. Restore
them on the executing session before resuming; another active session does not
determine the fingerprint. This includes registered SQL defaults, not just
explicitly configured settings. The same identity check runs before preparing
each pending batch, not only on resume. A detected change stops before that
batch sends requests and releases the quiescent owner; restore the original
settings before resuming. Do not concurrently mutate SQL semantic settings on
the executing session during a run. Resume on the original Spark runtime: a runtime
update that adds semantic settings or changes their defaults can reject recovery,
even with unchanged stage parameters. The check deliberately fails closed rather
than submit pending prompts with changed semantics. If the original runtime
cannot be restored, `loadOutput` still reads committed results; do not blindly
resubmit the full input. Spark execution tuning such as `spark.sql.shuffle.partitions`
does not participate. Stage transport settings do: `concurrency`, `timeout` and
`concurrentTimeout` must retain their original values and explicit/default bindings
on resume. The durable contract conservatively freezes these settings because
they affect request execution and failure outcomes. HTTP `timeout` exhaustion
can become a terminal row error, whereas `concurrentTimeout` expiry fails the
Spark task rather than creating a row error. If it causes a durable batch write
to fail, ownership remains held; confirm the previous execution has stopped
before releasing that owner to recover. Spark retries can resend requests in
the uncommitted batch; earlier committed batches remain protected from replay.
`concurrentTimeout` cannot be increased on `resume`. Tune these settings before
starting a run. To change them after an interruption, start an explicit new run
with a fresh `outputPath` and deliberately select the rows to submit; `resume`
neither retunes pending requests nor retries committed errors.
Only the hash is stored, not these settings' plaintext. Do not
include sensitive columns in the input unless you intend to persist them.
Credential, custom-header and telemetry-header column bindings are rejected in
durable mode, before input persistence. Use scalar headers instead. Remote media,
provider file IDs, previous responses and retrieval/tool state are rejected
before inference: saved inputs must be self-contained. This includes nested
resource bindings and `data_sources`, `web_search_options` and stored `prompt`
references in body extensions, as well as known `image_url` and `file_url`
locations and object-shaped `url` carriers (including `file_data.url`), in scalar or
per-row extensions, typed `responseFormat`/`responseFormatCol` values and native
`questions`/`questionsCol` payloads. Arrays in these
media locations are checked recursively. Only a direct scalar string `file_data`
is treated as literal content; strings inside `file_data` arrays must be inline
data URLs, and object `file_data.url` values remain location checks. These conservative
checks apply even to malformed media shapes and native JSON state.
System One JSON
state and Decisions JSON input are checked on the committed snapshot, whether
supplied by `inputCol`, a rendered `promptTemplate`, or System One's `stateCol`,
before any inference request. These checks cover all successfully parsed values,
even malformed message or question shapes; restricted dependencies abort the
snapshot rather than becoming row errors while valid sibling rows are sent.
In System One JSON state, resource-shaped fields such as
`file_id`, `image_url` and `tools` are conservatively subject to these restrictions.
At any depth, non-null `previous_response_id`, `conversation`, `conversation_id`,
`file_id`, `file_ids`, `tools`, `tool_choice`, `tool_resources`, `functions`,
`function_call`, `retrieve`, `run_id`, `data_sources`, `web_search_options`, `prompt`
and `tool_call_id` are rejected, even if intended as ordinary user-defined names.
Rename those JSON members or use text mode for literal state; ordinary text state
is not interpreted as resource bindings. Diagnostics identify restricted member
names without including their values. Question names,
System One criteria labels, and field names inside an output JSON schema are
descriptions, not resource bindings; inert URLs in ordinary text remain valid.
Only criteria labels are exempt: nested objects and arrays used as criteria values
still undergo resource checks, even when the question shape is malformed.
Typed formats use the same checks after API-specific normalization; output-schema
properties remain exempt, but neighboring format extension fields do not.
Scalar format dependencies fail before storage creation; per-row dependencies
abort snapshot validation before inference. Malformed per-row formats remain
terminal row errors and are not sent or replayed on resume.
Output-schema exemptions apply only to object-member paths in the matching API's
request body: `response_format.json_schema.schema` for Chat and
`text.format.schema` for Responses. Array-wrapped paths, `response_format.schema`,
other APIs, and similarly named paths inside native state or input are not exempt.
Top-level `extraBody.metadata` is also an inert user-defined map: names such as
`conversation`, `prompt`, `tools` or `image_url` there do not create dependencies.
Resource checks still apply elsewhere in the request, including nested extensions.
Responses `include` options that request extra output, such as
`message.output_text.logprobs` and `reasoning.encrypted_content`, are supported in
scalar and per-row body extensions. They do not enable tools or bypass the independent
resource restrictions. Requested provider fields remain available in `rawResponseCol`.
Optional resource and message ID fields containing JSON null are not external
dependencies; actual IDs and remote media remain rejected.
Malformed per-row message shapes, native JSON input and body extensions become
committed terminal row errors rather than aborting initialization. Messages that
exceed JSON parser limits or otherwise cannot be checked for external resources
instead abort initialization before inference; they are never sent unchecked.
Unsupported content
forms are rejected by request validation without sending them to the provider.
Path attachment cells use the same trimming and missing-value handling as lazy
inference: null, empty and whitespace-only cells remain row-level missing inputs
when no attachment is available. Their direct filename placeholders render as
empty strings, so an absent optional attachment does not discard another usable
attachment. Null non-path template values retain Spark's null behavior.
Inline data URLs retain their case-insensitive
scheme/base64 handling, and filename interpolation uses the same normalized
path as attachment construction; actual remote paths are still rejected before inference.
Empty or malformed `data:` media values are self-contained and do not abort the
resource check. They follow the same row-level request validation and provider
error handling as lazy inference; non-`data:` media locations remain rejected.
String-valued `file_data` is literal inline content, not a media location. It is
preserved unchanged rather than interpreted as a path or downloaded, even when
its text resembles a URL. This exemption does not extend to strings inside
array-valued `file_data`; those are checked as media locations.
Supply the encoding required by the provider, normally
a base64 data URL; preserving raw strings does not guarantee provider acceptance.
Invalid file content follows ordinary request validation and provider error
handling, and a committed provider error is not resent on resume.
Simple attachment-column placeholders follow Spark's column resolver, including
case-insensitive aliases and quoted identifiers. Escaped literal braces and SQL
expressions retain their usual template meaning; only direct column placeholders
are replaced with attachment filenames.
Arbitrary input UDTs and nested empty structs (including inside arrays and maps)
are rejected before storage creation. Decimal fields must have nonnegative scales,
including inside structs, arrays and map keys or values: Delta's Parquet storage
cannot represent negative scales even when Spark's legacy option permits them.
This durable restriction does not change lazy inference. Spark ML vectors use a reversible storage
codec, including vectors in retained map keys and values. A zero-column logical input root is supported because the stored
snapshot includes an internal row key. Durable schemas use Delta-compatible nullability.
Sibling field names at every nesting level must also be unambiguous under the
executing session's Spark column resolver; Delta statistics collection resolves
nested fields by name. This check is separate from Delta's case-insensitive
storage-name restriction, which still applies with `spark.sql.caseSensitive=true`.
Stored schema JSON, including field metadata, uses the same bounded JSON parser
as native requests; excessive nesting is rejected before creating a run.
The limit applies to the encoded representation: each struct field adds three
JSON levels, while array elements and map keys or values add one. Field metadata
shares that depth budget; it is not a separate allowance.
Malformed stored schemas reject reads, recovery and owner release without changing the run.
Missing or
incompatible mandatory output bindings are corruption errors, never evidence
of zero failed rows.

After an interruption or error in an initialized run, including a failed cell in
the same application, first inspect status and partial results. Successful completion clears ownership.
Initial execution and resumed execution both report `running` once initialized.
This state records execution progress, not application liveness; it does not
replace the owner-token and quiescence checks below.
The count-based `isComplete` flag means every input row has a committed outcome;
`allSucceeded` additionally requires zero row errors. After the last batch commits,
these flags can be true while `state` is still `running` and `ownerToken` remains
present. Neither flag proves task quiescence or authorizes owner release.
Follow the stop-confirmed release procedure below; `resume` then finalizes the run
without repeating committed rows, even if no rows were pending.
A driver-side batch-preparation failure releases only its matching owner token
when all prior actions finished successfully and the next distributed action has
not started. Correct the preparation error before calling `resume`; committed
rows are not repeated. Cancellation, failed Spark jobs and uncertain storage
outcomes retain ownership because task/request quiescence cannot be proven.
A failed or disconnected notebook cell is not proof that its executor work stopped.
If status still has an owner, **stop the old Spark application and establish that
no work remains** before using
`releaseOutputOwner(spark, path, expectedOwnerToken, confirmStopped=True)`.
Falsely confirming stopped execution does not fence in-flight writes and can corrupt the ledger.
Use the exact token from status. There is no automatic expiry, takeover or
owner-independent force flag. Recreate the same stage configuration, bind fresh
scalar credentials and call `resume(spark)`; never pass a replacement input.
An incomplete initialization is an explicit error rather than an empty success.
It does not establish that initialization failed: the creating application may
still be writing or validating the input snapshot. Wait for initialization to
finish, or confirm that the creating application has stopped before abandoning
the path, starting a replacement run, or cleaning up. This also applies while
control-table data files exist but their Delta commit is not yet visible.
Run-management calls distinguish a missing path from an existing directory that
is not an AIPrompt run. Check the supplied path in either case; these errors do
not mean an existing run needs to restart and do not authorize deleting unrelated
data. A new `transform` can still create a run at a missing or empty path, but it
never overwrites an unrelated non-empty directory.
On local storage, a failure that leaves only the control lock file can retry
`transform` on that still-uninitialized directory. If a failed control-table write
leaves data files without a committed Delta log, run-management calls and another
`transform` report incomplete initialization rather than suggesting that the
directory can be resumed. They retain those files unchanged; after confirming
stopped execution, use a new `outputPath`.
If initialization fails after
the control table is created but before the run is ready, `getOutputStatus`,
`loadOutput` and `resume` explicitly reject the incomplete run. This includes a
snapshot rejected for external input dependencies. After confirming stopped
execution, correct the input and use a new `outputPath`; releasing ownership
does not make an incomplete run resumable.
The original directory is retained for inspection, not overwritten or automatically
deleted. Stop the original Spark application and establish that no work remains
before any administrative cleanup of that directory.
Local control transactions serialize both same-driver threads and separate drivers;
the temporary file lock does not replace or expire durable ownership. Waiting for
another local driver's control lock is interruptible, including on Windows.
Cancelling that wait does not run the control transaction or clear the durable owner.
Without cancellation, acquisition waits for the current lock holder; there is no
lock-wait deadline or automatic takeover.
On remote storage, ownership races can surface Delta concurrency or path-already-exists
errors rather than an AIPrompt-specific exception. Inspect status and partial results
and follow the stopped-execution rules above. These errors do not authorize takeover,
deleting the run directory, or blindly restarting inference.

The committed results table is the recovery ledger. An uncommitted interrupted
batch may run again; ambiguous commits are reconciled from Delta on resume.
Neither Delta nor this feature provides exactly-once external inference.
Provider ambiguity, Spark retries and speculation may still incur repeat charges.

In Fabric, use the default Lakehouse's `Files/...` path or a copied qualified
ABFS path with the existing Spark storage identity. This creates a run directory,
not a catalog table. Path qualification and directory checks use the supplied
Spark session's effective filesystem configuration, including session overrides.
Local tests and OneLake require their respective Delta
transaction guarantees; arbitrary filesystem configurations, particularly
single-driver S3 configurations, do not establish cross-driver ownership.

## Scaling responsibly


Durability adds transaction and integrity-check overhead. A
[single-run synthetic HTTP check](https://github.com/microsoft/SynapseML/pull/2813#issuecomment-6766867998)
on the Fabric runtime above measured the following with batch size 1,000,
eight input/shuffle partitions and per-partition concurrency eight:

| Rows | Ordinary inference | Durable inference | Completed resume |
| --- | ---: | ---: | ---: |
| 1,000 | 2.46 s | 55.21 s | 3.58 s |
| 10,000 | 8.64 s | 159.72 s | 3.22 s |

Both modes sent one request per row; completed resumes sent none. Timings
include result verification, with an HTTP-only warmup before measurement;
durable execution also includes initialization and Delta commits. This
fast-provider example exposes storage overhead, not live-model throughput or
a statistically controlled performance guarantee. It does not qualify larger
inputs or other batch sizes. Measure with your provider, payloads and runtime
before choosing durability or increasing batch size.

Before starting a run, tune partition count and `concurrency` to the deployment's rate limit.
In lazy mode, repartition the input DataFrame deliberately. Durable mode reads a
saved snapshot, so input repartitioning does not determine inference parallelism.
Each durable batch is explicitly repartitioned to the smaller of its row count
and the executing session's `spark.sql.shuffle.partitions`. Use a positive
integer in the qualified runtimes and set that Spark option deliberately
(for example, `spark.conf.set("spark.sql.shuffle.partitions", "8")`);
it can also be changed on resume without changing saved request settings.
On runtimes that accept `auto`, durable execution uses a fixed fallback of at most
200 partitions per batch, further capped by that batch's row count. This is not
cluster-adaptive sizing and does not create one partition per row for large batches.
Use an explicit integer to tune it for your quota and available executor slots.
This fallback is unit-tested; runtime acceptance of `auto` is not qualified here.
Explicit repartitioning prevents AQE from coalescing these partitions, and Delta
appends can produce a file per non-empty partition. Both the automatic fallback
and large explicit counts can therefore increase scheduling and small-file overhead.
Do not compact those result files during the run; follow the maintenance
restrictions above after completion.
Approximate in-flight requests are active Spark partitions times per-partition
concurrency, subject to available executor slots and rows in the batch.
With `handler` unset and its default unchanged, each active partition has its own HTTP worker pool, which
can create up to `concurrency` threads as rows are submitted. The default is `1`;
explicit positive values have no additional library-imposed ceiling and are not
silently clamped to an executor-wide budget. Budget roughly `concurrency` times
simultaneously running partitions per executor for HTTP workers, in addition to
Spark's own threads. Each partition also buffers up to `concurrency` pending
request/response entries; inline media and large responses can make that buffer
expensive. With `dropPrompt=true` (the default), generated messages are not kept
as an extra copy in that buffer. Retaining them with `dropPrompt=false` adds their
payload, including attachments, alongside the request and retained input.
High settings can exhaust container thread/process limits or native
and heap memory, causing executor loss and possible request replay. Do not copy
large values from legacy `OpenAIPrompt` without measuring this per-partition
resource cost. Start at the default, then increase only within provider quota
and executor limits. Settle this tuning before starting a durable run:
`concurrency` cannot change on resume; a different value requires a fresh run.

`outputBatchSize` bounds a
durable commit, not provider quota. Start with small batches to bound the
uncommitted work exposed to a crash, then measure throughput and commit overhead.
Do not collect the input dataset or loop over rows on the driver.
The implementation collects only the selected batch's numeric keys on the driver,
then builds two literal-key `IN` predicates to select and verify that batch.
Larger batches therefore add driver memory, query-planning and plan-log overhead,
even before inference. The maximum of 100,000 rows is a bound, not a
production sizing recommendation or a qualified performance target. Increase
from small batches only after measuring this cost in your runtime.
Durable execution reuses each validated post-commit ledger and skips repeated
input count/distinct validation, not all input-key scans. Every batch joins
against input keys to select pending rows and to verify committed keys, and scans
the growing results ledger for integrity checks. In the worst case, total key-scan
work grows with input size times the number of batches. Choose `outputBatchSize`
to balance this overhead against the uncommitted work exposed to a crash;
many tiny batches can be expensive. `getOutputStatus`, `loadOutput` and successful
`resume` attempts also run full input and result integrity checks. An unfinished
run that already has an owner is rejected before scanning its input or results;
the later ownership acquisition still checks for races.
After these integrity checks, returned result DataFrames preserve Spark's column
pruning and filter pushdown for columns without vectors. Only top-level columns
containing vectors need conversion, including nested vectors; unrelated raw
responses or retained messages need not be read by a narrow projection. Vector
payload validation runs when a vector-bearing column is read, not when it is
pruned from the query. Full schema and row-key integrity checks are unchanged.
Vector-bearing containers are converted with Spark SQL functions, with UDFs only
at vector leaves. Retained vector map keys are also encoded around inference's
row conversions in both lazy and durable modes, without an extra join. This
preserves SQL-distinct dense and sparse keys even when their mathematical vector
values are equal. Use SQL `map_entries` or `explode` to inspect such keys:
collecting a whole map into a language-level dictionary can apply that
language's key equality and collapse entries independently of Spark storage.
Other UDT map keys, including Spark ML matrices nested in structs or arrays,
are rejected during lazy schema preflight rather than risk silently losing keys.
This does not restrict ordinary retained UDT columns outside map keys; durable
mode separately permits only Spark ML vector UDTs.

In durable mode all committed row errors are terminal, including exhausted
transport failures and 429/5xx responses: `resume` does not retry them. There is
no failure-rate circuit breaker; later batches continue after row errors, so an
outage or quota limit can leave many rows without model output. Inspect the error
column and `rowErrorCount`; a complete run means all rows have committed outcomes,
not that every inference succeeded. Retry selected failed rows in a new run with
a fresh output path after the service recovers, accounting for possible duplicate
provider work when the original request's outcome was uncertain.
