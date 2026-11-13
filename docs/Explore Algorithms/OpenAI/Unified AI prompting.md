---
title: Unified AI prompting
hide_title: true
sidebar_label: Unified AI prompting
description: Call five model APIs from Spark with typed inputs, native questions, and explicit diagnostics.
---

# Unified AI prompting

`AIPrompt` applies a synchronous model API to a Spark DataFrame, using Spark
partitions and bounded concurrent HTTP requests. It does not require the OpenAI
Python SDK. Existing `OpenAIPrompt` and `OpenAIEmbedding` stages remain available
with their own contracts. Migrating to `AIPrompt` is not a drop-in replacement:
for example, `AIPrompt` requires a configured model or applicable operation-specific
default rather than inheriting `OpenAIPrompt`'s `gpt-6-luna` text deployment fallback.

![AIPrompt routes a Spark DataFrame through five explicit API contracts, with shared controls, distinct outputs and compatibility limits.](https://github.com/user-attachments/assets/37cd9d44-fea4-4c0a-9610-9cf0ff14265b)

The diagram summarizes routing, not universal provider support. Inherited setters
are validated against the selected API; a model can impose further restrictions.
Foundry Microsoft Decision-1 uses SystemOne. Foundry also exposes the separate
Decisions API for supported `gpt-6-luna` deployments; these are distinct protocols,
not interchangeable model names. Check the provider's model and region availability.

| `apiType` | Input | Output |
| --- | --- | --- |
| `chat_completions` (default) | Prompt template, text column, or messages | First choice's text, or configured Spark postprocessing type |
| `responses` | Prompt template, text column, or messages | Assistant text parts concatenated in response order, or configured postprocessing type |
| `systemone` | Text/JSON state and named questions | Struct with returned model, lossless answer map and provider usage JSON |
| `decisions` | Text or documented user messages and ordered questions | Struct with returned model, typed ordered answers and normalized usage |
| `embeddings` | Text column, scalar text or prompt template | Spark ML vector |

Use `AISystemOne` and `OpenAIDecisions` directly when only that protocol is needed.
They share the facade's request validation, transport, diagnostics and persistence.
They have **no default model**. Model availability is provider-specific; selecting
an API never silently redirects to another API based on a model or hostname.

Inference is lazy: `show`, `collect`, repeated actions, recomputation and Spark
task retries can repeat paid requests. These stages do not persist input or
result datasets and do not require Delta. Provider `store=true` is unrelated to
persisting a Spark result. Plan how your application will materialize outcomes;
caching alone is not an exactly-once inference guarantee.

See the [AIPrompt notebook](AIPrompt.md) for examples. Its runnable source is
[AIPrompt.ipynb](https://github.com/microsoft/SynapseML/blob/master/docs/Explore%20Algorithms/OpenAI/AIPrompt.ipynb).

## PySpark and Scala

```python
import os
from synapse.ml.services.ai import AIPrompt, AISystemOne
from synapse.ml.services.openai import OpenAIDecisions

prompt = (
    AIPrompt()
    .setApiType("responses")
    .setUrl(os.environ["MODEL_ENDPOINT"])
    .setDeploymentName(os.environ["MODEL_DEPLOYMENT"])
    .setSubscriptionKey(os.environ["MODEL_API_KEY"])
    .setPromptTemplate("Summarize this ticket: {ticket}")
    .setOutputCol("answer")
    .setErrorCol("error")
    .setUsageCol("usage")
    .setRawResponseCol("raw")
    .setExtraBody({"text": {"verbosity": "low"}})
)
result = prompt.transform(tickets)  # Lazy: no model calls yet.
```

```scala
import com.microsoft.azure.synapse.ml.services.ai.AIPrompt

val prompt = new AIPrompt()
  .setApiType("responses")
  .setUrl(sys.env("MODEL_ENDPOINT"))
  .setDeploymentName(sys.env("MODEL_DEPLOYMENT"))
  .setSubscriptionKey(sys.env("MODEL_API_KEY"))
  .setPromptTemplate("Summarize this ticket: {ticket}")
  .setOutputCol("answer").setErrorCol("error")
  .setUsageCol("usage").setRawResponseCol("raw")
  .setExtraBody("""{"text":{"verbosity":"low"}}""")
val result = prompt.transform(tickets)
```

Do not print credentials or save a credential-bearing stage into a shared model
directory. Standard SparkML stage persistence preserves configured Params,
including explicitly supplied authentication Params.
Use the corresponding stage's `load`/`read` API to restore a saved `AIPrompt`,
`AISystemOne` or `OpenAIDecisions`, or the standard Spark `PipelineModel` loader
for a saved pipeline. Copy and save/load preserve native JSON values, parameter
defaults and explicit bindings. Saving a stage stores its configuration, not
inference outcomes.
`Pipeline.fit`, cross-validation and repeated scoring can evaluate inference
more than once. Account for those model calls before placing a stage in a
training or tuning pipeline.

Choose exactly one input binding: `promptTemplate`, `messagesCol`, `stateCol`,
`inputCol`, or `text`/`textCol`. Bind native JSON strings with `inputMode="json"`
(`setStateMode("json")` is an alias); the default treats them as text.
For System One, `stateCol` and `inputCol` are supported but distinct persisted
Params, not aliases.
For Responses and Decisions, message structs may include `type="message"` or
a null/absent `type`; other non-null message types are rejected before HTTP.
Decisions JSON messages follow the same rule; a null `type` is omitted on the wire.
For inline Decisions images, null `file_id` and `detail` options are also omitted;
non-null file IDs remain unsupported. Decisions JSON input preserves provider
extension members; `messagesCol` uses the narrower Responses-compatible conversion.
Chat message structs do not accept this discriminator.
Spark's resolver selects the top-level `messagesCol` column; nested protocol keys
are case-sensitive JSON names such as `role`, `content`, `type` and `name`.
Duplicate field names anywhere within a bound message struct fail schema planning,
including for empty inputs, rather than silently discarding one of the values.
Explicit messages already contain their system instructions and attachments:
`messagesCol` cannot be combined with an explicitly set `systemPrompt`,
`fileSizeLimitMB`, `dropPrompt`, or non-empty `columnTypes`. For Decisions it
also cannot be combined with an explicitly set `inputMode`; message structs
are already structured input. These conflicts fail before HTTP.
Unlike legacy `OpenAIPrompt`, `AIPrompt` uses an explicitly set `messagesCol`
only as an input binding, not as the name of generated output messages.
With a prompt template and `dropPrompt=False`, retained messages use a UID-based,
collision-safe name reported by `transformSchema`; there is no separate naming
Param. Rename that column on the returned DataFrame if a stable consumer-facing
name is needed. Existing `OpenAIPrompt` behavior is unchanged.
Prompt templates must render a Spark string. Convert array or binary expressions
explicitly, for example with `to_json` or `cast(... as string)`, before templating.
A template's outer projection must be row-local:
it cannot filter, aggregate or expand the input rows. Compute window expressions
(including `window(...)`), outer aggregates and generators as input columns first.
Templates may contain scalar SQL subqueries, including aggregates inside the
subquery. These queries can read other data again on each action.
Row-local templates also support command-result DataFrames such as `SHOW TABLES`
and `SHOW COLUMNS`.
Direct attachment placeholders follow Spark's column resolver and use the
attachment filename. Wildcards such as `{to_json(struct(*))}` still see only
the public input columns with their original values, not generated scratch fields.
Escaped braces and SQL expressions retain their usual template meaning.
Native and embedding input cells distinguish missing values from whitespace:
null or empty strings produce `missing_input`. System One and embeddings send
nonempty text unchanged, including whitespace-only text. Decisions requires
nonblank text, including text inside JSON messages; whitespace-only text produces
`invalid_request` without HTTP. Nonblank text is sent unchanged, not trimmed.
JSON mode requires a JSON value, so a whitespace-only JSON document also produces
`invalid_request`. Normalize or filter blank input before inference when your
application treats it as missing.

### Native question types

System One uses a named object, not a chat prompt or the Decisions question array:

```python
system_questions = {
    "repeat_contact": {
        "type": "noul",
        "instructions": "Has the customer contacted support before?",
    },
    "team": {
        "type": "choice",
        "instructions": "Which team should handle this request?",
        "criteria": {
            "billing": "Charges, invoices and refunds",
            "engineering": "Bugs, errors and outages",
        },
    },
    "severity": {
        "type": "score",
        "instructions": "How severe is this issue?",
        "criteria": ["Cosmetic", "Major", "Critical"],
    },
}
systemone = (
    AISystemOne()
    .setUrl(os.environ["FOUNDRY_ENDPOINT"])
    .setDeploymentName(os.environ["SYSTEMONE_DEPLOYMENT"])
    .setSubscriptionKey(os.environ["FOUNDRY_API_KEY"])
    .setStateCol("ticket")
    .setQuestions(system_questions)
    .setOutputCol("decision")
    .setErrorCol("error")
    .setRawResponseCol("raw")
)
```

The route is `<resource endpoint>/providers/microsoft/v1/systemone`, without an
Azure OpenAI dated API version. `Decision-1` is an example deployment name, not
a built-in default. Returned model identity can differ from the deployment alias.
`decision.answers` is a **SynapseML** `map<string,string>` whose values are complete
JSON encodings. A numeric answer remains `"1.7"` as JSON; a provider string is
quoted JSON; objects, booleans, arrays and nulls remain lossless. This does not
assert an undocumented provider answer schema. Decode values according to your
application's schema, or retain them with the raw response.

Decisions uses an ordered question array. For a supported Foundry deployment:

```python
decision_questions = [
    {"type": "predicate", "name": "repeat_contact",
     "instructions": "Has the customer contacted support before?"},
    {"type": "choice", "name": "team",
     "instructions": "Which team should handle this request?",
     "choices": [
         {"value": "billing", "description": "Charges, invoices and refunds"},
         {"value": "engineering", "description": "Bugs, errors and outages"},
     ]},
    {"type": "score", "name": "severity",
     "instructions": "How severe is this issue?",
     "levels": [
         {"label": "Cosmetic", "description": "Appearance only"},
         {"label": "Major", "description": "A task fails but a workaround exists"},
         {"label": "Critical", "description": "A task is fully blocked"},
     ]},
]
decisions = (
    OpenAIDecisions()
    .setUrl(os.environ["FOUNDRY_ENDPOINT"])
    .setDeploymentName(os.environ["DECISIONS_DEPLOYMENT"])
    .setAADToken(os.environ["FOUNDRY_TOKEN"])
    .setInputCol("ticket")
    .setQuestions(decision_questions)
    .setOutputCol("decision")
    .setErrorCol("error")
)
```

For Azure resource roots, the route is `/openai/v1/decisions`, with no dated
API-version query by default. An `/openai` or `/openai/v1` base and a complete
`/openai/v1/decisions` URL are also accepted. Use the **deployment name**, not
necessarily the underlying model name. The [Foundry Decisions documentation](https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/decisions)
lists `gpt-6-luna` support and region restrictions, including the East US 2
exclusion. Routing support is not a guarantee of availability in every Azure cloud
or region. An available System One deployment does not establish Decisions support.
For a separately authorized public OpenAI deployment, use
`setUrl("https://api.openai.com/v1")`, `setModel(...)` and
`setSubscriptionKey(...)`; its route is `/v1/decisions`.
Choice values use a tagged Spark struct
(`type`, `stringValue`, `booleanValue`); `true` and `"true"` remain distinct.
Score probability entries preserve numeric index and label. Scores must be
between zero and the last probability-entry index, inclusive; bounds are checked
on the original JSON number before conversion to Spark `DoubleType`. Out-of-range
scores produce a `parse_error`. Refusals remain
typed answers alongside any other answers. Inline images are supported in the
documented Decisions input form; hosted-image URLs are rejected. Image data URLs
accept case-insensitive scheme, media type and `base64` markers, plus MIME
parameters before `;base64`. Parameter values with special characters must use
URL escapes. The original URL is forwarded unchanged; the payload must decode
to nonempty base64 data. Syntax validation does not allocate a decoded image.
Callers must still bound inline payloads to their executor memory budget and the
provider's request limits; `fileSizeLimitMB` does not apply to Decisions input.

Native JSON validation checks the supported question and input forms, including
recognized image fields; it is not a complete provider-schema allowlist.
Additional provider-owned members are forwarded without rewriting them, and
the provider determines whether they are valid. This permits supported options
such as Decisions `prompt_cache_breakpoint` on input parts. `messagesCol` uses
the narrower Responses-compatible message converter.

The same three-question workflows are available directly from Scala:

```scala
import com.microsoft.azure.synapse.ml.services.ai.AISystemOne
import com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisions

val systemQuestions = """{
  "repeat_contact": {"type":"noul", "instructions":"Has the customer contacted support before?"},
  "team": {"type":"choice", "instructions":"Which team should handle this request?",
    "criteria":{"billing":"Charges, invoices and refunds", "engineering":"Bugs, errors and outages"}},
  "severity": {"type":"score", "instructions":"How severe is this issue?",
    "criteria":["Cosmetic", "Major", "Critical"]}
}"""
val systemone = new AISystemOne()
  .setUrl(sys.env("FOUNDRY_ENDPOINT"))
  .setDeploymentName(sys.env("SYSTEMONE_DEPLOYMENT"))
  .setSubscriptionKey(sys.env("FOUNDRY_API_KEY"))
  .setStateCol("ticket").setQuestions(systemQuestions)
  .setOutputCol("decision").setErrorCol("error").setRawResponseCol("raw")
val systemResults = systemone.transform(tickets) // Lazy; an action sends requests.

val decisionQuestions = """[
  {"type":"predicate", "name":"repeat_contact",
    "instructions":"Has the customer contacted support before?"},
  {"type":"choice", "name":"team", "instructions":"Which team should handle this request?",
    "choices":[{"value":"billing", "description":"Charges, invoices and refunds"},
               {"value":"engineering", "description":"Bugs, errors and outages"}]},
  {"type":"score", "name":"severity", "instructions":"How severe is this issue?",
    "levels":[{"label":"Cosmetic", "description":"Appearance only"},
              {"label":"Major", "description":"A workaround exists"},
              {"label":"Critical", "description":"A task is fully blocked"}]}
]"""
val decisions = new OpenAIDecisions()
  .setUrl("https://api.openai.com/v1")
  .setModel(sys.env("DECISIONS_MODEL"))
  .setSubscriptionKey(sys.env("OPENAI_API_KEY"))
  .setInputCol("ticket").setQuestions(decisionQuestions)
  .setOutputCol("decision").setErrorCol("error").setRawResponseCol("raw")
val decisionResults = decisions.transform(tickets)
```

### Validate live deployments

The opt-in live suites use synthetic inputs. Native tests exercise both the
protocol-specific stage and `AIPrompt`; generative tests exercise Chat and
Responses through `AIPrompt` with the default v1 route and structured JSON output.
Configure credentials only in the test process environment, never in a notebook
checked into source control.

| API | Required environment variables | Authentication |
| --- | --- | --- |
| Chat and Responses | `SYNAPSEML_GENERATIVE_ENDPOINT`, `SYNAPSEML_GENERATIVE_DEPLOYMENT` | `SYNAPSEML_GENERATIVE_TOKEN` or `SYNAPSEML_GENERATIVE_KEY` |
| System One | `SYNAPSEML_SYSTEMONE_ENDPOINT`, `SYNAPSEML_SYSTEMONE_DEPLOYMENT` | `SYNAPSEML_SYSTEMONE_TOKEN` or `SYNAPSEML_SYSTEMONE_KEY` |
| Decisions | `SYNAPSEML_DECISIONS_ENDPOINT`, `SYNAPSEML_DECISIONS_MODEL` | `SYNAPSEML_DECISIONS_TOKEN` or `SYNAPSEML_DECISIONS_KEY` |

Use an endpoint that exposes the selected API and a model authorized for
it. The generative deployment must support structured output and temperature,
such as `gpt-4.1-mini`; use an Azure resource root to exercise default v1 routing.
Model discovery or a successful chat request does not prove native API
availability. Configure the native endpoint and its authentication on the stage;
native APIs do not inherit process-wide OpenAI URL or API-key defaults, and the
facade does not infer an implicit Fabric native route.

```bash
sbt "cognitive/testOnly com.microsoft.azure.synapse.ml.services.ai.AISystemOneLiveSuite com.microsoft.azure.synapse.ml.services.openai.OpenAIDecisionsLiveSuite"
python -m unittest discover -s cognitive/src/test/python/synapsemltest/services -p test_AIPromptLive.py -v
```

For the Python command, install this build's generated Python packages and
configure Spark with the matching JARs first. The Python tests check the public
wrappers, runtime schema, null-input errors, native answers, usage, raw responses,
and conversion of the cached results to pandas. Each configured test makes real
inference requests and can incur charges. Missing endpoint, model, or credentials
explicitly skip a live test; a configured endpoint's failure fails the test.
Skipped tests and local protocol fixtures are not live qualification evidence.

## Typed option inventory and body extensions

Common configuration includes endpoint/complete URL, deployment/model, scalar or
column authentication, custom headers, concurrency, HTTP timeout, concurrent
timeout, output/error/usage/raw columns, and SparkML copy/save/load. An explicit
deployment/model binding wins over operation-specific global defaults. A null
explicit deployment column is a row error, not permission to use another model.
When implicit defaults select a deployment, unused model defaults do not affect
input-column validation or requests. The embedding-specific deployment
default takes precedence over the general deployment default.
Native System One and Decisions APIs require an explicit model binding and do not
inherit global chat deployment, Foundry model, endpoint or API-key defaults.
Configure credentials for the selected native provider on the stage rather than
reusing process-wide OpenAI defaults.
Endpoint URLs and complete `customUrlRoot` overrides must use a DNS host name or
IP address that Java's URI parser accepts as a server host. Host names containing
underscores are not supported by these new stages, even when a legacy stage or
self-hosted gateway accepts them. Configure a DNS-compatible alias instead.
Invalid hosts fail preflight; credentials and
fragments in endpoint URLs are separately rejected. An explicit endpoint port
must be between 1 and 65535; omit it to use the scheme's default.
The selected endpoint must contain correctly paired UTF-16 surrogates, including
in its path and query. Malformed strings fail before HTTP requests.
Valid supplementary characters and U+FFFD remain supported;
an unused `url` does not override or invalidate an explicit `customUrlRoot`.
On OpenAI-hosted endpoints (`api.openai.com` and `*.api.openai.com`, including
regional endpoints), subscription keys use the `Authorization: Bearer` header
for Chat Completions, Responses and embeddings. Decisions uses `api-key` for
Azure hosts and bearer keys for non-Azure endpoints. Entra authentication through
`setAADToken` uses `Authorization: Bearer`. Pass the raw API key to `setSubscriptionKey`, without
an `Authorization` scheme prefix. Use custom authentication headers when supplying
a complete header value. Host comparisons ignore case and one DNS trailing dot;
the supplied URL itself is retained. These rules do not classify lookalike domains
such as `api.openai.com.example.org` as OpenAI hosts.
For these OpenAI hosts, a bare `setUrl` root (with or without trailing slashes)
automatically uses `/v1` for Chat Completions, Responses, embeddings and Decisions.
For example, `setUrl("https://api.openai.com")` routes Responses requests to
`https://api.openai.com/v1/responses`. Dated API-version defaults are ignored and
explicit Chat, Responses and Decisions versions follow the rules below.
The saved URL parameter is unchanged.

### API versions

Chat Completions and Responses default to `"v1"`, selecting the OpenAI-compatible
v1 route from an Azure resource endpoint without an `api-version` query.
You can also select it explicitly with `setApiVersion("v1")`. Other strings,
including `"preview"`, `"2025-04-01"` and future version names, are forwarded
as the URL-encoded `api-version` query value. There is no version allowlist or
date-format check; the service decides whether a version is available.

| Configuration with an Azure resource root | Request route |
| --- | --- |
| Chat with no version override, or `setApiVersion("v1")` | `/openai/v1/chat/completions` |
| Chat with `setApiVersion("2025-04-01")` | `/openai/deployments/<deployment>/chat/completions?api-version=2025-04-01` |
| Responses with no version override, or `setApiVersion("v1")` | `/openai/v1/responses` |
| Responses with `setApiVersion("preview")` | `/openai/responses?api-version=preview` |

```python
prompt = (
    AIPrompt()
    .setApiType("responses")  # Or "chat_completions".
    .setUrl(os.environ["AZURE_OPENAI_ENDPOINT"])  # Resource root.
    .setDeploymentName(os.environ["MODEL_DEPLOYMENT"])
    .setAADToken(os.environ["MODEL_TOKEN"])
    .setPromptTemplate("Summarize: {text}")
    .setOutputCol("answer")
)
```

`setApiVersionCol("version")` selects the version per row, including a mixture
of v1 and query-versioned requests. Scalar/column bindings follow last-setter-wins
and survive copying and save/load. `v1` is recognized without regard to case;
other version strings retain their value.

An existing v1 base or complete v1 URL keeps its path. Explicitly choosing
`"preview"` or another non-v1 value on that URL appends `?api-version=<value>`;
choosing `"v1"` adds no version query. On non-v1 URLs, choosing `"v1"` converts
recognized Chat/Responses operation routes to the corresponding v1 route,
preserving a proxy prefix and checking any supplied deployment identity.
Without a configured version override, Chat and Responses use v1 even when
`setUrl` supplies a legacy operation route. Existing scalar or column-bound
global/parameter version defaults still override this fallback on non-v1 URLs.
Existing v1 URLs omit the query unless a non-v1 version is explicitly set.

`OpenAIDecisions` defaults to `"v1"` and calls `/v1/decisions` without a version
query. The facade's `apiType="decisions"` uses the same effective default.
Other explicit scalar or column versions are forwarded on that native route
as `api-version`; they are not rejected by a hard-coded v1-only check.
This does not claim that a future version is supported by the service.
SystemOne and embeddings retain their existing version behavior.

### Request headers and defaults

All new stages send one `Content-Type: application/json` header, replacing
caller or telemetry content-type headers regardless of casing. Other header
values retain their existing precedence; legacy stages are unchanged.
Before sending a request, these stages validate their resolved credentials,
custom headers and telemetry headers, including scalar or column credentials
after bearer conversion. Header names
must be nonempty ASCII HTTP tokens. Values may contain horizontal tabs, printable
ASCII or Latin-1 characters in U+00A0-U+00FF; other characters are rejected rather
than silently replaced by the HTTP client. Invalid resolved scalar credentials or
headers fail preflight before processing, without including
the header name or value in the diagnostic. Correct the configuration and run again.
When a higher-priority credential column or a telemetry-header column could override
a scalar value, validation of that conditional fallback waits until the row is resolved.
Null or blank credentials and missing telemetry entries can expose an invalid scalar
fallback; only those rows fail, while rows with valid overrides can run.
Invalid column-bound values instead produce an `invalid_request` row error without
sending that row; valid sibling rows can still run. Credential precedence and ignored
header values are unchanged. Schema checks do not fetch implicit Fabric credentials;
those credentials are validated when building requests. Legacy OpenAI stages are unchanged.
The shared transport adds its platform `User-Agent` afterward; this existing
header, including `MMLSPARK_PLATFORM_INFO`, is outside the stage's validation.
Use representable characters in that environment value to avoid the HTTP
client's existing character replacement.
Their shared `questions` configuration must be non-null and valid before
execution; null cells in `questionsCol` instead produce row-level `missing_input`
errors without sending requests.
Global body-option defaults that do not apply to the selected API are ignored,
including their column bindings. Global API-version defaults
are likewise ignored for native APIs, `/v1` routes and complete `customUrlRoot`
overrides. On non-v1 URLs, configured scalar or column-bound version defaults
override the built-in fallback.
An unused global URL is also ignored when `customUrlRoot` supplies the endpoint.
A null cell in a bound API-version column produces `missing_input` without HTTP;
it does not fall back to the built-in version.
Explicit `apiVersion` and `apiVersionCol` bindings remain unsupported for
SystemOne, v1 embeddings URLs and complete `customUrlRoot` overrides. For a complete override,
include any required version directly in the URL query instead. Explicit
unsupported body options still fail validation.
Deployment names used in Azure URL paths are encoded as one path segment. They
cannot contain `.` or `..` path components separated by `/` or `\`, or percent
signs that could introduce another decoding layer. The same check applies after
decoding the deployment segment in a complete operation URL. Ordinary encoded
characters remain supported; model names sent only in native or `/v1` request
bodies are not subject to this path restriction.
As with legacy OpenAI stages, v1-base detection ignores case.
AIPrompt also recognizes complete API routes without regard to case, using the raw
path: percent-encoded separators such as `%2F` remain data, not route boundaries.
A deployment named `v1` does not itself select a v1 route. Chat uses its
configured API version or the v1 fallback; embeddings retains its dated default.
For Azure, supply the resource endpoint plus `deploymentName`, or the
complete operation URL. An incomplete `/openai/deployments/<name>` base passed
to `setUrl` is rejected rather than interpreted as an operation.
`setUrl` removes trailing slashes before adding a route. Existing v1 paths and
non-rewritten paths preserve their casing; the endpoint must accept the supplied path.
Complete API URLs never receive a duplicate route suffix. A
deployment in a complete Azure URL must match the effective deployment: scalar
contradictions fail preflight, and column-value contradictions produce row errors
without HTTP. A complete `/models/chat/completions` URL uses `model`, not
`deploymentName`. These checks also apply to recognizable routes in
`customUrlRoot`; the complete override itself retains its path and query bytes,
including trailing slashes, and never receives an appended route or API version.
In Fabric, Chat Completions, Responses and embeddings automatically use the
Fabric endpoint when no stage URL, complete override or global URL is configured.
This execution-time default is not saved into the stage or global settings.
Native APIs still require a configured endpoint. Outside Fabric, configure an
endpoint with `setUrl` or `setCustomUrlRoot`.
Automatic Fabric credentials are available only for the
trusted Fabric destination, never an unrelated global URL or explicit custom endpoint.
On the implicit Fabric endpoint, requests retain SynapseML's background/flex
workload headers, including when explicit credentials are supplied. Custom and
telemetry headers cannot override that classification, regardless of casing or
scalar/column binding. Explicit endpoints retain their caller-supplied headers.
Unlike legacy OpenAI stages, AIPrompt treats even an explicit `setUrl` or
`setCustomUrlRoot` pointing to the exact managed Fabric endpoint as caller-owned:
configure credentials and workload headers yourself, or leave the endpoint
implicit to use automatic Fabric authentication and classification.
Scalar models use the existing Fabric model-status preflight before inference.
Column-selected models remain distributed and are validated by
the inference endpoint; they are not collected to the driver for preflight.
Schema planning itself never performs that service call.

HTTP `timeout` is measured in seconds and must be finite and at least `0.001`
(one millisecond); smaller values are rejected before inference.
Very large values saturate at the HTTP client's limit of 2,147,483,647 milliseconds
(about 24.9 days), rather than wrapping into a disabled deadline.
When set, `concurrentTimeout` must be finite and at least `0.000000001` seconds
(one nanosecond). It limits the wait for each next result, starting when the
iterator begins waiting for that result, not when its request was submitted.
Requests overlap, so this is not a per-request wall-clock deadline or a deadline
for the entire batch. Expiry cancels all in-flight workers for that task and fails
the Spark task; it does not produce a `transport_error` row. Spark task retries
may resend requests from the failed task, including ones whose external calls
already completed. If retries are exhausted, the Spark job fails.
The default bounded HTTP handler propagates Spark task cancellation and concurrent
timeouts, including at concurrency one, to its own requests and retry workers,
including during Retry-After backoff. A shared executor-local monitor observes
Spark cancellation even when downstream row processing is blocked, interrupting
workers and starting a scope-owned abort pass without waiting for another row.
A slow abort cannot block the shared monitor or cancellation of unrelated tasks.
Task cleanup waits up to five seconds in total for its abort pass and HTTP workers
to stop. A failed abort wait still attempts to drain HTTP workers within the same
budget; timeout or interruption also interrupts the abort pass. Failure to confirm
quiescence is an execution error, not a successful result. An already-sent provider
request may still complete or incur charges.
Non-fatal abort callback errors are logged but do not fail task cleanup when both
the abort pass and all HTTP workers have stopped. Interruption and fatal callback
failures still fail cleanup after worker draining is attempted.
Explicitly calling `setHandler` with any handler, or replacing the handler
default in a subclass, opts out of
the default transport protections: it selects the legacy HTTP client, whose
automatic retries, redirects and cookie management remain enabled and whose requests are not
registered for cancellation aborts. Leave `handler` unset and its default unchanged
to retain the bounded transport guarantees below. Legacy stages keep their existing transport behavior.
Caller `Content-Length` and `Transfer-Encoding` headers are rejected case-insensitively:
the HTTP client owns request framing. Scalar custom/telemetry headers fail preflight;
column-bound headers produce `invalid_request` for the affected row before HTTP.
The same restriction applies to outgoing authentication headers introduced by subclass
header-name hooks, after credential selection and bearer conversion. Unused hooks do not
invalidate requests, and a column-dependent credential fallback is checked per row.
Other valid caller headers, including `Host`, `Connection`, `Expect`, `TE` and
`Upgrade`, retain their existing behavior.
Saving an AIPrompt stage with a replaced handler default is rejected, including
defaults replaced in subclass constructors. To persist a custom handler, leave the
original default unchanged and use `setHandler`; the explicit binding survives
copy/save/load. Loading a persisted custom handler requires
`spark.synapseml.legacy.allowUnsafeJavaDeserialization=true`. Enable this only for
trusted artifacts: Java deserialization can execute code. Stages using the
unchanged bounded default do not require this opt-in.
Rejecting replaced defaults avoids silently restoring different executable
behavior on load. All ordinary parameter metadata, including defaults hidden by
explicit values, is encoded before a direct stage overwrite deletes an existing
artifact. This includes native JSON parameters and scalar service parameters.
The metadata and parameter decoders must also accept the saved values under the
runtime's Spark JSON parsing limits, so standard Spark pipeline loaders can read
them. Oversized values fail with an actionable error before a direct stage overwrite.
Metadata containing literal unpaired UTF-16 surrogates also fails before saving:
Spark must not replace an invalid string setting with a valid but different prompt.
This covers explicit values and defaults, including shadowed defaults. Valid
supplementary characters, U+FFFD and literal JSON escape text remain unchanged.
Obtaining the writer (`stage.write` in Scala or `stage.write()` in Python) checks
handler defaults and parser configuration; metadata encoding happens when saving.
The guard does not make storage writes atomic or protect an enclosing Spark
`Pipeline` or `PipelineModel` overwrite. To preserve an existing artifact against
storage or complex-parameter serialization failures, save to a new path instead
of overwriting it.
The internal bounded handler is not a public retry-tuning API.
The bounded handler shares its finite retry budget across connection failures,
retryable HTTP statuses and one implicit Fabric authentication refresh. With no
retries configured, a 401 is returned without refreshing. This is intentionally
different from the legacy handler's additional authentication replay.
The default AIPrompt transport disables Apache's internal retries and automatic
redirects. Redirect responses (`300` through `399`) are terminal: they are neither
followed nor retried, and become `http_error` rows with their original status.
Once a redirect status is received, even an I/O failure reading its body or metadata
preserves that status instead of replaying the request. A sanitized body-unavailable
message is logged and exposed through `rawResponseCol`, if enabled.
This avoids repeating an accepted POST or forwarding authentication headers
to a different destination.
Automatic cookie management is also disabled: response `Set-Cookie` headers
are not stored or replayed on later rows or retries. Requests
therefore do not acquire hidden session state from another row's credentials.
An explicitly configured caller `Cookie` header is still sent unchanged.
Legacy HTTP clients retain their existing retry, redirect and cookie behavior.
If token refresh fails, the original 401 becomes a terminal `http_error` row
without another inference request. Interruption and cancellation still propagate.
Successful HTTP responses (`200` through `299`) are terminal once their body and
HTTP metadata have been read completely. Malformed JSON in a fully read response
produces a row diagnostic rather than another inference request. An I/O failure
while reading the body or HTTP metadata, even after a successful status arrives,
can resend the POST within the shared retry budget. The provider may already
have processed it, so these retries can duplicate work and charges. Unlike this
read-failure case, an available redirect status is preserved without replay.

| Typed request options | Supported APIs |
| --- | --- |
| `maxTokens`, `maxCompletionTokens` (mutually exclusive aliases) | Chat: `max_completion_tokens`; Responses: `max_output_tokens` |
| `temperature`, `topP`, `user`, `verbosity`, `reasoningEffort`, `responseFormat` | Chat, Responses |
| `n`, `stop`, `presencePenalty`, `frequencyPenalty`, `seed` | Chat |
| `store`, `previousResponseId` | Responses |
| `dimensions`, `user` | Embeddings |
| `safetyIdentifier` | Decisions |
| `questions` | System One, Decisions |
| `stateCol` | System One |
| `systemPrompt`, `columnTypes`, `fileSizeLimitMB`, `dropPrompt` | Chat, Responses with generated messages, not `messagesCol` |
| `postProcessing`, `postProcessingOptions` | Chat, Responses |
| Legacy `echo`, `cacheLevel`, `bestOf`, `logProbs` | Explicitly rejected; not silently dropped |

The newer typed OpenAI controls (`promptCacheOptions`, `promptCacheKey`,
`promptCacheRetention`, `serviceTier`, `metadata`, `reasoning`, `include`,
`truncation`, `logitBias`, `stopSequences`, `logProbsEnabled`, `topLogProbs`)
are supported by the legacy OpenAI stages, not by `AIPrompt`'s typed adapter.
Explicit scalar or column bindings fail before HTTP on the facade, including its
native subclasses. For provider-supported extensions, use wire names through
`extraBody`, for example
`setExtraBody({"prompt_cache_options": {"mode": "auto"}, "prompt_cache_key": "job"})`.
Provider support and cache hits are not guaranteed.

When configured explicitly or through a parameter default, `fileSizeLimitMB` must
be positive. An explicit value overrides its default. Invalid limits, including NaN,
are rejected during preflight in Scala and Python before HTTP.
A file exceeding a valid limit still produces a row error. For
filesystem attachments, `AIPrompt` reads at most the configured byte limit plus
one byte to detect overflow; it does not first buffer the entire file. Unset or
positive-infinite limits remain unlimited. `AIPrompt` captures the input Spark
session's Hadoop configuration, including session overrides, when `transform`
builds the lazy plan. Configure filesystem access before calling `transform`;
later configuration or credential rotation requires a new `transform` call.
Legacy `OpenAIPrompt` continues using its executor-default Hadoop configuration.
Null, empty or whitespace-only attachment paths contribute no attachment; their
direct filename placeholders render as empty strings. Other usable attachments
and prompt text are preserved when at least one configured attachment is usable.
If every configured attachment is absent, the row produces `missing_input` even
when its prompt text is populated. Null non-path template values retain Spark's
null behavior.

**Token-limit compatibility:** Both `AIPrompt` and legacy `OpenAIPrompt` Chat
send `maxTokens` as `max_completion_tokens`, not `max_tokens`;
`maxCompletionTokens` uses the same wire field. Responses maps either alias to
`max_output_tokens`. An older dated
API or compatible endpoint may require `max_tokens` instead. For such an
endpoint, leave both typed token options unset and use
`setExtraBody({"max_tokens": 32})` in Python or
`setExtraBody(Map("max_tokens" -> 32))` in Scala. This wire-name override cannot
be combined with an explicitly set typed token option, including a column binding.

**Responses sampling:** `AIPrompt`, `OpenAIPrompt` and `OpenAIResponses` do not
infer model capabilities from deployment names. Responses forwards configured
`temperature`/`topP` as `temperature`/`top_p`, including for GPT-5-named
deployments. The legacy stages no longer silently suppress those options by
name. `seed` remains unsupported for Responses and is rejected when configured.
Some reasoning models reject sampling options. Leave unsupported options unset,
and clear inherited sampling defaults with `OpenAIDefaults.resetTemperature()` /
`resetTopP()` in Scala or `reset_temperature()` / `reset_top_p()` on a Python
`OpenAIDefaults` instance before executing. A provider rejection becomes a row
error.

`AISystemOne` and `OpenAIDecisions` specialize the shared `AIPrompt` implementation
and inherit its setters; they do not expose a separately narrowed Param surface.
Their API selector is locked to their native protocol. The capability table
still applies: setting an inherited generative, embedding, or other-protocol
option fails validation before inference rather than silently ignoring it.
The direct stages keep their documented package names: `AISystemOne` is in
`services.ai`, while the OpenAI-branded `OpenAIDecisions` is in `services.openai`.
Both belong to the cognitive module and reuse the shared facade rather than
independent provider implementations.

ServiceParams retain last-setter-wins scalar/column semantics. `setModelCol`,
`setStoreCol` and `setResponseFormatCol` are also available. Native outputs and
embedding vectors are not fed through generative-text postprocessing.
`setResponseSchema` configures the wire JSON schema; `setPostProcessing` and its
Spark DDL options configure local text parsing. These are separate contracts.
`setResponseFormat` accepts a schema/format map or the string shorthand `"text"`
or `"json_object"` in both Scala and Python. Shorthands ignore case and surrounding
whitespace; a blank string leaves the current binding unchanged. Pass scalar
schemas as maps, not JSON-document strings. Python constructors and `setParams`
follow the same rules.
For Chat and Responses, `responseFormat` and `responseFormatCol` preserve format
descriptions and provider extension fields while translating between the nested
Chat and flat Responses shapes. Put format metadata inside `json_schema` when
using the nested shape; mixing that wrapper with other top-level format fields,
or putting `type` inside it, is rejected rather than silently discarding fields.
Descriptions in a raw JSON Schema remain inside the schema.
`responseFormatCol` accepts a JSON string or a map with string keys throughout
and JSON-compatible scalar, array and map values. Non-string map keys and
struct, binary, date/time, interval or UDT values are rejected during schema validation,
before inference. Convert richer SQL shapes to an explicit
JSON string first. Invalid format contents within a supported column type remain
per-row request errors.
JSON postprocessing follows Spark's nullable parsing semantics, including when
the supplied DDL contains `NOT NULL`. Planned and actual output schemas make
nested struct fields, array elements and map values nullable; `NOT NULL` is not
an output validation constraint.
JSON object order is preserved, including structured-output property order.
Use a Python dictionary, Scala `ListMap`, or Java `LinkedHashMap`
when the provider should follow a particular property order; an unordered map
cannot establish that order. Copying and saving/loading a stage preserve it.
JSON-string columns and parameters that accept JSON text also preserve object order.
This includes top-level `extraBody` fields, named native questions and their
criteria. JSON decimal precision is preserved for both explicitly set values
and JVM-defined defaults. `setResponseSchema` uses the same JSON value rules as
`setResponseFormat`, including exact JVM decimal values and Python integers
larger than 64 bits through the setter, constructor and `setParams`. Python
dictionaries require string keys at every level. The new APIs accept finite Python
`decimal.Decimal` values in `extraBody`, `responseFormat`, `questions` and
`setResponseSchema`, including constructor and `setParams` inputs where applicable.
Their JSON parameter getters preserve integers as `int` and return `float` when
its JSON representation preserves the number; otherwise they return `Decimal`.
This preserves exact values when passing a getter result back to a setter.
Numeric spelling can change: for example, `2.50` becomes `2.5` and `1e2`
becomes `100.0`. Providers can distinguish the **rendered** integer and decimal
JSON forms, such as `32` and `32.0`.
The JVM stores numbers as `BigDecimal`; a stored scale of zero renders as an
integer. Consequently, input tokens such as `1e0` and `3.2e1` render as `1` and
`32`, and their Python getters return `int`. JVM floating-point conversion can
also yield scale zero: Scala `12345678.0` renders as `12345678`, unlike the Python
float input `12345678.0`. These rendered forms, not the original language type
or token spelling, are sent to the provider.
Use `Decimal("0.12345678901234567890123456789")` for precise Python inputs, or
pass JSON text to a parameter that accepts it,
for example `setExtraBody('{"threshold":0.12345678901234567890123456789,"limit":1e400}')`.
The JVM parses that text without converting its numbers through Python floats.
Converting a value to `float` before passing it can already lose precision;
`float("1e400")` is infinity and is rejected.
Non-finite `Decimal` values are also rejected. Standard Python `json.dumps`
does not serialize `Decimal`; pass getter results directly to these setters
rather than converting them through `float`. Legacy OpenAI stages retain their
existing Python numeric conversion behavior.
Scala and Java `Float` values retain their decimal
representation rather than being widened to a `Double` before JSON encoding.
Loaded Python stages also retain ordinary parameter
defaults in their Python form, including empty dictionaries for `columnTypes`
and `postProcessingOptions`. Attachment parts from
`columnTypes` follow input DataFrame column order, not map insertion order;
reorder the input columns or provide explicit messages to choose a different order.
`columnTypes` keys must also be distinct under Spark's column resolver. With
case-insensitive resolution, `asset` and `ASSET` cannot both be keys, even when
their kinds agree: preflight rejects the duplicate before HTTP.
With case-sensitive resolution, distinct input columns may use those names.
`responseIdCol` exposes an observed response ID; Responses `store` can enable
the default ID column but does not persist a Spark result.

`setExtraBody` accepts a Python dictionary or JSON-object string, Scala map/`JsObject`/JSON-object
string, or Java map. `setExtraBodyCol` accepts a JSON-object string column; null
means no extension for that row. Unknown provider-owned fields are forwarded
unchanged. Use **wire names**, not Spark setter names.
`extraBody` and `responseFormat` reject null scalar objects and null column names,
including through generic JVM parameter assignment. Nested JSON nulls and null
cells in these optional object columns remain supported.
Inference also accepts an empty-named input column, whether retained or bound
to an input option. Output-role names must be nonempty.

An extension can replace an implicit default, but cannot override a subtree
owned by an explicitly set typed option, even with an equal value. Disjoint
object siblings merge recursively. Arrays, scalars and nulls are atomic.
Token aliases conflict semantically, not only by spelling. Invalid scalar
configuration fails preflight; invalid row extensions produce row errors.
Duplicate keys, cycles, non-finite numbers and non-object extensions are rejected.
JSON parameter values are limited to 100 nesting levels below their root,
including scalar leaves. Object and question parameters preserve that same limit across Scala/Java
containers, typed JSON values, JSON strings and save/load; the persistence
envelope does not consume a payload level. Scalar response formats are stored
in Chat's normalized shape: the `json_schema` and `schema` wrappers consume two
levels, leaving 98 levels inside the schema. Deeper schemas are rejected before
replacing an existing scalar or column binding, including with `setResponseSchema`.
Responses reply parsing reserves three additional envelope levels for an echoed
`text.format.schema`, without increasing request parameter limits. Duplicate-key,
numeric-token and malformed-JSON checks still apply to the whole reply.
Native request JSON parsing allows strings up to the JVM string-size limit rather
than a runtime-dependent smaller Jackson default, without changing legacy stages'
parsers. Stage persistence is different: saved parameter metadata must satisfy
Spark's JSON parser limits (Jackson's default string limit is 20,000,000 characters).
Oversized parameter metadata is rejected before a direct stage overwrite; keep
large per-row values in input columns rather than saved scalar parameters.
Available memory and provider payload limits still apply during inference.
The native parser's 1,000-character numeric-token
constraint also applies to serialized numbers supplied through Scala/Java
containers and typed JSON setters, so accepted values remain decodable during
save/load. The exponent in scientific-notation serialization must not exceed
2,147,483,647; a short token such as `100e2147483647` is therefore rejected too.
Parser-limit diagnostics distinguish those limits from malformed JSON.

Identity/input, route, authentication, headers, retry and storage settings cannot
be overridden through the body. Streaming and background lifecycles are rejected.
Embeddings support only absent or `"float"` `encoding_format`, not base64.
Tool requests require `rawResponseCol`; the stage does not execute tools or run
a tool loop. Provider rejection is reported rather than silently removing fields.
Responses output-only `include` options, such as `message.output_text.logprobs`
and `reasoning.encrypted_content`, can be supplied through body extensions;
their returned fields remain available in `rawResponseCol`. A string `file_data`
is literal content, not a path to download, even when it resembles a URL.
Supply the encoding required by the provider; preserving the string does not
guarantee provider acceptance.

## Output and errors

Input columns remain in order. A requested output can replace an existing input
column in place. Output, error, usage, raw and response-ID roles must be distinct
under the configured Spark case resolver. Scratch columns never replace inputs
or public outputs. `transform` uses the input dataset's Spark session for planning;
`transformSchema`, which has no dataset, uses the active session and is side-effect free.
Use the execution session as the active session for schema checks so column
resolution and template analysis use the same SQL configuration as execution.
Schema checks do not run Spark jobs or make model calls. Model availability and
row-dependent checks occur during execution.

`rawResponseCol` contains the complete UTF-8 response body before typed projection,
including unknown fields and all chat choices, except that bodies containing
invalid UTF-8 are discarded.
It never contains HTTP headers. Invalid UTF-8 in a successful HTTP response produces
a terminal `parse_error` without retrying; the answer, raw body, usage and response
ID are null rather than silently repaired text. Non-success HTTP responses retain
their `http_error` status even when the body cannot be decoded. Valid Unicode,
including a correctly encoded replacement character (U+FFFD), is preserved.
Raw responses and retained prompts can contain sensitive user or provider data.
Apply appropriate access controls before displaying or saving them.
JSON strings and object keys must also contain correctly paired UTF-16 surrogates,
including characters decoded from JSON escapes. Lone surrogates are rejected, not
replaced: malformed scalar JSON parameters fail validation, malformed row-bound JSON
produces `invalid_request` without HTTP, and malformed provider JSON produces
`parse_error` with no decoded answer, usage or response ID. An otherwise valid UTF-8
raw response remains available for diagnosis. Valid supplementary characters are
preserved through requests, responses and persistence.
Prompt templates are checked before Spark constructs string literals.
A literal lone surrogate fails template validation instead
of being repaired and sent as a different prompt or JSON state.
Dated-route API versions are checked before URL encoding: malformed scalar or
default values fail preflight, while malformed column values produce `invalid_request`
without HTTP. Valid Unicode is encoded unchanged; unused version defaults on
non-dated routes remain ignored.
The same checks apply to model or deployment names embedded in deployment URLs:
selected scalar and default values fail preflight, and column values fail per row.
Names used only in request bodies retain per-row validation, and overridden
defaults remain ignored.
URL-related Unicode errors name the selected parameter (`url`, `customUrlRoot`,
`model`, `deploymentName` or `apiVersion`) without including its value.
JSON postprocessing also checks decoded strings and keys, including nested fields
outside the configured schema. Invalid escapes produce `parse_error` rather than a
successful, silently repaired answer. As with other postprocessing conversion errors,
Spark's partial parsed output may remain present; check `error` before using it.
The original raw response is retained, and the existing Spark JSON syntax and read
options are unchanged.
Chat projects the first choice; Responses concatenates assistant text, not tool
arguments. Diagnostics in `error.response` contain stable JSON `code` values:
request/input validation, transport/HTTP failure, provider failure, refusal,
incomplete result, no-text result and parse failure are distinct. Missing text
does not imply safety filtering. Available usage and partial/raw content are
retained; unknown usage is null rather than invented zeroes.
For lazy path attachments in `AIPrompt`, read failures (including filesystem or security
exceptions) produce a sanitized `input_io_error`;
invalid content or attachment settings produce `invalid_request`. Neither sends
an inference request. A storage-client cancellation fails the `AIPrompt` task with a
fixed-message `CancellationException`, without the client's original message or cause.
This is a task failure, not a Spark task kill: Spark retries can repeat earlier
inference in that lazy partition. Legacy `OpenAIPrompt` instead retains its existing
sanitized attachment-error row when neither the thread nor the Spark task is interrupted.
Both stages stop on `InterruptedException` and `ClosedByInterruptException`.
Unlike earlier legacy handling of non-fatal read failures, they also stop when
a read fails while the thread is interrupted or the Spark task is canceled.
Actual thread interruption is preserved; a Spark task-only cancellation does not
set the executor thread's interrupt flag, so cleanup honors `interruptOnCancel=false`.
HTTP and attachment task-kill exceptions preserve Spark's recorded cancellation
reason rather than replacing it with a generic message. The bounded handler also
rechecks cancellation after a rejected worker submission and before reporting
an expired result wait.
Null entries in optional usage-detail maps are omitted without discarding other
counters. When supplied, the main input, output and total token counters must still
be non-negative integers; explicit null or invalid numeric values are parse errors.
Non-null Responses `incomplete_details` marks partial output incomplete even
when the top-level status is absent or inconsistent.
Answer, usage and response ID are validated independently: malformed optional
metadata does not discard a valid answer or other valid observations. Existing
provider diagnostics take precedence over metadata parse errors. This includes
optional `model` metadata for Chat Completions, Responses and embeddings. Native
responses still require a valid model in their typed output. Non-success HTTP
responses retain valid usage and identity, but keep the HTTP status diagnostic.
An established provider failure remains `provider_error` even when its Chat
`choices` or Responses `output` is null or malformed. Valid partial text is
retained in order where it can be decoded; Chat still projects only the first
choice and never substitutes a later choice for an invalid first one. Malformed
answer fields in an otherwise successful response yield a null answer and
`parse_error`, even if another choice or content part was valid.
Optional embedding `object` tags may be absent or null; non-null tags and the
vector's index, values and shape are still validated.
Malformed optional error objects, chat finish/refusal fields and Responses status
fields likewise retain valid answers. Non-null `incomplete_details` still reports
incomplete output even if its details are malformed; successful answer fields remain strict.
Response-body and HTTP metadata read failures preserve a received redirect or
error status. The same status policy still applies: ordinary `4xx` errors are
terminal, while `429`, `5xx` and the managed Fabric authentication refresh use
their existing bounded retry rules. An unreadable body is replaced with a
sanitized diagnostic, not a successful response. Read failures after a `2xx`
status or before a usable status consume the connection retry budget and can
replay an already-processed, billable request. Exhausting those retries produces
`transport_error`. Once a complete response has been buffered, a
connection-close IO failure is logged without discarding the answer or resending
the request. Legacy HTTP handlers retain their existing behavior.
JSON postprocessing also reports `parse_error` for malformed JSON or incompatible
struct fields, even when Spark's permissive parser returns a partially populated
struct. After removing supported code fences, text must contain exactly one
complete JSON value; trailing fragments or another value are errors.
Spark's root-object conversion to a singleton array of structs is preserved, with
the same field-conversion checks as an explicit array.
This check uses Spark's JSON read syntax, not the stricter validation used for
provider request JSON: single-quoted keys and strings, `NaN`/`Infinity` in floating
fields, and duplicate keys (last value wins for struct fields) retain Spark's
default behavior.
A valid object containing only null fields is not a parse failure.
Partial parsed values and raw output remain available; an existing provider
diagnostic is not replaced.
Explicitly disabling postprocessing with `setPostProcessing("")` survives
copy and save/load. Clear that Param before supplying options that infer a mode.

## Scaling responsibly

Before inference, tune partition count and `concurrency` to the deployment's rate
limit. Repartition the input DataFrame deliberately; do not collect a production
dataset or loop over its rows on the driver.
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
and executor limits. Responses are buffered in memory; budget for their size as
well as the number of concurrent requests.

Retained vector map keys are encoded around inference's
row conversions without an extra join. This
preserves SQL-distinct dense and sparse keys even when their mathematical vector
values are equal. Use SQL `map_entries` or `explode` to inspect such keys:
collecting a whole map into a language-level dictionary can apply that
language's key equality and collapse entries independently of Spark storage.
Other UDT map keys, including Spark ML matrices nested in structs or arrays,
are rejected during lazy schema preflight rather than risk silently losing keys.
This does not restrict ordinary retained UDT columns outside map keys.

The adapter prepares scalar JSON options once and resolves column positions
outside the row loop. The facade uses the shared HTTP transport and one bounded
retry policy, not a second outer retry loop. When a valid Retry-After hint is
present, the retry waits at least that long. Hints longer than 64 characters are
treated as unusable before numeric or date parsing to bound parsing work.
Decimal-second hints are rounded up to the next whole millisecond. Hints above 60 seconds terminate the
bounded request with the original HTTP error rather than sleeping indefinitely
or retrying early. This includes implicit Fabric authentication repair after a
401: a usable hint delays token refresh and replay, and a hint above the cap
returns the rejection without refreshing. Without a usable hint, authentication
repair is immediate. It happens at most once and consumes the same retry budget.
If an error response body cannot be read, a received usable
hint still applies: the HTTP status and hint are preserved with a sanitized
body-read diagnostic, rather than retrying as though no response was received.
Without a usable hint, 429 responses use bounded exponential
backoff with jitter; other retryable failures use the 100/500/1,000 ms schedule.
Unlike the legacy HTTP policy, 429 retries consume this finite retry budget.
There is no failure-rate circuit breaker; other rows can continue after row
errors, so an outage or quota limit can leave many rows without model output.
Inspect the error column rather than treating a completed Spark action as proof
that every inference succeeded. Any retry must account for possible duplicate
provider work when the original request's outcome was uncertain.
Do not claim a throughput improvement without comparing the same workload,
provider latency, partition count, concurrency, errors and physical sends.

The opt-in `AIPromptBenchmarkSuite` compares the public chat paths using 4,096
synthetic rows, varying text lengths of 128-1,023 characters, and a recording
HTTP server with 5 ms response latency. One local Spark run measured:

| Partitions / concurrency per partition | `OpenAIPrompt` rows/s | `AIPrompt` rows/s |
| --- | ---: | ---: |
| 4 / 1 | 56.79 | 81.46 |
| 16 / 4 | 336.74 | 1,074.55 |

Both paths produced zero row errors and exactly 4,096 physical sends per
measurement. This measures local serialization/transport overhead, not provider
quota or remote-network performance. It is not a
production throughput guarantee. The new path constructs Spark row converters
once per partition, rather than resolving encoders for each request and response.
Reproduce with `SYNAPSEML_AI_BENCHMARK=true` and
`sbt "cognitive/testOnly com.microsoft.azure.synapse.ml.services.ai.AIPromptBenchmarkSuite"`.
