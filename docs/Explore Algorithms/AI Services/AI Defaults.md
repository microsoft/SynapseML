---
title: AI Defaults
hide_title: true
status: stable
---

# Shared AI defaults

`AIDefaults` is the unified entry point for configuring AI services. Select an API
scope instead of sharing one endpoint, key, and model across unrelated APIs.
The `openai` scope shares the existing `OpenAIDefaults` store: writes and resets
through either interface are immediately visible through the other. Existing
`OpenAIDefaults` imports, methods, and consumers continue to work.
API-specific overrides do not modify that shared store: the legacy interface
cannot represent separate Responses, embeddings, or Content Understanding settings.

```python
from synapse.ml.services.ai import AIPrompt
from synapse.ml.services.ai.AIDefaults import AIDefaults

defaults = AIDefaults()
responses = defaults.for_api("responses")
responses.set_endpoint("https://your-resource.openai.azure.com/openai/v1")
responses.set_api_key(api_key)  # Obtain from your secret provider.
responses.set_model("your-responses-deployment")

embeddings = defaults.for_api("embeddings")
embeddings.set_endpoint("https://your-other-resource.openai.azure.com/openai/v1")
embeddings.set_api_key(embedding_api_key)
embeddings.set_model("your-embedding-deployment")

prompt = AIPrompt(
    apiType="responses", inputCol="text", outputCol="answer", errorCol="error"
)
result = prompt.transform(df)
```

In Scala, use
`com.microsoft.azure.synapse.ml.services.ai.AIDefaults.forApi("responses")`
with `setEndpoint`, `setApiKey`, and `setModel`.

When OpenAI-compatible APIs share a resource, configure the common endpoint and
key once and override only their models:

```python
defaults.for_api("openai").set_endpoint(shared_endpoint).set_api_key(shared_key)
defaults.for_api("responses").set_model("responses-deployment")
defaults.for_api("embeddings").set_model("embedding-deployment")
```

A scope with its own endpoint must supply its own credentials instead of
inheriting the shared key. Native APIs and Content Understanding remain isolated.

## Scopes and fields

Only the fields listed for a scope are accepted. Unknown scopes, unsupported
fields, blank strings, invalid endpoints, and invalid numeric ranges fail early.
These are scalar defaults; use stage column setters for row-dependent values.

| Scope | Fields (Scala names; Python uses snake_case) |
| --- | --- |
| `openai` | `endpoint`, `apiKey`, `model`, `deploymentName`, `apiVersion`, `apiType`, `embeddingDeploymentName`, `temperature`, `topP`, `seed`, `verbosity`, `reasoningEffort` |
| `chat_completions` | `endpoint`, `apiKey`, `model`, `deploymentName`, `apiVersion`, `temperature`, `topP`, `seed`, `verbosity`, `reasoningEffort` |
| `responses` | Same as `chat_completions`, except `seed` |
| `embeddings` | `endpoint`, `apiKey`, `model`, `deploymentName`, `apiVersion` |
| `systemone` | `endpoint`, `apiKey`, `model` |
| `decisions` | `endpoint`, `apiKey`, `model`, `apiVersion` |
| `content_understanding` | `endpoint`, `apiKey`, `apiVersion`, `analyzerId`, `modelDeployments`, `stringEncoding`, `processingLocation`, `allowInputTruncation` |

`model` selects the request model; `deploymentName` selects an Azure deployment
binding. For v1 endpoints the configured model normally is the deployment name.
Do not set both in an API-specific scope: reset the previous binding before
switching. The legacy `openai` scope retains its existing behavior when both are
present. `embeddingDeploymentName` is the legacy shared embeddings fallback.

`apiType` in `openai` accepts `chat_completions` or `responses`, as before.
Selecting a scope does not change a stage's API type. `apiVersion` is considered
only where that API's endpoint uses a version; unrelated dated versions are not
injected into v1 routes. Service-specific model and option support is still
validated by the service.

## Public signatures

Python:

```text
AIDefaults()
AIDefaults.for_api(api: str) -> AIDefaultsScope
AIDefaults.reset_all() -> None

# On the returned scope, for each supported field:
scope.set_endpoint(value: str) -> AIDefaultsScope
scope.get_endpoint() -> Optional[str]
scope.reset_endpoint() -> None
scope.reset() -> None
```

Every listed field has the corresponding `set_<field>`, `get_<field>`, and
`reset_<field>` methods. Strings use `str` / `Optional[str]`; `temperature` and
`top_p` use `float` / `Optional[float]`; `seed` uses `int` / `Optional[int]`;
`allow_input_truncation` uses `bool` / `Optional[bool]`; `model_deployments` uses
`Dict[str, str]` / `Optional[Dict[str, str]]`. Setters return the same scope for
chaining. An unset getter returns `None`; an explicitly empty map or `False`
remains distinguishable from unset. Use reset methods, not `None`, to clear values.
Getters report values stored in that scope, not a stage's effective inherited settings.

Scala:

```text
AIDefaults.forApi(api: String): AIDefaultsScope
AIDefaults.resetAll(): Unit

// For each supported field, using its PascalCase suffix:
scope.setEndpoint(value: String): scope.type
scope.getEndpoint: Option[String]
scope.resetEndpoint(): Unit
scope.reset(): Unit
```

Scala field types are `String`, `Double`, `Int`, `Boolean`, and
`Map[String, String]`, respectively. `setModelDeployments` additionally accepts
`java.util.Map[String, String]` for JVM interoperability.

## Resolution, lifetime, and credentials

For `AIPrompt`, `AISystemOne`, and `OpenAIDecisions`, resolution occurs on an
effective copy during schema validation and transformation:

1. Explicit instance parameters, including column bindings, win.
2. Defaults in the selected API scope apply.
3. Compatible legacy `openai` defaults fill remaining unset values.
4. Existing stage and Fabric runtime defaults remain the fallback.

Native `systemone` and `decisions` never inherit the legacy OpenAI endpoint, key,
or model. API-specific scopes do not affect legacy `OpenAIPrompt`,
`OpenAIChatCompletion`, or `OpenAIEmbedding`; use `openai` for those classes.

API-specific keys require an endpoint in the same scope. An API-specific
destination does not inherit the legacy OpenAI key. Overriding a stage's
destination to another origin (scheme, host, or port) withholds the scoped key;
configure credentials explicitly on that stage. Explicit instance authentication
also takes precedence. Endpoint defaults must use HTTPS, except literal loopback
HTTP addresses for local tests. Defaults do not acquire, refresh, or store a
Fabric workload token. Without an override, existing managed Fabric endpoint and
identity discovery continue to work.

Defaults are shared across Spark sessions in the same **driver JVM**, not
process-wide across Python workers, cluster-wide, or thread-local. Set them
before constructing Spark actions; changing defaults during concurrent planning
is not a supported way to isolate tenants. Already planned DataFrames retain
their resolved configuration. Use explicit stages for concurrently different
credentials.

Resolving an `AIPrompt` does not mutate its original parameters. Saving an
unresolved stage does not save ambient defaults or their keys; a loaded stage
uses the defaults of its current JVM. Explicitly configured stage credentials
retain their normal Spark ML persistence behavior. Do not print key getters.

## Foundry Content Understanding

Content Understanding has its own scope; it never inherits OpenAI defaults.
Because its transformer ships separately, apply the shared configuration
explicitly to a copy using `apply_to` (Python) or `applyTo` (Scala):

```python
from synapse.ml.services.contentunderstanding import ContentUnderstanding

content = defaults.for_api("content_understanding")
content.set_endpoint("https://your-foundry-resource.services.ai.azure.com")
content.set_api_key(content_understanding_key)
content.set_api_version("2026-06-01-preview")
content.set_analyzer_id("prebuilt-invoice")
content.set_model_deployments({"completion": "your-deployment"})

stage = content.apply_to(
    ContentUnderstanding()
    .setDocumentUrlCol("document_url")
    .setOutputCol("analysis")
    .setErrorCol("error")
)
```

Signatures:

```text
scope.apply_to(stage: T) -> T  # T is a ContentUnderstanding stage.
```

```text
scope.applyTo[T <: CognitiveServicesBaseNoHandler with HasCognitiveServiceInput](stage: T): T
```

Only the `content_understanding` scope and a Content Understanding stage are
accepted. The returned stage has an independent parameter copy; explicit
instance settings win. It snapshots the defaults, including credentials:
saving that copy persists them just like calling the stage's own setters.
Later defaults changes do not change the returned copy.

`modelDeployments` is a per-request mapping from service model names or prebuilt
aliases to Foundry deployment names. The defaults API neither provisions
analyzers nor changes Foundry resource defaults. Preview-only options retain
Content Understanding's normal API-version checks.

For managed Fabric Content Understanding, leave the Foundry endpoint/key unset.
The transformer retains its own runtime endpoint and identity behavior.

To clear settings:

```python
responses.reset_model()  # One field.
responses.reset()        # One API scope.
defaults.reset_all()     # All AI scopes, including legacy OpenAIDefaults.
```
