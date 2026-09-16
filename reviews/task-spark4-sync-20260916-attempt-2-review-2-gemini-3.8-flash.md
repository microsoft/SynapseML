# Spark 4 CI repair review: attempt 2, round 2

## Review summary
- Attempt: 2; round: 2; theme: Architecture & Patterns; mode: sequential.
- Model: `gemini-3.8-flash`; reasoning: standard; reviewed directly without agents.
- Verdict: **CLEAN**. 0 issues found.
- Scope: four uncommitted CI-repair files per port across Spark 4.0 (#2718) and Spark 4.1 (#2720).
- Worktrees: `sync-spark40-20260916` (HEAD `e2ea243ce83fa4a8d529d6ede7bcaa04529dfb24`), `sync-spark41-20260916` (HEAD `1c5f8921a7e6ebfd29dc49b24646877f1b3cc605`).
- Artifact: `reviews\task-spark4-sync-20260916-attempt-2-review-2-gemini-3.8-flash.md`.

## Source snapshot
Working file SHA256 hashes verified prior to review:

| ID | File relative to worktree root | SHA256 (Spark 4.0) | SHA256 (Spark 4.1) |
| --- | --- | --- | --- |
| J | `core\src\main\scala\com\microsoft\azure\synapse\ml\core\utils\JarLoadingUtils.scala` | `5a1f1de29b966c7e95b94cb351df1bb53d86e4e5d4e0e1743b432cc90e36ca86` | `5a1f1de29b966c7e95b94cb351df1bb53d86e4e5d4e0e1743b432cc90e36ca86` |
| D | `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\CodegenDiscoverySuite.scala` | `0c1d309b089013d6c19621ba2abf864fe78ffcc318299e54dc5ce9286625e84d` | `0c1d309b089013d6c19621ba2abf864fe78ffcc318299e54dc5ce9286625e84d` |
| P | `core\src\test\scala\com\microsoft\azure\synapse\ml\codegen\PyCodegenSuite.scala` | `2c73e4f97aac6064aad0db7921a6fdc23b2ac9746af1ee79ff3bfb42d63631d3` | `5fe89b55c0dfad07c89eeeea337b9f08fb362ba66d3386f1c4983864fe92aab0` |
| O | `cognitive\src\test\python\synapsemltest\services\openai\test_OpenAIResponseSchema.py` | `675c70fa2f6049b7682abffa4ecba54f414526809447bc2697ea91e6b4497b7d` | `675c70fa2f6049b7682abffa4ecba54f414526809447bc2697ea91e6b4497b7d` |

J, D, and O are byte-identical across both ports. P diff is identical across ports (updating the nesting comment at line 20); base file differences account for P hash variation.

## Evidence checklist
- [x] **Repository conventions & AGENTS.md**: No edits to `target/`, no secret leaks, no RDD implementations, and no modifications to build pipelines or shared guides. Scalastyle reported 0 errors and 0 warnings across all modules in both ports. Pinned Black 22.3.0 passed on changed Python code.
- [x] **Dependency direction & encapsulation**: `JarLoadingUtils.matchesJar` encapsulates URL/archive matching cleanly as `private[ml]`, depending strictly on JDK standard networking and reflection APIs. Subprocess isolation in `CodegenDiscoverySuite` prevents classloader pollution across test suites.
- [x] **Abstraction & typing**: Protocol checking cleanly decouples `jar:` archive handling (`JarURLConnection.getJarFileURL` with decoded URI path) from `file:` exploded directory matching (`classes` vs `test-classes`). Encoded path characters (`%21`, spaces) and classifier-preserving snapshot aliases are properly handled.
- [x] **Public JVM signature compatibility**: Public methods `instantiateServices[T: ClassTag](Class[_] => Any, Option[String])`, `instantiateServices[T: ClassTag](Option[String] = None)`, and `instantiateObjects[T: ClassTag](Option[String] = None)` retain identical binary signatures, default arguments, and constructor exception unwrapping (`InvocationTargetException => throw e.getCause`).
- [x] **Production/test class-discovery separation**: Fix correctly addresses CI 236207038 at the artifact/classloader boundary rather than using anti-patterns (no dummy constructors, no blanket catch, no fixture blacklists). Multi-JVM regression `CodegenDiscoverySuite` proves production codegen excludes test-JAR fixtures while explicit test discovery and unscoped discovery remain functional.
- [x] **Public/private Python wrapper contract**: `test_OpenAIResponseSchema.py` respects the hand-written extension architecture for `OpenAIPrompt` (inheriting generated `_OpenAIPrompt`) while verifying `issubclass`, checking `_OpenAIPrompt.pyi` stubs, and asserting all stub methods are present on the public `OpenAIPrompt` class. Direct generated stages (`OpenAIChatCompletion`, `OpenAIResponses`) continue testing their public stubs.

## Architectural findings
**CLEAN**. No architectural violations, abstraction leaks, circular dependencies, or API signature breaks found. The round 1 URL classification defect is verified resolved by `JarURLConnection.getJarFileURL` and covered by two compiled regressions passing on both ports.

## Validation limitations
Review is based on direct static inspection of code diffs, AST contracts, and review evidence artifacts:
- 29/29 tests across 4 suites passed per port (`CodegenDiscoverySuite`, `PyCodegenSuite`, `RCodegenSuite`, `VerifyJarLoadingUtils`), including packaging of main and test JARs (`spark40-url-green.log`, `spark41-url-green.log`).
- Python stub regression passed on PySpark 4.0.1 / Python 3.12.3 and PySpark 4.1.1 / Python 3.13.13 (`spark40-openai-stub-green.log`, `spark41-openai-stub-green.log`).
- No full end-to-end cloud pipeline, remote package publication, or live Azure OpenAI service calls were executed in this local review.
