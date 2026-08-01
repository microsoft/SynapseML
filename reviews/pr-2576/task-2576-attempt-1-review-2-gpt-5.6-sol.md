## Review Summary
- **Round**: 2
- **Theme**: Architecture & patterns
- **Mode**: sequential
- **Model**: gpt-5.6-sol (highest reasoning setting)
- **Artifact**: C:\Users\singhrana\Documents\SynapseML-pr-2576-review\reviews\pr-2576\task-2576-attempt-1-review-2-gpt-5.6-sol.md
- **Issues Found**: 3
- **Verdict**: ISSUES_FOUND

The live scope is six tracked files (+489/-68) plus the untracked
`OpenAIPromptPostProcessing.scala` (53 lines). The generated prompt omitted that untracked source file, so it
was reviewed directly from the worktree.

## Evidence Checklist
- [x] Re-derived the live change set with `git diff refs/remotes/pr2576/base`, `git status --short`, and
  `git ls-files --others --exclude-standard`; no source file was accepted solely from the embedded prompt.
- [x] Read `.github/copilot-instructions.md` and applied the SynapseML Scala/Python/Py4J, code-generation,
  API-compatibility, serialization, security, and test-pattern checklists. No `AGENTS.md` exists in the
  worktree.
- [x] Audited the existing generated `OpenAIPrompt.py` with Python's AST: it parses, contains exactly one
  `setParams`, one `setPostProcessingOptions`, and no duplicate class methods. This proves the new hook works
  with today's template; Issue 3 concerns its unguarded future divergence.
- [x] Verified the stage's public parameter surface and persistence format remain structurally unchanged:
  `postProcessing`/`postProcessingOptions` names and types, both existing
  `setPostProcessingOptions` JVM signatures, `OpenAIPrompt extends ComplexParamsReadable`, defaults, and
  `MapParam.jsonEncode/jsonDecode` are unchanged. No generated `target/` file is in the diff.
- [x] Traced validation through public setters, raw Spark `Params.set`, `defaultCopy`, metadata loading, and
  `getParser`. The new consumption-time guard covers only “options present, mode empty”; a nonempty but
  inconsistent mode still bypasses the package-private validator and can select the wrong parser (Issue 1).
- [x] Traced the generated Py4J path from `setParams` and `setPostProcessingOptions` into
  `_validate_post_processing_options`. It constructs a complete logged `OpenAIPrompt`; `setParams` takes that
  path twice. Constructor logging, Fabric certified-event posting, and global-param registration therefore
  occur during validation (Issue 2).
- [x] `git diff --check refs/remotes/pr2576/base` passed. New Scala files have the required license header, no
  tabs/trailing whitespace, and remain within 800 lines/120 columns; the only 131-column line in
  `Wrappable.scala` is pre-existing inside its `scalastyle:off line.size.limit` region.
- [x] Security inspection found no secrets, unsafe deserialization, shell/SQL interpolation, new file/URL
  handling, or edits to authentication code. Regex and DDL values are parsed in the JVM; the incidental
  telemetry side effect of validation is recorded separately as Issue 2.
- [ ] Did not run sbt tests or scalastyle because the task permits modifying only this artifact and those
  commands write build/generated outputs under `target/`. A Black check was attempted, but neither the
  `synapseml` conda environment nor the `black` module is available in this shell; formatting was inspected
  statically instead.

## Issues

### Issue 1: Parser consumption does not enforce the validator's invariant
- **Severity**: Medium
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`
- **Line(s)**: 693-701 (also `OpenAIPromptPostProcessing.scala:19-50`)
- **Description**: `OpenAIPromptPostProcessing.inferMode` is invoked only by the public convenience setter.
  `getParser` re-checks only one invalid state: a nonempty options map with an empty mode. Other public Spark
  parameter paths can still create inconsistent state without invoking the validator. For example:
  `postProcessing = "csv"` plus `postProcessingOptions = Map("jsonSchema" -> "value STRING")`, written through
  public `Params.set`, `copy(extra)`, or metadata loading, reaches the `"csv"` branch. It silently constructs a
  comma `DelimiterParser` and ignores the JSON schema because the mode is nonempty, so line 700 never fires.
  Maps containing a valid primary key plus an unsupported key similarly evade the new exhaustive validation.
  The new raw-set test covers only the empty-mode case and therefore does not protect this invariant.
- **Risk**: Copied or deserialized stages can silently use the wrong parser and return incorrectly typed or
  incorrectly parsed output. This reopens the original silent-ignore failure mode through standard Spark ML
  lifecycle paths even though front-door Scala/Java/Python setters now validate correctly.
- **Suggested Fix**: Make the package-private helper the single consumption-time authority. Before selecting a
  parser, call `inferMode(opts)` and, when it returns a mode, require it to equal `getPostProcessing`; this also
  revalidates malformed/unknown values loaded without the setter. Keep the eager setters for good error
  locality, but add raw-set, `copy(extra)`, and persistence round-trip tests for mismatched and malformed maps.
  This can be done without changing public JVM signatures or the serialized parameter format.

### Issue 2: Python validation constructs logged service stages, twice for `setParams`
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptPythonOverrides.scala`
- **Line(s)**: 43-45, 79-81, 97-103
- **Description**: `_validate_post_processing_options` creates a full temporary `OpenAIPrompt` with
  `_new_java_obj` merely to call the Scala setter. A real `OpenAIPrompt` constructor calls
  `logClass` (`OpenAIPrompt.scala:49`), which schedules certified-event logging
  (`SynapseMLLogging.scala:129-150`) and posts on Fabric
  (`CertifiedEventClient.scala:24-33`). Its inherited field initializers also register multiple parameters in
  the process-global `GlobalParams.ParamToKeyMap`, which has no removal path. Moreover, custom `setParams`
  calls the validator on line 79 and then calls `setPostProcessingOptions` on line 81, whose generated setter
  calls the validator again on line 43. One logical `setParams` operation therefore constructs and logs two
  validation stages.
- **Risk**: A parameter-validation operation unexpectedly produces constructor logs and, on Fabric, duplicate
  background telemetry requests; it also retains registrations for throwaway validation UIDs. This pollutes
  usage data and adds avoidable driver/network work to routine Python configuration.
- **Suggested Fix**: Expose a Java/Py4J-friendly, non-mutating bridge around
  `OpenAIPromptPostProcessing.inferMode` (on a companion/helper or the existing stage) and call that directly
  from generated Python instead of constructing a transformer. Reuse the returned inferred mode when applying
  parameters so `setParams` validates exactly once. Keep the helper package-owned and avoid changing parameter
  names, existing setter signatures, or serialization.

### Issue 3: The custom `setParams` forks the complete codegen template without a drift guard
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptPythonOverrides.scala`
- **Line(s)**: 59-82 (also `OpenAIPrompt.scala:123-124`, `Wrappable.scala:306-321`)
- **Description**: The new protected `pySetParamsFunc` is a reasonable extension point, but
  `OpenAIPromptPythonOverrides.setParamsFunc` reconstructs the decorator, signature, docstring, keyword-capture
  logic, and default return path from `pyParamsArgs` instead of transforming `super.pySetParamsFunc`.
  `initFunc` and `postProcessingOptionsSetter` both derive from the generated default and fail loudly when
  their expected template no longer matches; `setParamsFunc` has no equivalent relationship or guard. The
  generated-source test checks only today's method count and marker strings.
- **Risk**: A future generic `setParams` change (keyword handling, conversion, documentation, compatibility
  logic, or decorator behavior) will reach every generated wrapper except `OpenAIPrompt`, while codegen and
  the current tests remain green.
- **Suggested Fix**: Pass `super.pySetParamsFunc` into the override helper and replace only the kwargs
  application block with an exact, one-occurrence, `require`-guarded transformation, mirroring the initializer
  and setter approach. Alternatively, narrow the core abstraction to a protected “apply captured kwargs”
  body hook so stages do not need to own the whole generated method. Add a codegen test that fails when the
  OpenAIPrompt override no longer incorporates the base implementation.

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: `getParser` now always calls `OpenAIPromptPostProcessing.inferMode` and requires any
  inferred mode to match the stored `postProcessing` value before selecting a parser. The test suite now covers
  missing modes, mismatched modes, malformed raw maps, and `copy(ParamMap)` bypasses.
- **Why**: Eager public-setter validation improves error locality, but the parser boundary must enforce the
  invariant for raw Spark parameter writes, copies, and deserialized metadata.
- **How verified**: `OpenAIPromptParamsSuite` passed 10/10, including
  `Raw and copied postProcessingOptions should enforce parser invariants`.

### Issue 2
- **Status**: Fixed
- **What changed**: Added package-private static JVM bridges on `OpenAIPromptPostProcessing`:
  `validateAndInferMode` performs non-mutating validation, and `applyPrevalidated` applies the already validated
  values. Generated Python no longer creates a temporary stage. Both the setter and `setParams` validate once,
  then reuse the inferred mode during application.
- **Why**: Parameter validation must not emit constructor telemetry, register throwaway global params, or add
  network/background work.
- **How verified**: Generated-source tests assert the static bridge is present and the old
  `_post_processing_validation` UID path is absent. `cognitive/codegen` and generated Python compilation
  passed; the direct Py4J suite passed 10/10 tests and 21/21 subtests.

### Issue 3
- **Status**: Fixed
- **What changed**: `OpenAIPrompt` now passes `super.pySetParamsFunc` into the override helper. The helper
  replaces only the captured-kwargs application block and uses a shared exact-one-occurrence guard also used
  by the initializer and setter transformations.
- **Why**: Generic decorator, signature, documentation, and keyword-capture changes must flow automatically
  into the OpenAIPrompt wrapper; template drift must fail code generation loudly.
- **How verified**: The generated wrapper contains exactly one `setParams` and one validated setter. A dedicated
  Scala test passes a drifted template and pins the expected guard failure. The targeted Scala suite passed
  10/10.

## Rerun 1 findings

### Issue 4
- **Severity**: Medium
- **Confidence**: High
- **Location**:
  `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptPythonOverrides.scala`,
  generated setter and `setParams` application path
- **Finding**: The generated Python path eagerly mutated `_java_obj` after validation. Clearing the Python
  parameters before the next transfer left the JVM object with stale post-processing values, so later
  transforms or saves could continue using cleared options.
- **Status**: Fixed
- **What changed**: The static JVM bridge is now validation-only. Generated Python applies the validated options
  and inferred mode exclusively through Python `_set`, allowing normal Java parameter transfer to synchronize
  the stage. The mutating `applyPrevalidated` bridge was removed.
- **Why**: Validation must execute on the JVM, but the Python wrapper must retain the standard single source of
  truth for parameter mutation so `clear()` behaves like other generated parameters.
- **How verified**: Added a Py4J regression that sets options, clears both inferred parameters before transfer,
  and verifies the JVM retains its empty defaults. The direct Py4J suite passed 12 tests and 21 subtests.

### Issue 5
- **Severity**: Medium
- **Confidence**: High
- **Location**:
  `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`,
  parser selection
- **Finding**: Requiring the stored mode to equal the inferred mode rejected legacy persisted Python stages
  that contained valid options but retained the default empty `postProcessing` value.
- **Status**: Fixed
- **What changed**: Parser selection treats an explicitly stored empty mode as legacy-unset and uses the
  validated inferred mode. A genuinely unset mode with nonempty options is rejected, covering raw writes,
  copies, and Scala/Java `clear`. Non-empty conflicts remain rejected.
- **Why**: Older generated Python wrappers could persist options without setting the inferred mode. Those valid
  models must remain loadable while malformed options and real explicit conflicts still fail.
- **How verified**: Scala tests cover rejection of raw and copied options-only stages, explicit-empty legacy
  inference, and non-empty conflicts. A Python/Py4J persistence regression saves and loads an options-only
  stage, then verifies its inferred parser output. The final targeted suites passed 11/11 Scala tests and
  13 Python tests with 23 subtests.

## Rerun 2 finding

### Issue 6
- **Severity**: Medium
- **Confidence**: High
- **Location**:
  `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` and
  `OpenAIPromptPostProcessing.scala`
- **Finding**: Treating every empty stored mode as legacy-unset can change a raw/copied stage that deliberately
  paired `postProcessing=""` with nonempty options from pass-through to inferred processing.
- **Status**: Resolved with compatibility boundary documented and public paths hardened
- **What changed**: Supported Python constructor, setter, and `setParams` paths now distinguish `None` from an
  explicitly supplied empty string. Nonempty options plus explicit `postProcessing=""` fail immediately,
  matching the existing Scala public setter. At parser consumption, an empty stored value is still treated as
  legacy-unset so previously persisted options-only stages remain loadable.
- **Why**: SynapseML persistence writes default values into `paramMap`. Direct inspection showed an options-only
  legacy stage reloads with `postProcessing=""` marked explicitly set, making it byte-for-byte
  indistinguishable from a stage created through raw/private parameter mutation with an intentional empty
  value. Preserving both meanings would require a new persisted marker, changing serialization. The safe,
  compatible boundary is therefore to reject the ambiguous combination on supported public setters and retain
  legacy inference for deserialized/raw state. Private Python `_set`, raw Scala `Params.set`, and crafted
  `ParamMap` values remain the already documented bypasses.
- **How verified**: The captured metadata contains both
  `"postProcessingOptions":{"delimiter":";"}` and `"postProcessing":""` in `paramMap` even though
  `postProcessing` was unset before save. Scala tests cover raw/copied legacy inference and nonempty conflicts.
  Python tests cover explicit-empty constructor rejection plus options-only save/load inference. Scala passed
  10/10; direct Py4J passed 12 tests and 21 subtests; generated Python compilation and Black 22.3.0 passed.

## Rebase note

The PR head was externally rebased from `29731b15848954efd58201b5879601cacc0d6bdb` to
`5d076f98c1b873fa7b37c7c251b1eef57476b76b`. The driving agent created a replacement isolated worktree at the
new head and reapplied the review fixes without rewriting or force-pushing remote history. The touched source
blobs in the two PR-head commits were identical before applying review fixes.

## Final rerun finding

### Issue 7
- **Severity**: Medium
- **Confidence**: High
- **Location**:
  `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala` and generated
  Python mode setter/clear paths
- **Finding**: Validation was order-dependent. After options inferred CSV, `setPostProcessing("")`,
  `setPostProcessing("json")`, `setParams(postProcessing=...)`, or `clear(postProcessing)` could leave a mode
  value that disagreed with the options while parser consumption silently re-inferred CSV.
- **Status**: Fixed
- **What changed**: Scala `setPostProcessing` now validates against existing options. Generated Python
  `setPostProcessing` and the no-options `setParams` branch validate through non-mutating JVM helpers before
  mutation, including loaded stages whose map value is represented as a `JavaObject`. Python
  `clear(postProcessing)` atomically clears `postProcessingOptions`. Spark's Scala/Java `Params.clear` is final
  and cannot be overridden, so parser consumption rejects the resulting genuinely-unset-mode/nonempty-options
  state instead of silently inferring. Persisted legacy stages remain distinguishable there because the
  existing writer reloads their default empty mode as explicitly set.
- **Why**: Both parameter-order directions must enforce the same invariant. A clear operation must either
  remove the dependent options or produce a loud error, never preserve options while silently ignoring the
  cleared mode.
- **How verified**: Scala tests cover reverse-order empty/nonmatching setters, matching setters, final
  Scala/Java clear followed by parser rejection, raw/copy missing-mode rejection, and generated-source
  uniqueness. Python tests cover setter and `setParams` rejection, atomic clear, loaded-stage Java-map
  validation, constructor conflicts, and persistence. Final results: Scala 11/11; Py4J 13 tests and 23
  subtests; codegen, generated Python compilation, and Black 22.3.0 passed.

## Subsequent rerun findings

### Issue 8
- **Severity**: Medium
- **Confidence**: High
- **Location**:
  `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptPythonOverrides.scala`,
  generated `setParams`
- **Finding**: Although post-processing validation ran first, PySpark `_set(**kwargs)` converted and mutated
  parameters one at a time. A later conversion error could therefore leave an earlier mode change applied.
- **Status**: Fixed
- **What changed**: Generated `OpenAIPrompt.setParams` now converts every supplied value into a temporary
  `Param -> value` map using PySpark's existing conversion/error semantics, and updates `_paramMap` only after
  all conversions succeed. Validated options and inferred mode are included in the same atomic update.
- **Why**: Validation-first is insufficient if generic conversion can still fail after partial mutation.
- **How verified**: A Py4J regression starts with CSV mode, calls
  `setParams(postProcessing="json", concurrency="not-an-integer")`, and verifies both mode and concurrency
  remain unchanged after `TypeError`. The final Py4J suite passed 15 tests and 25 subtests.

### Issue 9
- **Severity**: Medium
- **Confidence**: High
- **Location**:
  `OpenAIPromptPostProcessing.scala` and `OpenAIPrompt.getParser`
- **Finding**: Empty options combined with `json` or `regex` mode reached direct map indexing in `getParser`,
  producing `NoSuchElementException` rather than a stable validation error.
- **Status**: Fixed
- **What changed**: Central mode-requirement validation now requires `jsonSchema` for JSON and both `regex` and
  `regexGroup` for regex. Options setters and combined Python `setParams` calls reject empty invalid
  combinations immediately; parser consumption repeats the validation for raw/copy/load safety. Empty maps
  remain compatible with pass-through and CSV/default-delimiter modes.
- **Why**: Accepted empty maps must not permit structurally incomplete parser modes.
- **How verified**: Scala/Java and Python tests cover constructor, setter, `setParams`, and parser-boundary
  errors with pinned messages. Scala passed 12/12 and Py4J passed 15 tests plus 25 subtests.

### Issue 10
- **Severity**: Medium
- **Confidence**: High
- **Location**:
  `OpenAIPromptPythonOverrides.scala`, JVM-map normalization
- **Finding**: Loaded map parameters are exposed to Python as `JavaObject`s, so passing a getter result back to
  `setPostProcessingOptions` failed the mapping-only conversion.
- **Status**: Fixed
- **What changed**: Generated Python normalizes both Scala collection maps and Java `Map` objects into a
  validated Python dictionary before JVM conversion or Python parameter storage. Legacy loaded options with an
  explicitly persisted empty mode are treated as mode-unset only for this getter-to-setter round trip.
- **Why**: Getter output must remain a valid setter input across persistence boundaries.
- **How verified**: The persistence regression loads an options-only stage, passes
  `getPostProcessingOptions()` directly to the setter, and verifies the normalized dictionary and inferred
  CSV mode. The final Py4J suite passed 15 tests and 25 subtests.

### Issue 11
- **Severity**: Medium
- **Confidence**: High
- **Location**:
  `OpenAIPromptPythonOverrides.scala`, legacy-empty handling for JVM-backed option maps
- **Finding**: The initial JVM-map compatibility path treated every current empty mode as legacy-unset. An
  explicitly supplied `postProcessing=""` could therefore be replaced by inferred processing when options
  came from a loaded Java/Scala map.
- **Status**: Fixed
- **What changed**: Generated Python now tracks a transient `_post_processing_explicitly_set` provenance flag.
  Public `setPostProcessing`, explicit `setParams(postProcessing=...)`, and constructor mode arguments set the
  flag only after successful mutation. Deserialization does not, because `_from_java` transfers parameters
  through `_set`. Empty mode is treated as legacy-unset only when this flag is false. Clearing the mode resets
  the flag while atomically clearing options.
- **Why**: Existing serialization cannot distinguish default-empty from explicit-empty, but the Python wrapper
  can distinguish public runtime intent from deserialized state without changing JVM signatures or metadata.
- **How verified**: The persistence suite verifies that an untouched loaded options-only stage can round-trip
  its JVM map and infer CSV, while explicit-empty setter, constructor, and `setParams` calls using that same
  JVM map all fail with the pinned CSV conflict. Scala generated-source tests pin the provenance logic.
  Results remained 12/12 Scala and 15 Python tests plus 25 subtests.

### Issue 12
- **Severity**: Medium
- **Confidence**: High
- **Location**: `OpenAIPrompt.scala`, parser visibility
- **Finding**: `getParser` had been widened from JVM-private to package-private solely so the parameter suite
  could call it, emitting a public bytecode method and unnecessarily changing the class surface.
- **Status**: Fixed
- **What changed**: Restored `private def getParser`. The Scala suite now extends `TestBase` and verifies
  parser-boundary behavior through the existing public `transformSchema` API.
- **Why**: Testability must not widen an implementation method or create a compatibility liability.
- **How verified**: Targeted Scala tests passed 12/12 with a local Spark session and codegen completed.

### Issue 13
- **Severity**: Medium
- **Confidence**: High
- **Location**: `OpenAIPromptPostProcessing.scala`, JSON schema validation
- **Finding**: DDL syntax validation accepted atomic schemas such as `STRING`, which Spark `from_json` cannot
  use as its output schema.
- **Status**: Fixed
- **What changed**: JSON schema validation now accepts only `StructType`, `ArrayType`, or `MapType` with string
  keys, matching Spark JSON parsing's supported schema shapes. Other parsed DDL types retain the stable
  `Invalid jsonSchema` error.
- **Why**: Configuration-time validation must enforce parser semantics, not only DDL grammar.
- **How verified**: Scala/Java and Python/Py4J tests reject `STRING` and `MAP<INT, STRING>` in addition to
  malformed DDL. Results: Scala 12/12; Py4J 15 tests and 27 subtests; generated wrapper compilation and Black
  passed.

### Issue 14
- **Severity**: Medium
- **Confidence**: High
- **Location**: `OpenAIPrompt.write` and post-processing state validation
- **Finding**: Scala/Java `clear(postProcessing)` is final and leaves options set. Saving that state serialized
  the default empty mode as explicit, so loading could re-enable inferred processing.
- **Status**: Fixed
- **What changed**: `OpenAIPrompt.write` now validates the same effective post-processing invariant used by
  parser consumption before returning the unchanged `ComplexParamsWriter`. Invalid unset-mode/nonempty-options
  states cannot be persisted. Existing legacy metadata with explicitly stored empty mode remains readable.
- **Why**: Validation must occur before serialization loses the distinction between unset and default-empty.
- **How verified**: A Scala persistence test clears mode after setting delimiter options and pins save-time
  rejection. The Python compatibility test writes a valid stage, rewrites only its temporary metadata to the
  historical empty-mode shape, and verifies load/inference. Scala passed 13/13; Py4J passed 15 tests and 29
  subtests.

### Issue 15
- **Severity**: Medium
- **Confidence**: High
- **Location**: Recursive JSON schema validation
- **Finding**: Root-shape checks still accepted nested maps with non-string keys, which Spark JSON parsing
  rejects.
- **Status**: Fixed
- **What changed**: JSON schema validation recursively traverses structs, arrays, and map values and rejects
  any `MapType` whose key is not `StringType`.
- **How verified**: Scala/Java and Python reject `STRUCT<x: MAP<INT, STRING>>` with `Invalid jsonSchema`.

### Issue 16
- **Severity**: Medium
- **Confidence**: High
- **Location**: Delimiter option validation
- **Finding**: Delimiters are Spark regular expressions, but invalid patterns were accepted until `split`
  analysis/execution.
- **Status**: Fixed
- **What changed**: `inferMode` compiles delimiter patterns eagerly with `java.util.regex.Pattern` and raises
  `Invalid delimiter` on failure.
- **How verified**: Scala/Java and Python reject delimiter `[` at configuration time. Final targeted results:
  Scala 13/13; Py4J 15 tests and 29 subtests; codegen and generated compilation passed; Black 22.3.0 formatted
  and passed the updated Python test.

### Issue 17
- **Severity**: Medium
- **Confidence**: High
- **Location**: JVM legacy load-to-setter behavior
- **Finding**: A legacy JVM stage reloads with empty mode marked set, so reapplying valid options hit the normal
  explicit-mode conflict guard.
- **Status**: Fixed
- **What changed**: `OpenAIPrompt` now tracks a transient JVM `postProcessingExplicitlySet` flag. The public
  mode setter sets it; deserialization and inferred raw parameter writes do not. Options setters treat
  empty-mode/false-provenance as legacy-unset and infer the mode, while public explicit empty remains a
  conflict. No flag is serialized.
- **Why**: This mirrors the Python provenance solution without changing parameter metadata or public
  signatures.
- **How verified**: A Scala persistence test writes the historical explicit-empty metadata shape, loads it
  twice, and verifies both Scala-map and Java-HashMap setters infer CSV and update options. Scala passed 14/14.

### Issue 18
- **Severity**: Medium
- **Confidence**: High
- **Location**: `OpenAIPrompt.write`
- **Finding**: Validating when `write` was obtained allowed mutation after writer creation but before `save`.
- **Status**: Fixed
- **What changed**: `write` now returns a delegating `MLWriter` that validates inside `saveImpl`, propagates
  Spark session, options, and overwrite state, then invokes the unchanged `ComplexParamsWriter`.
- **Why**: The invariant must be checked at the serialization commit point.
- **How verified**: The save regression captures a writer before clearing mode, mutates the stage, and verifies
  `writer.save` rejects the invalid state. Scala passed 14/14; the Python/Py4J persistence suite passed 15
  tests and 29 subtests.

### Issue 19
- **Severity**: Medium
- **Confidence**: High
- **Location**: `OpenAIPrompt.copy`
- **Finding**: `defaultCopy` copied Spark parameters but lost the transient JVM explicit-mode provenance flag.
- **Status**: Fixed
- **What changed**: The existing `copy(ParamMap)` override still delegates parameter copying to
  `defaultCopy`, then transfers provenance. A mode supplied in `extra` is treated as explicit.
- **Why**: Copying must preserve the same conflict semantics as the source without serializing the transient
  flag.
- **How verified**: A Scala regression copies an explicitly empty-mode prompt and verifies delimiter options
  are still rejected. Scala passed 15/15.

### Issue 20
- **Severity**: Medium
- **Confidence**: High
- **Location**: Recursive JSON data-type validation
- **Finding**: Nested Spark `CHAR`/`VARCHAR` types passed the map-key recursion but are rejected by
  `from_json`.
- **Status**: Fixed
- **What changed**: Recursive validation now rejects `CharType` and `VarcharType` at any depth.
- **How verified**: Scala/Java and Python reject `STRUCT<x: VARCHAR(10)>`. Final targeted results: Scala 15/15;
  Py4J 15 tests and 30 subtests; codegen, generated compilation, and Black passed.

### Issue 21
- **Severity**: Medium
- **Confidence**: High
- **Location**: Delegating `MLWriter` overwrite timing
- **Finding**: Spark processes overwrite deletion before `saveImpl`, so validation there could remove an
  existing destination before rejecting the new invalid stage.
- **Status**: Fixed
- **What changed**: The delegating writer now overrides `save` and validates before invoking Spark's base
  `MLWriter.save`; `saveImpl` only configures and invokes the unchanged delegate.
- **Why**: Validation must precede all destructive overwrite handling while still observing mutations made
  after writer creation.
- **How verified**: The regression saves a valid stage, captures an overwrite writer for another stage, clears
  its mode, verifies save rejection, then reloads the original destination and confirms its CSV options are
  intact. Scala passed 15/15; Py4J passed 15 tests and 30 subtests.

### Issue 22
- **Severity**: Medium
- **Confidence**: High
- **Location**: Generated Python `copy`
- **Finding**: PySpark `JavaParams.copy` applies extras through `_set`, so an explicit empty mode supplied in
  `extra` did not update the transient Python provenance flag.
- **Status**: Fixed
- **What changed**: Generated `OpenAIPrompt.copy` delegates to the standard JavaParams implementation, then
  preserves source provenance and marks `postProcessing` explicit when present in `extra`.
- **Why**: Copy extras must have the same conflict semantics as public setters without replacing normal Java
  component copying.
- **How verified**: A Py4J regression copies a prompt with explicit empty mode in `extra` and verifies delimiter
  options are rejected while state remains unchanged. Results: Scala 15/15; Py4J 16 tests and 30 subtests.

### Issue 23
- **Severity**: Medium
- **Confidence**: High
- **Location**: Scala and generated Python `copy(extra)` validation
- **Finding**: Provenance was recorded on the copy but an explicitly conflicting mode extra was not rejected
  during the copy operation; later consumption could still interpret empty as legacy.
- **Status**: Fixed
- **What changed**: Scala validates an explicit copied mode against copied options before returning. Generated
  Python performs the same non-mutating mode validation after the standard JavaParams copy and provenance
  update. JVM consumption additionally rejects explicit-empty mode when options imply a mode, while
  deserialized/raw legacy state retains false provenance.
- **Why**: `copy(extra)` is an atomic configuration operation and must reject conflicts immediately.
- **How verified**: Scala and Python regressions copy a CSV-configured prompt with explicit empty mode and pin
  immediate rejection with unchanged source state. Separate raw legacy-copy coverage still infers CSV.
  Results remain Scala 15/15 and Py4J 16 tests plus 30 subtests.

### Issue 24
- **Severity**: Medium
- **Confidence**: High
- **Location**: Scala and generated Python options-only `copy(extra)`
- **Finding**: Copy validation ran only when a mode extra was supplied. Options-only extras bypassed inference,
  conflict validation, and malformed-value validation.
- **Status**: Fixed
- **What changed**: When `postProcessingOptions` appears in copy extras, Scala and generated Python reapply the
  copied value through the validated public options setter. Options-only copies infer mode; combined conflicts
  and malformed options fail before the copy is returned. Mode-only extras retain their dedicated validation.
- **Why**: Spark transform-with-ParamMap and ordinary copy extras must have the same behavior as setters.
- **How verified**: Scala and Python tests cover options-only delimiter inference, malformed delimiter rejection,
  and combined conflicting extras. Results: Scala 15/15; Py4J 16 tests and 30 subtests; Black 22.3.0 formatted
  and passed the expanded test file.

### Issue 25
- **Severity**: Medium
- **Confidence**: High
- **Location**: `OpenAIPrompt.scala`, file length
- **Finding**: The accumulated changes pushed the file beyond the enforced 800-line scalastyle limit.
- **Status**: Fixed
- **What changed**: Existing `OutputParser`, pass-through, delimiter, JSON, and regex parser types were moved
  unchanged into `OpenAIPromptParsers.scala`. Validation helpers were split to satisfy complexity limits and
  the generated-method template remains a field to satisfy method-length and naming rules.
- **Why**: CI enforces file length, complexity, method length, and member naming.
- **How verified**: `OpenAIPrompt.scala` is exactly 800 lines. Cognitive main and test scalastyle both report
  zero errors. Scala tests passed 15/15, codegen passed, and the regenerated Py4J suite passed 16 tests and 30
  subtests.

## Final clean rerun

**CLEAN**

GPT-5.6 Sol independently rechecked the final live base-to-working-tree diff and found no remaining concrete
issues. The clean gate covered API and persistence compatibility, Spark parameter lifecycle, Py4J and codegen,
parser validation, serialization timing, scalastyle, and the recorded test evidence.
