# Code Review — PR microsoft/SynapseML#2576 — Round 1 of 6 (sequential, DIRECT)

## Review Summary
- **Round**: 1
- **Theme**: Broad sweep
- **Mode**: sequential
- **Model**: Claude Opus 5 (Slot 1, highest reasoning effort)
- **Artifact**: C:\Users\singhrana\Documents\SynapseML-pr-2576-review\reviews\pr-2576\task-2576-attempt-1-review-1-claude-opus-5.md
- **Issues Found**: 10
- **Verdict**: ISSUES_FOUND

Scope reviewed: base `refs/remotes/pr2576/base` (master snapshot `8d528f3c`) → working HEAD `29731b15`,
4 files, +298/-1:
`cognitive/src/main/scala/.../openai/OpenAIPrompt.scala`,
`cognitive/src/main/scala/.../openai/OpenAIPromptPythonOverrides.scala` (new),
`cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIPromptParams.py` (new),
`cognitive/src/test/scala/.../openai/OpenAIPromptParamsSuite.scala` (new).

---

## Evidence Checklist

- [x] **Confirmed the exact base→HEAD change set.** `git --no-pager diff refs/remotes/pr2576/base..HEAD --stat`
  returns exactly the 4 files above (298 insertions, 1 deletion) — no generated `target/` files are in the diff,
  and no other source was touched.
- [x] **Verified the Java `HashMap` setter now routes through Scala validation.**
  `git show refs/remotes/pr2576/base:.../OpenAIPrompt.scala` shows the old body was
  `set(postProcessingOptions, v.asScala.toMap)` (no validation). Working tree `OpenAIPrompt.scala:123-124` now
  delegates to `setPostProcessingOptions(v.asScala.toMap)` → the validating overload at
  `OpenAIPrompt.scala:99-121` (mode inference at 108-118 + `require` conflict check at 100-106).
  **Requirement met**, but see Issue 1 for the empty-map regression this introduces.
- [x] **Verified the codegen template string used by `initFunc` matches the live `Wrappable.pyInitFunc()` template
  byte-for-byte and occurs exactly once.** Reconstructed `PythonWrappable.pyInitFunc()`
  (`core/src/main/scala/.../codegen/Wrappable.scala:279-303`) and ran a Python replica:
  `DEFAULT_LOOP present: True`, `occurrences: 1`, `replacement changed init: True`. The
  `require(result != defaultInitFunc, ...)` drift guard at `OpenAIPromptPythonOverrides.scala:28` is therefore
  live and correct today.
- [x] **Verified generated constructors process `postProcessingOptions` last regardless of caller keyword order.**
  `OptionsLastInitParamLoop` (`OpenAIPromptPythonOverrides.scala:16-24`) pops the key before the
  `for k,v in kwargs.items()` loop and applies it afterwards. Simulated with a faithful replica of PySpark's
  `keyword_only`: `Fake(promptTemplate="x", postProcessingOptions={...})` → `applied order: ['promptTemplate']`,
  then options applied. **Requirement met.** (Side effect → Issue 4.)
- [x] **Verified the generated Python is syntactically valid and that the custom setters actually win.**
  Assembled a faithful replica of `PythonWrappable.pythonClass()` (`Wrappable.scala:307-375`) with the PR's
  `pyAdditionalMethods` appended, then `ast.parse`d it: `PARSE OK`; class body contains
  `duplicates: {'setParams': 2, 'setPostProcessingOptions': 2}`; the *last* definitions are the custom ones
  (`custom setParams wins: True`, `custom setPPO wins: True`). **Requirement met — but only by definition
  ordering, with no guard.** See Issue 3.
- [x] **Verified empty Python option maps stay compatible.** `OpenAIPromptPythonOverrides.scala:35-37`
  short-circuits on `if not value:` → `self._set(...)` only. Combined with `setDefault(postProcessing -> "")`
  (`OpenAIPrompt.scala:236-237`) and the generated getter `return self.getOrDefault(self.postProcessing)`, the
  assertions in `test_OpenAIPromptParams.py:16-23` hold. **Requirement met on the Python side only** — Issue 1
  covers the Java/Scala divergence.
- [x] **Verified public JVM signatures and serialization are unchanged.**
  `setPostProcessingOptions(v: java.util.HashMap[String, String]): this.type` is signature-identical to base;
  only the body changed. New members are `override def pyAdditionalMethods` / `override def pyInitFunc()`
  (both already public in `PythonWrappable`) and `private[openai] object OpenAIPromptPythonOverrides`
  (not public API). No param was added/removed/renamed; `StringStringMapParam.jsonEncode/jsonDecode`
  (`core/.../param/MapParam.scala:26-32`) is untouched, so persisted metadata format is unchanged.
- [x] **Verified conflicts with explicitly selected modes fail immediately** for Scala, Java and Python:
  `OpenAIPrompt.scala:100-106` (`require(getPostProcessing == expected, ...)` on line 102), exercised by
  `OpenAIPromptParamsSuite.scala:65-85` (both overloads) and `test_OpenAIPromptParams.py:110-131`,
  and reached from Python because `OpenAIPromptPythonOverrides.scala:38-39` syncs the Python-side
  `postProcessing` onto `_java_obj` before calling the validated JVM setter.
- [x] **Verified scalastyle constraints are not breached.** `OpenAIPrompt.scala` = 774 lines (limit 800,
  `scalastyle-config.xml:3-4`), max line 119 chars (limit 120, `scalastyle-config.xml:6-7`);
  `OpenAIPromptPythonOverrides.scala` = 67 lines / max 96 chars, with the required Microsoft header
  (`scalastyle-config.xml:12-17`). Headroom on file length is only 26 lines.
- [x] **Verified test wiring.** `CodegenPlugin.scala:testPython` runs pytest against
  `target/scala-2.12/generated/test/python/synapsemltest`; `PyTestGen.main` copies `pyTestOverrideDir`
  (`cognitive/src/test/python/synapsemltest`) into it and `makeInitFiles` creates the missing `__init__.py`,
  so `test_OpenAIPromptParams.py` will be collected. `AnyFunSuite` (without `TestBase`) is an established
  convention here (`ResponseFormatOrderSuite.scala:11`, `AzureMapsTraitsSuite.scala`), and
  `SynapseMLLogging.logClass` is SparkSession-safe (`SynapseMLLogging.scala:66` uses
  `SparkSession.getActiveSession.map{...}.getOrElse(Map())`), so `new OpenAIPrompt()` in a bare suite is fine.
- [x] **Verified no documentation/notebook impact.** Repo-wide ripgrep for `postProcessing` across
  `docs/` and `website/` returns no matches; all 74 repo hits are under `cognitive/`. No doc update is owed.
- [ ] **Did not execute `sbt cognitive/test:compile`, `sbt scalastyle`, `sbt testPython`, or `black --check`.**
  Reason: this is a read-only review worktree on Windows; the repo's own `synapseml-local-setup` skill documents
  that SynapseML sbt builds require WSL/Linux, and `black` is not installed in this environment. Compensating
  evidence: full static verification of the codegen templates via AST parsing (above), manual scalastyle
  budget checks, and manual black-22.3 formatting inspection of `test_OpenAIPromptParams.py` (magic trailing
  commas present on every exploded call — lines 49-52, 58-60, 65-67, 68-71, 103-106, 117-120, 127-130 — so the
  file should be black-stable).

---

## Issues

### Issue 1: Empty `java.util.HashMap` is now rejected — public Java/R API regression and Python/JVM divergence
- **Severity**: Medium
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`
  (and `cognitive/src/main/scala/.../openai/OpenAIPromptPythonOverrides.scala`)
- **Line(s)**: `OpenAIPrompt.scala:99-121`, `OpenAIPrompt.scala:123-124`;
  `OpenAIPromptPythonOverrides.scala:35-37`
- **Description**: The base implementation of the Java overload was
  `set(postProcessingOptions, v.asScala.toMap)` — it accepted **any** map, including an empty one (verified with
  `git show refs/remotes/pr2576/base:...`). After this change it delegates to the validating overload, whose
  pattern match ends in `case _ => throw new IllegalArgumentException("Invalid post processing options")`
  (`OpenAIPrompt.scala:116-117`). An empty `HashMap` matches none of the `delimiter` / `jsonSchema` / `regex`
  guards, so `prompt.setPostProcessingOptions(new java.util.HashMap<String,String>())` now **throws**, where it
  previously succeeded. The same is true for R, whose generated wrapper calls
  `invoke("setPostProcessingOptions", <named list>)` (`Wrappable.scala:rSetterLines`).
  The PR authors clearly knew this hazard, because the Python wrapper special-cases it
  (`OpenAIPromptPythonOverrides.scala:35-37` `if not value: ...`) and the new Python test asserts `{}` succeeds
  (`test_OpenAIPromptParams.py:16-23`). The result is that `{}` is legal from Python but illegal from Java,
  Scala and R for the same logical operation.
- **Risk**: Silent behavioural break for existing Java/R callers and for any config-driven code that passes a
  possibly-empty options map (a very common shape when options come from a properties file or a UI form).
  Because it is a `require`/`throw` at set time, such callers fail hard at job submission. The
  cross-language inconsistency will also confuse anyone porting a Python example to Scala/Java, and it
  contradicts the PR's own "empty option maps must remain compatible" goal, which is only honoured in one
  of the four supported languages. No test covers the empty Java map or the empty Scala map, so the regression
  is invisible to CI.
- **Suggested Fix**: Hoist the empty-map short-circuit into the single Scala source of truth instead of the
  Python template, so every language agrees:
  ```scala
  def setPostProcessingOptions(value: Map[String, String]): this.type = {
    if (value.isEmpty) return set(postProcessingOptions, value)   // parity with the Python wrapper
    ...
  }
  ```
  Then simplify `OpenAIPromptPythonOverrides.scala:35-37` (it can keep the fast path, but it is no longer
  load-bearing), and add tests: empty Scala `Map`, empty `java.util.HashMap`, and the existing empty-Python-dict
  case, all asserting `getPostProcessing == ""` and `getPostProcessingOptions == Map.empty`.

### Issue 2: "Invalid regex/options must fail immediately" is not satisfied — the regex pattern, `regexGroup` and `jsonSchema` are never validated
- **Severity**: Medium
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`
- **Line(s)**: `OpenAIPrompt.scala:108-118` (validation), `OpenAIPrompt.scala:693-703` (`getParser`),
  `OpenAIPrompt.scala:755-774` (`JsonParser` / `RegexParser`)
- **Description**: `setPostProcessingOptions` validates only **key presence**: it checks that `regexGroup`
  accompanies `regex` and that at least one recognised key exists. It never checks that the values are usable.
  All of the following are accepted without error today:
  - `Map("regex" -> "([", "regexGroup" -> "1")` — `"(["` is not a compilable pattern.
  - `Map("regex" -> ".*", "regexGroup" -> "abc")` — not an integer.
  - `Map("regex" -> ".*", "regexGroup" -> "-1")` — negative group index.
  - `Map("jsonSchema" -> "not a ddl schema")` — not parseable by `DataType.fromDDL`.
  The failure is deferred to `getParser` (`OpenAIPrompt.scala:699`,
  `new RegexParser(opts("regex"), opts("regexGroup").toInt)` → `NumberFormatException`; and
  `OpenAIPrompt.scala:767`, `DataType.fromDDL(schema)`), which is first reached from `transformSchema`
  (`OpenAIPrompt.scala:716`). This directly contradicts the stated requirement that invalid regex/options must
  fail immediately, and it is the one part of the requirement set that the new tests do not exercise at all —
  every regex fixture in both new suites (`OpenAIPromptParamsSuite.scala:32,68,71`,
  `test_OpenAIPromptParams.py:39,78,115`) uses a valid pattern and a numeric group.
- **Risk**: A typo in a schema or pattern is not surfaced at configuration time; it surfaces as an opaque
  `NumberFormatException` / `PatternSyntaxException` / DDL parse error much later, potentially after an
  expensive upstream stage has run. Because the pattern is applied per row via `F.regexp_extract`
  (`OpenAIPrompt.scala:771`), a pathological user pattern is also a catastrophic-backtracking (ReDoS) hazard
  against untrusted LLM output — cheap to reject up front, expensive to discover in production.
- **Suggested Fix**: Extend the `regex` and `jsonSchema` branches of the Scala validator (which now serves
  Scala, Java, R **and** Python) — for example:
  ```scala
  case v if v.contains("regex") =>
    require(v.contains("regexGroup"), "regexGroup must be specified with regex")
    require(Try(java.util.regex.Pattern.compile(v("regex"))).isSuccess,
      s"Invalid regex: ${v("regex")}")
    require(Try(v("regexGroup").toInt).filter(_ >= 0).isSuccess,
      "regexGroup must be a non-negative integer")
    setOrValidatePostProcessing("regex")
  case v if v.contains("jsonSchema") =>
    require(Try(DataType.fromDDL(v("jsonSchema"))).isSuccess,
      s"Invalid jsonSchema: ${v("jsonSchema")}")
    setOrValidatePostProcessing("json")
  ```
  (`scala.util.Try` and `DataType` are already imported at `OpenAIPrompt.scala:26,35`.) Add negative tests for
  each case in `OpenAIPromptParamsSuite.scala` for both the Scala and Java overloads, plus one Python case.

### Issue 3: The Python overrides rely on silent, order-dependent method shadowing with no drift guard
- **Severity**: Medium
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptPythonOverrides.scala`
- **Line(s)**: `OpenAIPromptPythonOverrides.scala:32-66` (`methods`); cf. `OpenAIPromptPythonOverrides.scala:26-30`
  (`initFunc`, which *does* have a guard)
- **Description**: `pyAdditionalMethods` is **appended** to the class body, and `PythonWrappable.pythonClass()`
  emits it last (`core/.../codegen/Wrappable.scala:373`), after the auto-generated `setParams`
  (`Wrappable.scala:337-349`) and after `pyParamsSetters` (`Wrappable.scala:367`), which already emits a generic
  `def setPostProcessingOptions(self, value): self._set(postProcessingOptions=value)`
  (`Wrappable.scala:186-191`). I confirmed by AST-parsing a faithful replica of the generated class that the
  body contains **two** `setPostProcessingOptions` and **two** `setParams` definitions, and the custom ones win
  only because Python's last-definition-wins rule happens to favour them
  (`duplicates: {'setParams': 2, 'setPostProcessingOptions': 2}`, `custom setParams wins: True`,
  `custom setPPO wins: True`).
  `initFunc` protects itself against template drift with
  `require(result != defaultInitFunc, "OpenAIPrompt Python initializer template did not match")`
  (line 28). `methods` has **no equivalent guard**. If `pythonClass()` is ever reordered, or a de-duplication /
  lint pass is added to codegen, or `pyParamsSetters` moves after `pyAdditionalMethods`, the custom validated
  setter and `setParams` silently vanish and Python reverts to the exact bug this PR fixes — with a fully green
  build and green Scala tests, because no test asserts anything about the generated Python source.
- **Risk**: Regression of the entire fix with no failing signal. Secondary effects today: the generated file
  has duplicate method definitions (flake8 `F811` class of defect, confusing to anyone reading the wheel), the
  generated `setPostProcessingOptions` docstring (`Wrappable.scala:145-152`, `pyParamSetter`) is lost, and the
  `"""Set the (keyword only) parameters"""` docstring on `setParams` (`Wrappable.scala:342-344`) is lost.
- **Suggested Fix**: Two complementary changes:
  1. Use the same *replace-and-assert* technique as `initFunc` rather than append-and-shadow — i.e. override
     `pyParamSetter`/`pythonClass` output for `postProcessingOptions` and `setParams` via a `String.replace`
     with a `require(result != original, ...)` guard, so template drift breaks codegen loudly instead of
     silently.
  2. Add a cheap Scala unit test (next to `WrappableTests.scala`) that renders `new OpenAIPrompt()`'s Python
     class and asserts the invariant, e.g. that the last occurrence of `def setPostProcessingOptions` is
     followed by `_jvm.java.util.HashMap`, that `def setParams` appears with `dict(self._input_kwargs)` last,
     and that `post_processing_options = kwargs.pop(` appears exactly once. This runs in milliseconds and does
     not require the conda/wheel `sbt testPython` pipeline.
  Never hand-edit the emitted file under `target/scala-2.12/generated/` — the fix belongs in the Scala template.

### Issue 4: Generated `__init__` mutates `self._input_kwargs` in place
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptPythonOverrides.scala`
- **Line(s)**: `OpenAIPromptPythonOverrides.scala:16-18`
- **Description**: The generated `__init__` binds `kwargs = self._input_kwargs` **without copying**
  (`core/.../codegen/Wrappable.scala:293-296`), and PySpark's `keyword_only` sets `self._input_kwargs = kwargs`
  to the very dict it later passes in. The injected `kwargs.pop("postProcessingOptions", None)` therefore
  permanently removes the entry from the constructed object's recorded constructor kwargs. Demonstrated with a
  faithful `keyword_only` replica:
  `Fake(promptTemplate="x", postProcessingOptions={"delimiter": ";"})` →
  `_input_kwargs AFTER __init__: {'promptTemplate': 'x'}`, `postProcessingOptions lost: True`.
  Note the PR's own `setParams` override deliberately copies (`dict(self._input_kwargs)`, lines 54/56) — the
  two code paths are inconsistent about this.
- **Risk**: `_input_kwargs` is a documented-by-convention PySpark attribute; any current or future helper that
  reads it after construction (introspection, repro-script generation, the legacy
  `self.__init__._input_kwargs` fallback path at line 56) sees an incomplete record. Low impact today, but it is
  an invisible, action-at-a-distance side effect introduced purely as an implementation artifact.
- **Suggested Fix**: Copy before popping in `OptionsLastInitParamLoop`:
  ```
  |    if java_obj is None:
  |        kwargs = dict(kwargs)
  |        post_processing_options = kwargs.pop("postProcessingOptions", None)
  ```
  (or use `kwargs.get(...)` plus a `if k == "postProcessingOptions": continue` in the loop).

### Issue 5: `setParams` and the Python setter are non-transactional — the JVM object is mutated before validation
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptPythonOverrides.scala`
- **Line(s)**: `OpenAIPromptPythonOverrides.scala:38-39`, `OpenAIPromptPythonOverrides.scala:63-64`
- **Description**: Two ordering problems on the failure path:
  1. `setParams` executes `self._set(**kwargs)` (line 63) for *all other* parameters **before** calling
     `self.setPostProcessingOptions(value)` (line 64). If the latter raises, every other parameter in the same
     call has already been applied.
  2. `setPostProcessingOptions` writes `postProcessing` into `_java_obj` (line 39) **before** the validating
     JVM call on line 43. On a conflict the JVM object is left with `postProcessing` now *explicitly set*
     (Spark's `set` marks the param as set) even if it had only been Python-side-set before. The new Python
     test asserts exactly this residue at `test_OpenAIPromptParams.py:133-134`
     (`prompt._java_obj.getPostProcessing() == explicit_mode`), so the behaviour is baked in rather than flagged.
- **Risk**: A caller that catches `IllegalArgumentException` and retries on the same object works from a
  partially-mutated state. Concretely,
  `p.setParams(promptTemplate="x", postProcessing="json", postProcessingOptions={"delimiter": ";"})` raises but
  leaves `promptTemplate` and `postProcessing` applied; a subsequent `p.setPostProcessingOptions({"delimiter": ";"})`
  then fails again for a reason the user did not create in that call. This is the standard failure mode of
  non-atomic setters and is easy to hit in notebook workflows where objects are long-lived.
- **Suggested Fix**: Validate before mutating. Add a JVM-side non-mutating helper, e.g.
  `private[openai] def inferPostProcessing(v: Map[String, String]): String` used by both the Scala setter and
  a new Py4J-callable `validatePostProcessingOptions(java.util.HashMap[String,String], String)`, and have the
  Python override call it first; only on success perform the `_set`/`_java_obj` writes. At minimum, reorder
  `setParams` so `setPostProcessingOptions(value)` runs before `self._set(**kwargs)`, and document the residue
  in the test name rather than merely asserting it.

### Issue 6: The new validation is still bypassable through supported public paths, so the original defect remains reachable
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`
  (design), `core/src/main/python/synapse/ml/core/serialize/java_params_patch.py` (transfer path)
- **Line(s)**: `OpenAIPrompt.scala:94-124`; `java_params_patch.py:_mml_make_java_param_pair`;
  `core/.../param/MapParam.scala:21-23` (`w(java.util.HashMap)`)
- **Description**: Mode inference lives only in the setter, so every other supported way of writing the param
  skips it:
  - Python `p._set(postProcessingOptions={"delimiter": ";"})` (private, but the generated `setParams`
    fast path at line 58 also funnels through `_set` when the key is absent).
  - Python `p.copy({p.postProcessingOptions: {...}})` — `Params.copy(extra)` writes `_paramMap` directly.
  - Scala/Java `p.set(p.postProcessingOptions, map)` — `org.apache.spark.ml.param.Params.set` is public.
  - Model load / `_from_java` → `_transfer_params_from_java`.
  Crucially, at transform time `_transfer_params_to_java` reaches the JVM through
  `_mml_make_java_param_pair` → `java_param.w(java_value)` → `_java_obj.set(pair)`, i.e. the raw `ParamPair`
  path, never `setPostProcessingOptions`. Note that `StringStringMapParam` has **no** `pyTypeConverter`
  (`core/.../codegen/DefaultParamInfo.scala:50-51` uses the 2-arg `ParamInfo` ctor), so `_set` performs no
  coercion or checking either. Any of these paths reproduces the exact original symptom: options set,
  `postProcessing` still `""`, `getParser` returns `PassThroughParser` (`OpenAIPrompt.scala:700`) and the
  options are silently ignored.
- **Risk**: The fix is a front-door guard rather than an invariant. The silent-no-op failure mode — the thing
  this PR exists to eliminate — survives on several public paths, and no test documents that gap.
- **Suggested Fix**: Add a backstop where the value is actually consumed, so no path can bypass it. In
  `getParser` (`OpenAIPrompt.scala:693-703`), which is already called from `transformSchema`, add:
  ```scala
  case "" if opts.nonEmpty =>
    throw new IllegalArgumentException(
      s"postProcessingOptions ${opts.keys.mkString(", ")} were supplied but postProcessing is not set")
  ```
  This turns the silent no-op into a loud failure regardless of how the param was written, and complements
  (rather than replaces) the eager setter validation.

### Issue 7: The adjacent `setColumnTypes(java.util.HashMap)` overload still bypasses its Scala validation
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`
- **Line(s)**: `OpenAIPrompt.scala:194-200` and `202-207` vs `OpenAIPrompt.scala:209-210`
  (`validateColumnType` at `OpenAIPrompt.scala:187-190`)
- **Description**: `setColumnTypes(value: Map[String, String])` validates each entry via `validateColumnType`
  (lines 194-199, `require(value == "text" || value == "path", ...)`), but the Java overload 11 lines below is
  still `def setColumnTypes(v: java.util.HashMap[String, String]): this.type = set(columnTypes, v.asScala.toMap)`
  — the *identical* defect this PR fixes for `postProcessingOptions`, in the same class, left untouched. The
  same is true for the Python side: `setColumnTypes` is still the generic `_set` wrapper, so Python callers do
  not get eager validation either.
- **Risk**: Java/Python callers can set `columnTypes` to `{"c": "iamge"}` and only discover it inside
  `processPathColumns` (`OpenAIPrompt.scala:358-361`) at transform time, with a different error message than
  Scala callers get. It also means the class now has an inconsistent contract — one map param validates
  eagerly for all languages, its sibling does not — which is exactly the confusion this PR set out to remove.
- **Suggested Fix**: One-line change mirroring line 124:
  `def setColumnTypes(v: java.util.HashMap[String, String]): this.type = setColumnTypes(v.asScala.toMap)`,
  plus a Java-overload test case in `OpenAIPromptParamsSuite.scala`. (If it is deliberately out of scope for
  this PR, say so in the PR description and file a follow-up, because a reviewer will otherwise read the
  omission as an oversight.)

### Issue 8: Test-quality gaps — a self-satisfying assertion, duplicated coverage, and missing negative cases
- **Severity**: Low
- **File**: `cognitive/src/test/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptParamsSuite.scala`,
  `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIPromptParams.py`
- **Line(s)**: `OpenAIPromptParamsSuite.scala:54-62`; cf. `OpenAIPromptSuite.scala:511-549`
- **Description**:
  1. **Self-satisfying assertion.** The "reject unsupported options" test (lines 54-62) asserts only
     `assert(javaError.getMessage === scalaError.getMessage)`. Since both now execute the same code path by
     construction, this can never fail for a *wrong* message — it would still pass if the message became
     `""` or `"boom"`. Every other test in the file asserts the literal text; this one should too
     (`assert(scalaError.getMessage === "Invalid post processing options")`). Note this message, unlike the
     `require`-based ones, has **no** `"requirement failed: "` prefix, which is itself worth pinning.
  2. **Duplicated coverage.** Five of the six Scala-side behaviours in the new suite already exist verbatim in
     `OpenAIPromptSuite.scala:511-549` (delimiter→csv, jsonSchema→json, regex→regex, invalid options,
     regexGroup requirement, conflicting explicit mode). The genuinely new coverage is the `javaMap(...)`
     variants. Consolidating — moving the credential-free param tests out of the `OpenAIAPIKey`-gated
     `TransformerFuzzing` suite and deleting the originals — would avoid two places drifting apart.
  3. **Missing negative/edge cases** (each ties to an issue above): empty `java.util.HashMap` (Issue 1),
     empty Scala `Map`, invalid regex / non-numeric `regexGroup` / invalid `jsonSchema` (Issue 2),
     re-calling `setPostProcessingOptions` to switch mode families (Issue 9), Python
     `setPostProcessingOptions(None)`, and multi-mode option maps (Issue 10).
  4. **No coverage of the actual mechanism.** The change under review is a codegen string transformation, yet
     nothing asserts anything about the generated Python source; the only Python coverage requires the full
     conda + wheel `sbt testPython` job. See the Issue 3 fix for a millisecond-cost alternative.
- **Risk**: The suite reads as comprehensive but leaves the newly-introduced regression (Issue 1) and the
  unmet requirement (Issue 2) entirely unguarded, and the weak assertion in (1) gives false confidence about
  Scala/Java message parity.
- **Suggested Fix**: Pin the literal message in the "unsupported options" test, add the missing cases listed
  above (each as a `javaMap` + Scala `Map` pair, matching the file's existing structure), and either remove the
  now-duplicated tests from `OpenAIPromptSuite.scala:511-549` or add a comment explaining why both exist.

### Issue 9: Once a mode is inferred it can never be changed — new failure mode for Python and Java callers
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`,
  `cognitive/src/main/scala/.../openai/OpenAIPromptPythonOverrides.scala`
- **Line(s)**: `OpenAIPrompt.scala:100-105`; `OpenAIPromptPythonOverrides.scala:38-39,45`
- **Description**: Inference does not distinguish "the user chose this mode" from "we inferred this mode".
  `setOrValidatePostProcessing` calls `set(postProcessing, expected)` (line 104), and the Python override
  mirrors that into the Python param map with `self._set(postProcessing=...)` (line 45). A subsequent call with
  a different option family therefore hits the `require` on line 102 and fails:
  ```python
  p = OpenAIPrompt(postProcessingOptions={"delimiter": ";"})   # postProcessing := "csv" (inferred)
  p.setPostProcessingOptions({"jsonSchema": "a STRING"})       # IllegalArgumentException: postProcessing must be 'json'
  ```
  Before this PR the Python path was pure `_set`, so replacing the options dict was always allowed. (The Scala
  semantics are pre-existing on master; this PR propagates them to Python, Java and R.) The error message —
  `postProcessing must be 'json'` — is also actively misleading here, since the user never set `postProcessing`
  at all.
- **Risk**: Interactive/notebook users iterating on options get a confusing hard failure and no obvious remedy
  (the workaround, `setPostProcessing("")` first, is undiscoverable and itself trips the conflict check).
- **Suggested Fix**: Track whether `postProcessing` was user-supplied — e.g. a `private var
  postProcessingInferred: Boolean` set in `setOrValidatePostProcessing`, and only enforce the `require` when
  the value was *not* inferred; otherwise overwrite it. Alternatively, keep the strict behaviour but improve
  the message, e.g.
  `s"postProcessing is '$actual' but these options imply '$expected'; call setPostProcessing(\"$expected\") or clear the conflicting option"`.

### Issue 10: Mode-key precedence is silent and unknown keys are ignored when a known key is present
- **Severity**: Low
- **File**: `cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPrompt.scala`
- **Line(s)**: `OpenAIPrompt.scala:108-118`
- **Description**: The validator is a first-match-wins `match` over key presence, so:
  - `Map("delimiter" -> ";", "jsonSchema" -> "a STRING")` silently resolves to `csv`; the `jsonSchema` is
    dropped without warning.
  - `Map("delimiter" -> ";", "regexGroup" -> "1")` is accepted as `csv` and `regexGroup` is silently ignored.
  - `Map("delimiter" -> ";", "delimeter" -> ",")` (a plausible typo) is accepted; the typo is ignored.
  The `"Invalid post processing options"` rejection only fires when *no* recognised key is present, so the
  "unsupported options are rejected" guarantee — which the new tests assert at
  `OpenAIPromptParamsSuite.scala:54-62` — is much weaker than it appears.
- **Risk**: Misconfigurations are silently accepted and produce wrong output rather than an error, which is the
  same class of silent failure this PR is trying to eliminate. Now that the Java/Python/R paths all flow
  through this validator, the weak check is the single gate for every language.
- **Suggested Fix**: Make the validator exhaustive rather than first-match:
  ```scala
  private val ModeKeys = Map("delimiter" -> "csv", "jsonSchema" -> "json", "regex" -> "regex")
  private val AuxKeys  = Set("regexGroup")
  val unknown = value.keySet -- ModeKeys.keySet -- AuxKeys
  require(unknown.isEmpty, s"Unsupported post processing options: ${unknown.toSeq.sorted.mkString(", ")}")
  val modes = value.keySet.intersect(ModeKeys.keySet).map(ModeKeys)
  require(modes.size == 1, s"postProcessingOptions must select exactly one mode, found: ${modes.toSeq.sorted.mkString(", ")}")
  ```
  and add tests for the multi-mode and stray-key cases.

---

## Resolution Log
_Updated by the driving agent as findings are addressed._

### Issue 1
- **Status**: Fixed
- **What changed**: `OpenAIPrompt.setPostProcessingOptions(Map)` now treats an empty map as a valid
  no-op configuration and persists it without inferring a mode. The Java overload still delegates to this
  single Scala implementation, and the generated Python setter now routes empty dictionaries through the same
  JVM path. Scala/Java and Python tests cover explicit empty maps.
- **Why**: Preserving the old Java behavior avoids an API compatibility regression while making empty-map
  semantics consistent across Scala, Java, Python, and serialized defaults.
- **How verified**: `OpenAIPromptParamsSuite` passed 9/9, including
  `Scala and Java setPostProcessingOptions should preserve empty options`; the direct Py4J suite passed
  10/10 tests and 21/21 subtests, including constructor and setter empty-dictionary cases.

### Issue 2
- **Status**: Fixed
- **What changed**: Added package-private `OpenAIPromptPostProcessing`, which validates DDL schemas, regex
  syntax, non-negative/in-range regex groups, unsupported keys, auxiliary-key misuse, and mixed mode families
  before setting any Spark parameter. All public Scala, Java, and generated Python paths share this validator.
- **Why**: Invalid parser values should fail at configuration time with stable `IllegalArgumentException`
  messages rather than surfacing later from `transformSchema` or row processing.
- **How verified**: Scala/Java tests cover invalid DDL, invalid regex syntax, nonnumeric/negative/out-of-range
  groups, mixed modes, and unsupported keys. The matching direct Py4J cases passed in the 10-test suite.

### Issue 3
- **Status**: Fixed
- **What changed**: Added the protected `pySetParamsFunc` codegen hook and overrode `pyParamSetter` only for
  `postProcessingOptions`. The generated class now has exactly one `setParams` and one
  `setPostProcessingOptions`; private helper methods remain in `pyAdditionalMethods`. Each template
  replacement has an explicit `require` drift guard.
- **Why**: Dedicated codegen extension points remove order-dependent last-definition-wins shadowing and make
  future template drift fail during code generation.
- **How verified**: The Scala generated-source test asserts one occurrence of each public method and the
  validation markers. `cognitive/codegen` completed and the generated `OpenAIPrompt.py` compiled with
  `python3 -m py_compile`.

### Issue 4
- **Status**: Fixed
- **What changed**: The generated constructor copies `kwargs` before removing
  `postProcessingOptions`.
- **Why**: Constructor ordering still processes options last without mutating PySpark's recorded
  `_input_kwargs`.
- **How verified**: The Python test `test_constructor_preserves_input_kwargs` passed, and the generated-source
  test asserts `kwargs = dict(kwargs)` precedes the pop.

### Issue 5
- **Status**: Fixed
- **What changed**: Generated Python now converts and validates options against a temporary JVM
  `OpenAIPrompt` before mutating either the real JVM object or Python parameter map. `setParams` performs this
  validation before applying any supplied keyword parameters.
- **Why**: A rejected options/mode combination must leave the caller's object unchanged so notebook retries do
  not inherit partial state.
- **How verified**: Py4J tests assert failed `setParams` leaves `postProcessing` unset and failed public setter
  validation leaves the real JVM object's mode at its default. The direct suite passed 10/10 tests.

### Issue 6
- **Status**: Fixed
- **What changed**: `getParser` now rejects nonempty `postProcessingOptions` when `postProcessing` is empty,
  and `transformSchema` resolves the parser before constructing service schema stages.
- **Why**: Spark's raw `Params.set`, Python `_set`, copy, or deserialization can bypass public setters; the
  consumption-time invariant prevents those paths from silently ignoring options.
- **How verified**: `Raw nonempty postProcessingOptions should not be silently ignored` passed in
  `OpenAIPromptParamsSuite`.

### Issue 7
- **Status**: Deferred
- **What changed**: No `setColumnTypes` behavior was changed.
- **Why**: The cited Java/Python behavior predates this PR and is unrelated to post-processing inference.
  Expanding this focused compatibility fix to another public parameter would introduce a separate behavior
  change without requirements or dedicated cross-language design review.
- **How verified**: `git diff refs/remotes/pr2576/base` confirms the implementation and tests remain scoped to
  post-processing validation plus the reusable codegen hook required to avoid duplicate generated methods.

### Issue 8
- **Status**: Fixed
- **What changed**: Pinned literal error messages, added empty/malformed/mixed/bypass/atomicity cases, added a
  generated-source invariant test, and removed the duplicated parameter-only tests from the credential-gated
  `OpenAIPromptSuite` after moving their coverage into `OpenAIPromptParamsSuite`.
- **Why**: The new suite is deterministic, credential-free, and directly maps each compatibility requirement
  to Scala/Java and Python assertions.
- **How verified**: Scala suite passed 9/9; direct Py4J suite passed 10/10 tests and 21/21 subtests.

### Issue 9
- **Status**: Deferred
- **What changed**: No mode-switching semantics were changed.
- **Why**: Strict conflict rejection is established Scala behavior on the base branch and is an explicit PR
  requirement for all supported public setters. Spark parameter serialization does not retain whether a value
  was user-selected or inferred, so silently overwriting a matching stored mode would make loaded/copied
  stages behave differently and could override an explicit user choice.
- **How verified**: Base history confirms the Scala setter already enforced this rule. Scala/Java and Py4J
  conflict tests pass for csv, json, and regex modes.

### Issue 10
- **Status**: Fixed
- **What changed**: `OpenAIPromptPostProcessing` now requires exactly one primary mode option, rejects unknown
  options, and rejects `regexGroup` unless `regex` is selected.
- **Why**: Invalid or ambiguous maps must fail rather than silently selecting the first matching branch.
- **How verified**: Scala/Java and Python tests cover mixed delimiter/json options, stray `regexGroup`, and
  unsupported keys; all targeted suites pass.

---

## Round 1 Rerun

_Appended after the driving agent addressed the ten findings above. Nothing above this heading was altered;
the original findings and the driving agent's Resolution Log are preserved verbatim. This section reviews the
**current live diff** and records an independent re-verification of every prior resolution._

## Review Summary
- **Round**: 1 (rerun)
- **Theme**: Broad sweep
- **Mode**: sequential (DIRECT)
- **Model**: Claude Opus 5 — Slot 1 (latest Anthropic Opus), highest reasoning effort
- **Artifact**: C:\Users\singhrana\Documents\SynapseML-pr-2576-review\reviews\pr-2576\task-2576-attempt-1-review-1-claude-opus-5.md
- **Issues Found**: 0
- **Verdict**: CLEAN

Scope reviewed: base `refs/remotes/pr2576/base` → current working tree of
`C:\Users\singhrana\Documents\SynapseML-pr-2576-review`. Live change set is **6 tracked files (+489/−68) plus
one new untracked source file (53 lines) = 7 files**:

| File | State |
|---|---|
| `cognitive/src/main/scala/.../openai/OpenAIPrompt.scala` | modified (+22/−15) |
| `cognitive/src/main/scala/.../openai/OpenAIPromptPostProcessing.scala` | **new (untracked, 53 lines)** |
| `cognitive/src/main/scala/.../openai/OpenAIPromptPythonOverrides.scala` | new (+105) |
| `cognitive/src/test/scala/.../openai/OpenAIPromptParamsSuite.scala` | new (+153) |
| `cognitive/src/test/scala/.../openai/OpenAIPromptSuite.scala` | modified (−40, duplicate param tests removed) |
| `cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIPromptParams.py` | new (+191) |
| `core/src/main/scala/.../codegen/Wrappable.scala` | modified (+18/−13, new `pySetParamsFunc` hook) |

No new or reopened issues were found in the current diff, so the `## Issues` and `## Resolution Log` sections
are intentionally omitted for this rerun per the clean-review contract.

## Evidence Checklist

- [x] **Re-derived the live change set rather than trusting the supplied diff.**
  `git --no-pager diff refs/remotes/pr2576/base --stat` + `git status --short` in
  `C:\Users\singhrana\Documents\SynapseML-pr-2576-review` yields the 7 files in the table above.
  Note the regenerated prompt's embedded diff **omits `OpenAIPromptPostProcessing.scala`** because that file is
  still untracked and `git diff` cannot see it; I reviewed the file from disk instead of relying on the prompt.
- [x] **Issue 1 (empty maps) — verified fixed in the single Scala source of truth, not in a Python special
  case.** `OpenAIPromptPostProcessing.inferMode` (`OpenAIPromptPostProcessing.scala:19-22`) returns `None` for
  an empty map, and `OpenAIPrompt.scala:108` applies the mode only via `.foreach(setOrValidatePostProcessing)`,
  so `set(postProcessingOptions, value)` (line 109) still runs. The Java overload (`OpenAIPrompt.scala:112-113`)
  delegates to that same method, and the generated Python setter no longer contains a `if not value:`
  short-circuit — the empty dict travels the identical JVM path. Scala/Java parity is pinned by
  `OpenAIPromptParamsSuite.scala:86-94`; Python parity by
  `test_OpenAIPromptParams.py:15-23`. **Empty maps are accepted consistently in Scala, Java, R and Python.**
- [x] **Issue 2 (parser-value validation) — verified fixed and independently exercised each rejection path.**
  `OpenAIPromptPostProcessing.scala:23-47` now rejects, before any Spark param is written: unknown keys and
  multi-mode/aux-key-only maps (lines 23-29), non-DDL `jsonSchema` via `DataType.fromDDL` (33-36) — the *same*
  function `JsonParser.outputSchema` uses, so validator and parser cannot disagree — uncompilable `regex`
  (39-40), non-integer/negative `regexGroup` (41-43), and `regexGroup` beyond
  `Pattern.matcher("").groupCount()` (44-46). I re-ran the boundary arithmetic myself: `"(.*)"` has
  `groupCount == 1`, so `regexGroup=2` is out of range and `regexGroup=1` is in range, matching
  `OpenAIPromptParamsSuite.scala:96-107` and `test_OpenAIPromptParams.py:79-102`. `require` vs raw
  `IllegalArgumentException` produces two different message shapes (`"requirement failed: "` prefix only for
  `require`), and both suites pin the exact literals, so the distinction cannot silently drift.
- [x] **Issue 3 (method shadowing) — verified fixed by dedicated codegen hooks, and confirmed on the real
  generated artifact, not a replica.** `PythonWrappable` gained `protected def pySetParamsFunc`
  (`Wrappable.scala:306-321`) and `pythonClass()` now emits `${indent(pySetParamsFunc, 1)}`
  (`Wrappable.scala:354`); `OpenAIPrompt` overrides `pyParamSetter` for that one param only
  (`OpenAIPrompt.scala:115-121`) plus `pySetParamsFunc`/`pyAdditionalMethods`/`pyInitFunc` (123-130).
  On the freshly generated
  `cognitive/target/scala-2.12/generated/src/python/synapse/ml/services/openai/OpenAIPrompt.py` I ran an
  `ast.parse` census of the class body: `compile()` → **COMPILE OK (1906 lines)**, `duplicate methods: {}`,
  `def setPostProcessingOptions` × 1, `def setParams` × 1, `def __init__` × 1. Both replacement helpers keep a
  loud drift guard (`require(result != defaultInitFunc, ...)` at `OpenAIPromptPythonOverrides.scala:29`,
  `require(result != defaultSetter, ...)` at line 55), so template drift fails codegen instead of silently
  reverting the fix. The generated-source invariant is also locked in Scala by
  `OpenAIPromptParamsSuite.scala:131-141`.
- [x] **Verified the `Wrappable` refactor is byte-identical for every stage that does not override the new
  hook.** The old inline block used `indent(pyParamsArgs, 2)` at class-body depth; the new path is
  `indent(pySetParamsFunc, 1)` with `indent(pyParamsArgs, 1)` inside. I reimplemented Scala's
  `GenerationUtils.indent` (`GenerationUtils.scala:11-13`, including `String.split` dropping trailing empty
  segments) and diffed old vs new rendering for a representative argument list:
  **`OLD == NEW: True`**. No other stage in `core/`, `cognitive/`, `lightgbm/`, `vw/`, `deep-learning/` or
  `opencv/` overrides `pySetParamsFunc`, so no other generated wrapper changes shape.
- [x] **Issue 4 (`_input_kwargs` mutation) — verified fixed in the emitted file.** The generated `__init__`
  contains `kwargs = dict(kwargs)` immediately before
  `post_processing_options = kwargs.pop("postProcessingOptions", None)`
  (`OpenAIPrompt.py:266-267`, from `OpenAIPromptPythonOverrides.scala:16-24`). Also confirmed the guard is
  `is not None` rather than truthiness, which is what keeps `OpenAIPrompt(postProcessingOptions={})` routed
  through the validated setter instead of being skipped.
- [x] **Issue 5 (atomicity) — verified by executing the generated Python, not by reading it.** I extracted the
  verbatim source of `__init__`, `setParams`, `setPostProcessingOptions`,
  `_to_java_post_processing_options` and `_validate_post_processing_options` from the generated
  `OpenAIPrompt.py` via `ast`, bound them to a faithful Python port of the JVM side
  (`OpenAIPromptPostProcessing.inferMode` + `setPostProcessingOptions` + the `require` conflict check) and ran
  15 assertions. **All 15 passed**, including: constructor with `{}` → `postProcessing == ""` and
  `postProcessingOptions == {}`; constructor infers `csv` on both the Python param map and `_java_obj`;
  `_input_kwargs` still contains `postProcessingOptions` after construction; a conflicting
  `setPostProcessingOptions` raises `postProcessing must be 'csv'` while leaving the **real** `_java_obj`
  mode at `""` and `postProcessingOptions` unset; `None`/`[]`/`""`/`{"delimiter": 1}`/`{1: ","}` all raise
  `TypeError` before any JVM traffic; a conflicting `setParams` leaves **no** sibling parameter applied
  (`promptTemplate` unset, `postProcessing` unset). Validation genuinely happens on the temporary JVM object
  first (`OpenAIPromptPythonOverrides.scala:97-103`), so the caller's object is untouched on failure.
- [x] **Issue 6 (consumption-time backstop) — verified reachable and correctly ordered.**
  `getParser` gained `case "" if opts.nonEmpty => throw new IllegalArgumentException(...)`
  (`OpenAIPrompt.scala:700-701`), placed *before* the `case ""` pass-through so it cannot be shadowed, and
  `transformSchema` now resolves the parser first (`OpenAIPrompt.scala:708`). That reordering is behaviourally
  safe: `outputDataType` was already computed from `getParser` in the old code, the two expressions are
  independent, and moving it earlier only means a bad post-processing configuration is reported before service
  construction. `getParser` is the sole consumer and is also reached from `transform`
  (`OpenAIPrompt.scala:337`), so raw `Params.set`, Python `_set`, `copy(extra)` and model load can no longer
  silently ignore options. Pinned by `OpenAIPromptParamsSuite.scala:143-152`.
- [x] **Issue 7 (`setColumnTypes`) — independently confirmed the deferral is accurate.**
  `git --no-pager diff refs/remotes/pr2576/base -- OpenAIPrompt.scala | Select-String ColumnTypes` returns
  **0 matches**, and the base blob already contained
  `def setColumnTypes(v: java.util.HashMap[String, String]): this.type = set(columnTypes, v.asScala.toMap)`.
  Pre-existing, unchanged, genuinely outside this post-processing diff.
- [x] **Issue 9 (strict mode-switch) — independently confirmed the deferral is accurate.**
  `git --no-pager show refs/remotes/pr2576/base:.../OpenAIPrompt.scala` already contains
  `require(getPostProcessing == expected, s"postProcessing must be '$expected'")` inside
  `setOrValidatePostProcessing`. The rule is pre-existing Scala behaviour, and my simulation confirms Python
  now matches it exactly (`{"delimiter": ";"}` then `{"jsonSchema": "v STRING"}` → `postProcessing must be
  'json'`). Relaxing it only for *inferred* values is not implementable without a new persisted flag, because
  `StringStringMapParam`/`Param[String]` serialization cannot distinguish inferred from explicit values —
  loaded and copied stages would then behave differently from freshly configured ones. Keeping it strict is the
  correct cross-language-parity choice.
- [x] **Issues 8 and 10 (tests, precedence) — verified against the live suites.** Every rejection assertion in
  `OpenAIPromptParamsSuite.scala` now pins a literal message (`assertInvalidOptions`, lines 24-34) instead of
  only comparing Scala vs Java text; the six duplicated param tests were removed from the credential-gated
  `OpenAIPromptSuite` (`git diff` shows −40 lines, all six tests, nothing else); and mixed-mode
  (`{"delimiter", "jsonSchema"}`), stray-aux-key (`{"delimiter", "regexGroup"}`) and unknown-key cases are
  covered in Scala, Java and Python. The remaining `postProcessing*` call sites in `OpenAIPromptSuite`
  (lines 60, 139, 150, 164, 179-180, 277, 314, 504) were each re-checked against the new validator:
  `"prefix STRING, suffix STRING"` is valid DDL, and every options map is single-mode, so no existing test
  regresses.
- [x] **Verified style/lint budgets for the current files.** `OpenAIPrompt.scala` 775 lines / max 119 chars,
  `OpenAIPromptPostProcessing.scala` 53 / 103, `OpenAIPromptPythonOverrides.scala` 105 / 96,
  `OpenAIPromptParamsSuite.scala` 153 / 108, `Wrappable.scala` 516 lines with its only >120-char line (131) at
  line 159, inside the pre-existing `// scalastyle:off line.size.limit` block (153-193) — all within
  `scalastyle-config.xml` limits (800 lines / 120 chars). No tabs, no trailing whitespace, newline at EOF, and
  the required Microsoft header on both new Scala files.
- [x] **Ran the repo's pinned formatter on the new Python test.** `conda activate synapseml && black --version`
  → `black, 22.3.0`; `black --check --diff cognitive/src/test/python/synapsemltest/services/openai/test_OpenAIPromptParams.py`
  → `All done! 1 file would be left unchanged.` The test also follows the module's existing conventions
  (`from synapse.ml.core.init_spark import init_spark; spark = init_spark()`, matching
  `test_StructuredOutput.py`), and `core/src/main/python/synapse/ml/core/init_spark.py` exists.
- [x] **Re-checked documentation impact.** A repo-wide scan of `*.scala`, `*.py`, `*.ipynb`, `*.md`, `*.R`
  (excluding `target/` and `reviews/`) finds `postProcessingOptions` in exactly the five files of this change
  set. No notebook, doc or website page configures post-processing options, so no documentation is owed and no
  published sample breaks under the stricter validator.
- [x] **Examined two residual behaviours and confirmed neither is a defect in this diff.**
  (a) `_validate_post_processing_options` constructs a throwaway JVM `OpenAIPrompt`
  (`OpenAIPromptPythonOverrides.scala:97-99`), whose constructor calls `logClass` →
  `logBase("constructor", ...)` (`SynapseMLLogging.scala:148-150`), i.e. one extra INFO log line per Python
  setter call and, on Fabric only, one async certified-event POST
  (`CertifiedEventClient.scala:24-33` is gated on `runningOnFabric`). This is configuration-time only,
  bounded by the number of setter calls, and is the accepted cost of validating before mutating; it changes no
  observable API behaviour. (b) `DataType.fromDDL` accepts single-token type names such as `"int"`, which
  `from_json` would later reject as non-struct — but `JsonParser.outputSchema` uses that exact same
  `DataType.fromDDL` call (`OpenAIPrompt.scala:768`), so the validator is precisely as strict as the parser it
  guards, and this is strictly better than the base branch, which validated nothing at all. Neither warrants a
  finding.
- [ ] **Did not re-execute `sbt cognitive/testOnly ...OpenAIPromptParamsSuite`, `sbt scalastyle`, or
  `sbt cognitive/testPython`.** Reason: this review pass is constrained to read-only inspection plus artifact
  edits, and those commands rewrite `target/`. Compensating evidence: the already-generated
  `OpenAIPrompt.py` (regenerated at 2026-07-31 19:01) was byte-inspected, `compile()`d and AST-audited for
  duplicates; the generated Python's runtime semantics were executed against a faithful JVM port (15/15
  assertions); `black 22.3.0` was run for real; and every style budget was measured directly. The driving
  agent's reported results (Scala 9/9, direct Py4J 10/10 + 21 subtests, cognitive codegen, `py_compile`) are
  consistent with everything observed statically and dynamically here.

## Round 1 Rerun — Prior Findings Status

| # | Original severity | Claimed | Independently verified | Notes |
|---|---|---|---|---|
| 1 | Medium | Fixed | **Confirmed fixed** | Empty-map acceptance hoisted into `inferMode`; identical in Scala/Java/R/Python |
| 2 | Medium | Fixed | **Confirmed fixed** | DDL, regex syntax, group numeric/non-negative/in-range, all message literals pinned |
| 3 | Medium | Fixed | **Confirmed fixed** | Generated class AST shows zero duplicate methods; two `require` drift guards |
| 4 | Low | Fixed | **Confirmed fixed** | `kwargs = dict(kwargs)` present; simulation shows `_input_kwargs` intact |
| 5 | Low | Fixed | **Confirmed fixed** | Temp-JVM validation first; failure leaves setter *and* `setParams` fully unmutated |
| 6 | Low | Fixed | **Confirmed fixed** | `getParser` backstop before the pass-through case; `transformSchema` resolves parser first |
| 7 | Low | Deferred | **Deferral justified** | `setColumnTypes` byte-identical to base; 0 diff hunks touch it |
| 8 | Low | Fixed | **Confirmed fixed** | Literal messages pinned; duplicates removed; generated-source invariant test added |
| 9 | Low | Deferred | **Deferral justified** | `require(getPostProcessing == expected, ...)` exists on base; serialization cannot carry an "inferred" flag |
| 10 | Low | Fixed | **Confirmed fixed** | Exhaustive single-mode/unknown-key/aux-key validation replaces first-match-wins |

**New or reopened issues in the current diff: none.**

## Round 1 Rerun — Non-blocking Handoff Note

`cognitive/src/main/scala/com/microsoft/azure/synapse/ml/services/openai/OpenAIPromptPostProcessing.scala` is
still **untracked** (`git status` shows `??`). `OpenAIPrompt.scala:108` depends on it, so a commit created with
`git commit -a` would omit it and break the build. Run `git add` on that file (and on
`reviews/pr-2576/`, per the artifact-commit rule) before committing. This is a working-tree/staging
observation, not a defect in the reviewed code, and does not affect the CLEAN verdict.

## Post-Round 1 Architecture Follow-up

Round 2 replaced the temporary `OpenAIPrompt` instance used by generated Python validation with the
non-instantiating static JVM bridge
`OpenAIPromptPostProcessing.validateAndInferMode`. It performs validation without constructor telemetry,
global-parameter registration, or eager JVM parameter mutation. The later direct Py4J suite passed 12 tests
and 21 subtests with this architecture. This supersedes the temporary-stage implementation described in the
Round 1 rerun's residual-behavior note while preserving its CLEAN verdict.
