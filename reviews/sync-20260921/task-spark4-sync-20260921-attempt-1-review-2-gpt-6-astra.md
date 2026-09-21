# Round 2: architecture and patterns

**Result:** CLEAN (no actionable architecture finding).
**Reviewer:** GPT-6 Astra, direct fallback, 2026-09-21.
**Scope:** staged Spark 4.1 sync, content tree
`8a40c8312e62e3d7e650a851d968ddfc0b95a3d8`.

The requested Gemini slot could not run: Gemini 3.8 and 3.7 rejected requests,
and the Gemini 3.6 agent failed before its first turn with HTTP 400. This is
not a Gemini review and does not establish the gauntlet's three-family gate.

- `Wrappable.scala` keeps scalar/column aliases, getter/setter generation, and
  `.pyi` declarations together. Aliases are checked before both generated
  outputs. It matches master exactly.
- `Utils.py` shares service-parameter classification, validation, conversion,
  and legacy-wrapper fallback. Generated metadata avoids JVM introspection
  during ordinary configuration. It matches master exactly.
- `OpenAIPromptPythonOverrides.scala` reuses those helpers and validates
  service setters on a scratch JVM copy before applying them to the original.
  Its only master-relative differences retain the port's zero-argument
  `super()` calls.
- The cleanup parser change is identical to the isolated master prerequisite:
  unknown nested relation metadata fails the complete inventory read. It does
  not introduce a second port implementation or public/serialized API.
- Runtime/dependency settings remain target-owned. Spark 4.1 collection,
  streaming, NumPy, and Python adaptations are retained. The Fabric job stays
  disabled; this sync does not authorize a runtime rollout.

Master prerequisite validation completed on JDK 11: core compile, test compile,
both Scala-style tasks, and 44 cleanup tests passed without skips. Port
validation had passed 44 cleanup, 30 codegen, and 36 cognitive tests at review
time; full code generation and remote validation were still pending.
