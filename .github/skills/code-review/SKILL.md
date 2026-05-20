---
name: code-review
description: Review SynapseML Python and Scala code changes. Use before finalizing PR reviews or implementation changes to check security, compatibility, style, generated code, and targeted tests.
---

# Code Review

Use this skill when reviewing SynapseML changes.

## Steps

1. Inspect diff: `git --no-pager diff --stat && git --no-pager diff`
2. Run Scala style: `sbt scalastyle test:scalastyle`
3. Run Python format check: `black --check --extend-exclude 'docs/' .`
4. Run targeted tests for touched code.
5. Apply the checklists below to every changed file.
6. Report only concrete issues with file paths and fixes.

## Security Checklist

Apply when changes touch serialization, I/O, network, or authentication code.

### Deserialization (CWE-502)
- [ ] No raw `ObjectInputStream.readObject()`: use `SafeObjectInputStream` with an allowlist
- [ ] `resolveClass` allowlist validates array component types. Never allowlist the `[` prefix
      directly; array handling must extract and validate the component class name
- [ ] `resolveProxyClass` is overridden to block or validate dynamic proxy interfaces
- [ ] Allowlist uses package-prefix matching, not blocklisting
- [ ] Allowlist presets contain no catch-all entries that bypass the filtering logic

### Input Validation
- [ ] File paths, URLs, and user-supplied strings are validated before use
- [ ] No unsanitized string interpolation into SQL, shell commands, or config

### Resource Management
- [ ] Streams, connections, and closeable resources use `using()` or `try-finally`
- [ ] Cleanup runs even when assertions or exceptions are thrown (especially in tests)

### Secrets & Credentials
- [ ] No hardcoded secrets, tokens, or passwords in source or test code
- [ ] Credentials loaded from environment variables or secure config only

## API Compatibility Checklist

Apply when changes modify public classes, traits, or companion objects.

### Binary Compatibility (JVM)
- [ ] No method signature changes on existing public methods (default parameters
      generate synthetic bridges; use explicit overloads instead)
- [ ] No removed or renamed public classes, traits, or objects
- [ ] Companion object `extends DefaultParamsReadable[T]` preserved if it existed

### Source Compatibility
- [ ] New parameters have defaults so existing callers compile unchanged
- [ ] No narrowed return types or widened parameter types on public methods
- [ ] Import changes don't break wildcard imports in downstream code

## Scala Checklist

- [ ] License header present (enforced by scalastyle)
- [ ] `Wrappable` trait mixed in if the class needs a Python wrapper
- [ ] `SynapseMLLogging` trait mixed in; `logClass()` called in constructor
- [ ] No wildcard imports where explicit imports suffice (`java.io._` → named imports)
- [ ] No RDD API usage. Use DataFrame/Dataset only
- [ ] Lines ≤ 120 chars, files ≤ 800 lines

## Python Checklist

- [ ] License header present
- [ ] Formatted with `black==22.3.0`
- [ ] No edits to files under `target/` (auto-generated)
- [ ] Hand-written overrides in `src/main/python/` extend the generated `_ClassName`

## Test Checklist

- [ ] New functionality has corresponding tests
- [ ] Tests use `using()` for resource cleanup (no bare `.close()` after assertions)
- [ ] Negative tests verify rejection/error cases, not just happy paths
- [ ] No test-only dependencies leaked into main scope
