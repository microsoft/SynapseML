# SynapseML Local Setup Troubleshooting

## Java 21 compiler bridge failure

Failure signature:

```text
Non-compiled module 'compiler-bridge_2.12' for Scala 2.12.17. Compiling...
bad constant pool index: 0
while compiling: <no file>
library version: version 2.12.17
compiler version: version 2.12.17
```

Cause: the local default Java 21 runtime is not suitable for this SynapseML Scala 2.12 build path.

Fix:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
```

Then rerun SBT.

## Known-good validation

This was validated on 2026-05-05:

```bash
sbt 'cognitive/Test/compile'
sbt 'core/testOnly com.microsoft.azure.synapse.ml.stages.UDFTransformerSuite -- -z "Apply inputCol after inputCols error"'
```

Results:

- `cognitive/Test/compile` passed in 242 seconds.
- The filtered `UDFTransformerSuite` smoke test passed in under 10 seconds after compilation.

## External service test safety

Azure Search tests can create and delete real indexes. Search for live hooks before running:

```bash
rg -n "beforeAll\\(|afterEach\\(|SearchIndex\\.createIfNoneExists|AzureSearchWriter\\.write\\(|AzureSearchWriter\\.stream\\(|getExisting\\(|deleteIndex" <test-file-or-dir>
```

If matches are present, inspect the suite and ask the user before running it.

## Python notes

SynapseML Python wrappers are generated from Scala. Do not edit generated files under `target/`. Use `sbt codegen` when wrapper regeneration is needed.
