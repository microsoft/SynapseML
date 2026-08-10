# SynapseML Local Setup Troubleshooting

## Wrong Java runtime

The Spark 4.1 branch is built and tested with JDK 17. Check the active runtime:

```bash
java -version
```

If it is not JDK 17, select the supported toolchain:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
```

Then rerun SBT.

## Java module access

If sbt reports an access error under `java.util.prefs`, open that package for
the sbt process:

```bash
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:+$JAVA_TOOL_OPTIONS }--add-opens=java.prefs/java.util.prefs=ALL-UNNAMED"
```

The local wrapper applies this option automatically.

## External service test safety

Azure Search tests can create and delete real indexes. Search for live hooks before running:

```bash
rg -n "beforeAll\\(|afterEach\\(|SearchIndex\\.createIfNoneExists|AzureSearchWriter\\.write\\(|AzureSearchWriter\\.stream\\(|getExisting\\(|deleteIndex" <test-file-or-dir>
```

If matches are present, inspect the suite and ask the user before running it.

## Python notes

SynapseML Python wrappers are generated from Scala. Do not edit generated files under `target/`. Use `sbt codegen` when wrapper regeneration is needed.
