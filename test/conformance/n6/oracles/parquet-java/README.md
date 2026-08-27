# N6 Parquet Java interoperability harness

This harness checks the reviewed Parquet Java 1.17.1 N6 capability scope. It
uses the release runtime whose jar manifest names exact source commit
`78a8d3230eb4769db93de5f2f2e18363c04cae81`. It also binds the source files
that implement local reading, legacy statistics conversion, version parsing,
and PARQUET-251 policy.

The run path does not use Maven, a network, an ambient Java executable, an
ambient classpath, or an ambient Python installation. It uses the pinned
Temurin 21.0.8+9 tree already selected for the N6 raw Java scanner. It uses
three exact Maven Central jar inputs copied into the ignored `build/` cache by
`build.sh`. The Java audit process receives a minimal environment, explicit
heap and metaspace limits, read-only snapshots, bounded output, and a timeout.

The base logical and compatibility scope has 34 records. It covers 14 files, 13 passing
`read.logical-values` results, four fixture-backed
safe legacy-bound suppression results, one standalone
`producer-parquet-251` policy result, and one explicit `UNSUPPORTED` result for
`wire.statistics.nan-count`. The full reviewed scope adds ten passing
`wire.column-order.type` results and five new file records. The total is then
49 records. Each positive result compares Parquet Java's exposed column order,
path, physical type, and value count with the frozen raw facts before it emits
the shared raw observation envelope. The policy result has no fake file record.

## Prepare the offline cache

Obtain these exact upstream artifacts outside the repository. `build.sh` does
not download them and rejects every wrong byte identity.

- `parquet-cli-1.17.1-runtime.jar`, SHA-256
  `d0173051493c506a298c691e555a41a682a405895fd0c8cc429a7e1cb1fcc711`
- `hadoop-client-api-3.3.0.jar`, SHA-256
  `d549ba6d131fd6c8e5d42a78dab5c790950edd6258523dedc556b537ca6654aa`
- `hadoop-client-runtime-3.3.0.jar`, SHA-256
  `2ba23f1e1dbb03e73600a41fcb187ad2626529684ed226085a91b0a0d6d67ee5`

Then build the deterministic Java harness jar. The build rejects symbolic links
and non-directory cache paths before it writes any output.

```sh
export PARQUET_N6_JAVA_JDK_ROOT=/path/to/pinned/temurin-21.0.8+9
export PARQUET_N6_PARQUET_CLI_JAR=/path/to/parquet-cli-1.17.1-runtime.jar
export PARQUET_N6_HADOOP_CLIENT_API_JAR=/path/to/hadoop-client-api-3.3.0.jar
export PARQUET_N6_HADOOP_CLIENT_RUNTIME_JAR=/path/to/hadoop-client-runtime-3.3.0.jar
test/conformance/n6/oracles/parquet-java/build.sh
```

## Run a draft

Draft mode accepts a planned evidence entry. It does not relax any input,
source, binary, or descriptor digest check. The descriptor must have the exact
hash authorized in `capabilities.toml`. The normalized raw input must select the
exact repository input declared by `manifest.toml`.

```sh
export PARQUET_N6_INTEROP_PYTHON_ROOT=/path/to/reviewed/cpython-3.12.8
export PARQUET_N6_JAVA_ROOT=/path/to/parquet-java-1.17.1
export PARQUET_N6_JAVA_JDK_ROOT=/path/to/pinned/temurin-21.0.8+9
test/conformance/n6/oracles/parquet-java/run.sh \
  /path/to/parquet-testing \
  test/conformance/n6/evidence/raw-java-apache-corpus.normalized.jsonl \
  /private/tmp/parquet-java.normalized.jsonl \
  --draft
test/conformance/n6/oracles/parquet-java/check.sh \
  /path/to/parquet-testing \
  test/conformance/n6/evidence/raw-java-apache-corpus.normalized.jsonl \
  /private/tmp/parquet-java.normalized.jsonl \
  --draft
```

Run unit tests and the two-pass integration rehearsal:

```sh
"$PARQUET_N6_INTEROP_PYTHON_ROOT/bin/python3.12" -I -S -B \
  test/conformance/n6/oracles/parquet-java/runtests.py \
  --integration \
  --repository . \
  --corpus-root /path/to/parquet-testing \
  --raw-evidence test/conformance/n6/evidence/raw-java-apache-corpus.normalized.jsonl \
  --java-root "$PARQUET_N6_JAVA_ROOT" \
  --jdk-root "$PARQUET_N6_JAVA_JDK_ROOT" \
  --draft
```

The integration command writes the exact manifest target at
`test/conformance/n6/evidence/parquet-java.normalized.jsonl`, then checks it
without changing its bytes.

This directory does not authorize evidence freeze, oracle publication,
`oracles.lock`, or a release claim.
