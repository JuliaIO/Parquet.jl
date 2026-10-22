# Parquet Java N5 oracle

This test-only project pins Parquet Java 1.17.1 from peeled upstream commit
`78a8d3230eb4769db93de5f2f2e18363c04cae81`. It follows that release's Hadoop
3.3.0 and SLF4J 1.7.33 pins. The test pin is JUnit 5.11.4. All Maven plugins have
fixed versions. The build requires Maven 3.9.8 and emits Java 11 bytecode. The
locked gate uses Temurin 11.0.28+6. Local checks can use a newer Java runtime.

The Maven 3.9.8 archive SHA-512 is
`7d171def9b85846bf757a2cec94b7529371068a0670df14682447224e57983528e97a6d1b850327e4ca02b139abaab7fcb93c4315119e6f0ffb3f0cbc0d0b9a2`.
The locked N5 OCI image supplies the complete Maven repository for offline runs.

The harness owns fifteen legacy nested fixtures and diagnostic controls. It writes each with
Parquet Java V1 and V2 pages. The writer uses uncompressed pages, disables the
dictionary, enables page checksums and validation, and fixes page and row-group
targets at 1 MiB and 128 MiB. The JSON evidence records this configuration. The
harness canonicalizes each writer-owned footer by stable Parquet encoding enum
value. This removes Parquet Java's process-dependent encoding-set iteration order.
The canonicalizer preserves duplicate encodings and rejects the file unless the
data/page prefix and every other decoded footer field stay exact. It also requires
the original and canonical footers to be exact Compact Thrift round trips.
harness then checks raw `Group` rows, physical schemas, row-group-local repetition
and definition levels, dense values, page versions, and page row counts. Supported logical cases
are also read through `AvroParquetReader<GenericRecord>` with
`parquet.avro.add-list-element-records=false`. Unsupported high-level layouts
remain explicit diagnostic rejections.

The binding LIST rule-3 fixture retains the specification's `LIST` annotation on
the repeated inner group. Inferred Avro must return `LIST<LIST<INT32>>` and all
rows exactly. A separately identified unannotated near-neighbor records Parquet
Java 1.17.1's diagnostic `ClassCastException`. No explicit Avro schema changes the
binding interpretation.

The direct legacy LIST-of-MAP controls are also split. The INT32-key fixture
retains exact ordered physical pairs and records Avro's unsupported-key rejection.
The UTF8-key fixture retains duplicate physical pairs and requires inferred Avro
to report `array<map<string,int>>` with last-value-wins logical rows.

Generate and verify the Java-owned fixtures:

```sh
./run.sh generate --output /tmp/n5-java --evidence /tmp/n5-java.jsonl
```

Verify Parquet files produced or rewritten by Julia. Verification is always
strict. Every input must bind through Java case metadata, a Java case ID in its
file name, or the explicit single-file case and page-version options:

```sh
./run.sh verify --input /tmp/julia-files --evidence /tmp/julia-java.jsonl
./run.sh verify --input /tmp/julia-rule1.parquet \
  --case-id list_rule1_primitive --page-version v1 \
  --evidence /tmp/julia-rule1-java.jsonl
```

Inspect other files without applying a Java-owned expected case:

```sh
./run.sh inspect --input /tmp/files --evidence /tmp/inspect.jsonl
```

Run the deterministic generation and semantic self-test:

```sh
./check.sh
```

Pass `--offline` before the command for the locked OCI gate. Every command fails
when its input set is empty. Evidence is UTF-8 JSON Lines. It has no timestamps
or host paths.
