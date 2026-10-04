# Raw Parquet 2.13 footer scanner

This directory contains a test-only Java scanner for raw Parquet footer
evidence. It generates Java format classes from the pinned Parquet 2.13 IDL.
It then decodes Compact Thrift in a separate JVM process.

This scanner does not use Parquet.jl, parquet-java, or parquet-java's embedded
Parquet 2.12 format classes. It does not prove parquet-java semantic support.
It records raw Parquet 2.13 wire evidence only.

## Authority and fixed inputs

The N6 plan is `docs/dev/n6-statistics-plan.md` at SHA-256
`15adf34af765a3300d8a49ced73532ed02b7e4edee5764453c58e53fcf12c304`.
The format authority is Apache `parquet-format` commit
`c47e2a66e88943fc46fde1b028a9432f14fdf5c0`. The checked-in
`thrift/parquet.thrift` has SHA-256
`53bb8fc9b96469d7ca694121ead839e449e5156d7bf79f0df728cdd72796df38`.

The harness uses Apache Thrift 0.23.0 for both generation and decoding. It uses
Eclipse Temurin 21.0.8+9 for Java compilation and execution. The download URLs
and every expected digest are in `toolchain.env`.

| Input | Version or source | SHA-256 |
| --- | --- | --- |
| Apache Thrift source | 0.23.0 | `1859d932d2ae1f13d16c5a196931208c116310a5ff50f2bfd11d3db03be8f46f` |
| Homebrew compiler formula | core commit `bd296b14f19462baf03d5d96920209087ca99fa0` | `3929691e8a327dd0e61f41c8d7a6cccdd30ab1213b01a73d0148b195d752b209` |
| Apache Thrift Java runtime | 0.23.0 | `8b41b67a5ff13c371ab18b6d34506121dcecf11372829f7d50115cfb1bf72d42` |
| Apache Thrift runtime POM | 0.23.0 | `eeadf7b9d1e22ac01985fe552384ceecf44018cae93e7a23d6b0466f856330f3` |
| SLF4J API | 1.7.36 | `d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0` |
| SLF4J no-op binding | 1.7.36 | `c214958b07816cb4412b30c7bdbd4308ffdc6ba2a83767b8f3a9229cbd9274d6` |
| Generated Java source manifest | exact 2.13 IDL, 67 files | `f738c7346ad1dd70faafd54815f829b8587a2b0397ff6b6e6710a3a7276cac09` |
| Eclipse Temurin JDK archive | 21.0.8+9, macOS arm64 | `59422c2292ae4e76b87e00d8808dbe49cffa39af731e08bb0292ddb0af4e0261` |
| Pinned JDK `bin/java` | 21.0.8+9, macOS arm64 | `0045ae168ee132bbf469a26fb17dac6d1dee431c9b7826474f3b6ee574a997c9` |
| Pinned JDK `bin/javac` | 21.0.8+9, macOS arm64 | `7be7937fc6bae0ca89f0866f9ce94fc40a935dfb87806d3c701eca3402cfb90a` |
| [`jdk-darwin-arm64-sequoia.manifest`](jdk-darwin-arm64-sequoia.manifest) | 542 files and directories with type, mode, path, content or link target | `d595de66a27187223eb987765fc6c9c341d509ecd15de704656a2980bc6217bc` |

The compiler is the 0.23.0 binary from the content-addressed Homebrew bottle.
The formula source is pinned at Homebrew core commit
`bd296b14f19462baf03d5d96920209087ca99fa0`.

| Validated compiler platform | Bottle SHA-256 | Extracted compiler SHA-256 |
| --- | --- | --- |
| macOS 15 arm64 | `dd6ed015e1b7a980c3dfa2b0dd1c01d563a8cf73bdb7f3de87d0cc1656fc1e1b` | `5ee94e75371f7d0b2467db3acdb67b8b3814fcae3748c0ef078a490a15c57e11` |

The harness rejects other hosts. This avoids claiming support for toolchain
artifacts that have not completed this exact self-test. The initial exact run
used macOS 15.6.1 arm64. Add another platform only after its JDK archive,
compiler bottle, extracted executables, and full self-test are pinned.

The generated sources and downloaded tools stay under the ignored `build/`
directory. `scripts/generate.sh --check` regenerates all 67 classes and checks
the canonical per-file digest manifest. The runtime classpath contains only
the scanner classes, generated 2.13 classes, `libthrift`, `slf4j-api`, and
`slf4j-nop`.

The scripts always invoke the downloaded and verified JDK. They do not use a
host `java` or `javac`. Compilation uses the pinned `javac --release 11`.

## Use

The first command downloads the fixed toolchain artifacts. Later runs use the
verified local cache.

```sh
./test/conformance/n6/oracles/raw-java/check.sh
./test/conformance/n6/oracles/raw-java/run.sh scan \
    --input path/to/file-or-directory \
    --output raw-footer-evidence.jsonl
```

For a directory input, the scanner selects regular files whose names end in
`.parquet`. It sorts relative path labels with Java string order. It rejects a
duplicate label across inputs. If `--output` is absent, it writes JSONL to
standard output. It collects all evidence before output and replaces an output
file only after every input succeeds.

The output must not be the same path, hard link, or symbolic-link target as an
input. Output replacement requires an atomic move. If the file system does not
support it, the scan fails and leaves the old output unchanged.

`check.sh` performs these checks:

- exact plan, IDL, compiler, runtime, and generated-source hashes;
- exact JDK archive, executable, release-file, and full extracted-tree hashes,
  plus rejection and recovery from fake cached Java and Thrift executables and
  a corrupted `lib/modules` image;
- a clean Java 11 compilation;
- two byte-identical scans of the generated fixture;
- equality with `expected/self-test.jsonl`;
- direct field-ID, byte, bit-pattern, and presence assertions;
- raw ColumnOrder states for IDs 1 and 2, an unknown ID, a wrong wire type, and
  an empty union;
- rejection of direct, hard-link, and symbolic-link output aliases without an
  input-byte change;
- stable two-pass input hashing and deterministic mutation-race rejection;
- required metadata, nonnegative row-group and column counts, schema topology,
  child-count, leaf/column type agreement, and duplicate-path validation;
- pre-decode Compact-Thrift depth, declared binary length, container length,
  aggregate element, and aggregate binary-byte limits;
- incremental JSON character-limit enforcement before `StringBuilder` growth;
- rejection of invalid magic, invalid footer containment, trailing
  Compact-Thrift bytes, and a short file;
- preservation of an existing output file after a failed scan.

## Evidence

`evidence.schema.json` version 3 applies to each JSONL line. Output has no timestamps or
absolute input paths. Strings use deterministic ASCII JSON escapes. SHA-256
identifies the complete file and the exact encoded footer.

The evidence records:

- an ordinal and path-keyed schema leaf table. Each leaf retains its physical
  type; presence and value for `type_length`, legacy `converted_type`, `scale`,
  `precision`, repetition type, and field ID; and the raw LogicalType union
  member with INTEGER, DECIMAL, TIME, TIMESTAMP, VARIANT, GEOMETRY, and
  GEOGRAPHY parameters. A present LogicalType that the pinned generated union
  cannot identify retains `present: true` with a null member and parameters;
- a schema-leaf ordinal on every matched row-group column and ColumnOrder
  entry. The scanner rejects a column path or physical type that disagrees
  with the leaf table;
- the exact raw `FileMetaData.column_orders` union header bytes, field ID, wire
  type, state, and known member name for each entry. Any signed i16 field ID is
  retained. States distinguish known, unknown, wrong-type, and empty unions;
- every `Statistics` field from ID 1 through ID 9, with separate presence and
  value states;
- modern bounds at field IDs 5 and 6;
- exactness flags at field IDs 7 and 8, including present `false` versus
  absent;
- null, distinct, and NaN counts, including present zero versus absent;
- exact bound bytes in PLAIN wire order, including length-prefix-free
  BYTE_ARRAY values;
- Float32, Float64, and FLOAT16 bit patterns in conventional most-significant
  byte first hexadecimal form, with an explicit width-valid flag.

The self-test fixture is a metadata-only Parquet envelope. It is not a value
interoperability fixture. It includes TYPE_ORDER and IEEE_754_TOTAL_ORDER,
modern and deprecated bounds, positive and negative zero, two NaN payloads,
FLOAT16, arbitrary byte bounds, present and absent counts, all exactness
states, and INTEGER, DECIMAL, TIME, and TIMESTAMP schema parameters with legacy
annotations.

## Boundary

The scanner reads ordinary `PAR1` footer envelopes. It does not read encrypted
`PARE` footers. It accepts at most 10,000 input files, an 8 GiB file, a 64 MiB
footer, 1,000,000 schema or column-order entries, 1,000,000 elements in one
Compact-Thrift container, 4,000,000 aggregate container values, 64 MiB of
aggregate Compact-Thrift binary payload, 128 nested Compact-Thrift structs or
containers, and 256 MiB of retained JSON text. It hashes each open input twice
and rejects any content, size, path
identity, or modification-time change. It validates required metadata and
schema topology before it reports evidence.

This is a local test-corpus tool. The structural preflight runs before generated
decoding and prevents allocations from oversized wire declarations. Its inputs
must still be trusted fixtures. These limits do not make it a network service
or a general hostile-input sandbox. The scanner exposes raw metadata; it does
not apply Parquet order, trust, or pruning semantics.

This is local N6 test evidence. This directory does not publish an oracle
image and does not create or authorize a repository `oracles.lock`.
