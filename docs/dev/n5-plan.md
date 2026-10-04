# N5 nested conformance and hardening plan

This document refines N5 of the nested implementation plan. It is an
implementation agreement, not a statement of current support. Production code
does not change until the independent review of this plan reaches agreement.

## Binding inputs

- Apache Parquet format 2.13.0 at commit
  `c47e2a66e88943fc46fde1b028a9432f14fdf5c0`.
- Apache `parquet-testing` at commit
  `09f3cdbde45302f0f0c689c950e465e98a9df960`.
- Julia 1.10 and the current stable Julia release.
- Parquet Java tag `apache-parquet-1.17.1`, peeled commit
  `78a8d3230eb4769db93de5f2f2e18363c04cae81`.
- Arrow Rust tag `59.2.0`, commit
  `782e5a685501a9db6cc8e9a3b7cbff894940c47a`.

The Java and Rust projects are test oracles. They do not become package
dependencies. Their wrappers, lock files, source revisions, and artifact hashes
are checked in or verified before execution.

## Scope

N5 closes the Stage 4 nested-data gate. It adds generated wire-format evidence,
hostile-input coverage, exact resource boundaries, and durable Java and Rust
oracles for legacy nested layouts.

N5 does not implement ColumnIndex contents, statistics, bloom filters, predicate
pushdown, encryption, Variant, geospatial values, Dataset, or release work. It
does not change the public API or add exports.

## Baseline and open evidence

The current suite has schema-plan tests for all five specified LIST compatibility
rules. It uses six internal rule IDs because rule 4 has two exact name forms:
`array` and `<outer>_tuple`. Synthetic `LeafStream` tests cover values for each
rule. Full serialized corpus coverage exists for rules 1, 3, and 5, but not rule
2 or either rule-4 name.

MAP schema-plan tests cover modern and legacy outer annotations, standalone
`MAP_KEY_VALUE`, positional child names, optional-key compatibility, omitted
values, and marked entry groups. Full serialized tests cover several standard,
Impala, optional-key, omitted-value, and package-written forms. Standalone outer
`MAP_KEY_VALUE`, arbitrary child names, and direct legacy LIST-of-MAP do not yet
have serialized independent fixtures. Prior PyArrow and DuckDB checks cover
canonical output, but they are temporary evidence and not the required durable
Java and Rust gate.

The pinned corpus supplies canonical nested data, repeated unannotated data,
Impala list and map forms, omitted map values, optional map keys, old nested
lists, and a large-page limit fixture. The fixed tests are strong but do not yet
provide all required evidence.

The missing evidence is:

- An independent recursive shredder and assembler model.
- Serialized files for LIST rule 2, both rule-4 names, standalone outer
  `MAP_KEY_VALUE`, arbitrary MAP child names, and direct legacy LIST-of-MAP.
- Deterministic generated schemas and values across recursive combinations.
- Mutations between count, validation, and emission passes.
- Exact success and one-byte-or-one-element-short resource boundaries.
- Durable Parquet Java and Arrow Rust generation and verification jobs.

### Binding nested corpus

CI verifies the corpus commit and these SHA-256 values before tests run. A wrong
hash or missing file fails the gate; it does not produce a skip.

```text
5988ab91b6cb7efa7bf6a77f789b40929212280519be6c9daad56e01d5ceb218  list_columns.parquet
e64a64ff130c8dff64a6bc41480c51c87918d5e63bc75167b58524aa0fa01496  null_list.parquet
44f29191b5fa8cfe0ab848495bd8ef89344ac0d8f87b3dff12e267631e2b5c03  datapage_v2.snappy.parquet
065b336c65885ab9dfd97cf85ce39a45488ed12d0183917db0a11621b0711e3b  old_list_structure.parquet
2cb2cc0564486a28550429a8b6d0907bbb41e138546797bc91a4ebd850edd5a5  nested_lists.snappy.parquet
db1a493003a7dcd2011bf89e460fed007903fcdeb58f53df29387b4e908e2a6d  nested_maps.snappy.parquet
fcd6152058b8b8259a516105da5919b23cb8ccfc42258de0fe20e3107f8ef809  repeated_primitive_no_list.parquet
97d35acb9721e40fc0f66fba916a442c4a1cf77a35992dc891ad0cdcc5a24cfb  repeated_no_annotation.parquet
de9102a599d852be3af1d2af5d3498d8e019c329096a6f2d260f55ae2d6ed0ae  nullable.impala.parquet
e7927cde24c083e42a3d4b37ac962d34381f71c2d252169b627dd8459a5880e3  nonnullable.impala.parquet
5c4fc6c13fe7308acb2fd317a3bd59e5b9c9c206c005e863ae0a1abdbbf5e2ea  map_no_value.parquet
5591dde252b46bc238a88e9c02e35780c5eb086e2677df105aaa91ff1fde8fba  incorrect_map_schema.parquet
48427178bfef9e6edd9018f2ef7b084077c00057234a780271a8220ca53b33da  nested_structs.rust.parquet
1ce6839f093ebc0699b1e2769ed04036bab40405bacbb5dacdd376dd94c13451  large_string_map.brotli.parquet
```

The first thirteen fixtures are the materializable rewrite gate. This includes an
exact schema-bearing rewrite of `datapage_v2.snappy.parquet`.
`large_string_map.brotli.parquet` instead gets exact footer and schema checks,
exact small-value-leaf checks, and required rejection of the 1 GiB key page before
large decompression or value materialization under default limits. Its complete
high-memory read is an optional manual gate.

## Test ownership and independence

The reference model is test-only. It owns its schema AST, semantic values,
level calculation, and normalization. It must not call the production nested
planner, count pass, emit pass, zipper, or assembler when it computes expected
results.

The test fixture emitter may reuse the package's Compact Thrift scalar runtime,
page framing, compression, and primitive value encoders. Its schema layout and
repetition and definition streams must come from the independent model.

A production writer-to-production reader round trip is integration evidence only.
It does not prove either implementation independently. Writer conformance compares
the model streams directly with the production pre-encode leaf streams. A small
test-owned page and hybrid-level decoder then extracts repetition, definition, and
dense-value streams from serialized bytes without calling the production page,
level, column, or table reader. Reader conformance compares production assembly
with the independently generated streams and independent inverse assembler.

Maps use an ordered vector of pairs as their authoritative physical semantic
form. This preserves duplicate keys. A separate logical projection verifies the
specified last-value-wins behavior. JSON objects are not an authoritative map
manifest because they cannot preserve duplicate keys.

Generated tests use a small checked-in SplitMix64 implementation. They do not
use Julia's default random-number stream as a cross-version contract. Every
failure prints the seed, case ID, schema AST, semantic rows, and leaf streams.
The canonical manifest encoder uses only explicitly ordered arrays, fixed-width
integer text, and length-prefixed UTF-8. It must not use `hash`, `Dict` or `Set`
iteration order, `show`, or Julia Serialization.

## N5-A: deterministic compatibility model and goldens

Add a test-local model for primitive leaves, structs, lists, and maps. It must
support required, optional, and repeated nodes and produce:

- A flattened physical schema.
- Maximum repetition and definition levels for every leaf.
- Semantic rows.
- Exact repetition, definition, and dense-value streams.
- Package vector trees.
- Normalized semantic output.
- V1 and V2 serialized fixtures.

### LIST matrix

One mandatory golden set covers:

1. A repeated primitive.
2. A repeated group with multiple fields.
3. A repeated group with one repeated child.
4. A repeated one-field group named `array`.
5. A repeated one-field group named `<outer>_tuple`.
6. A rule-5 wrapper with a required child.
7. A rule-5 wrapper with an optional child.
8. A direct legacy LIST-of-MAP case.

Each applicable case contains null, empty, null-element, and present states.
Rule precedence is explicit:

- A multi-field `array` uses rule 2.
- A one-field `array` whose child is repeated uses rule 3.
- `Array`, `ARRAY`, and case-mismatched tuple names use rule 5.
- The exact `array` and `<outer>_tuple` names use rule 4.
- Modern-only, converted-only, and matching dual LIST annotations have the same
  semantic values.
- An unknown modern annotation blocks legacy LIST fallback.

The following semantic rows and leaf streams are binding. `null` is a null outer
list, and tuple syntax describes a structured element.

| Case | Semantic rows | Repetition | Definition | Dense values |
|---|---|---|---|---|
| Rule 1 and required-child rule 5 | `null`, `[]`, `[10]`, `[20,30]` | `[0,0,0,0,1]` | `[0,1,2,2,2]` | `[10,20,30]` |
| Rule 2 field `x` | `null`, `[]`, `[(1,null)]`, `[(2,20),(3,30)]` | `[0,0,0,0,1]` | `[0,1,2,2,2]` | `[1,2,3]` |
| Rule 2 field `y` | same rows | `[0,0,0,0,1]` | `[0,1,2,3,3]` | `[20,30]` |
| Rule 3 | `null`, `[]`, `[[]]`, `[[1,2],[],[3]]` | `[0,0,0,0,2,1,1]` | `[0,1,2,3,3,2,3]` | `[1,2,3]` |
| Rule 4 `array` | `null`, `[]`, `[(null)]`, `[(4),(null)]` | `[0,0,0,0,1]` | `[0,1,2,3,2]` | `[4]` |
| Rule 4 `<outer>_tuple` | `null`, `[]`, `[(7)]`, `[(null),(8)]` | `[0,0,0,0,1]` | `[0,1,3,2,3]` | `[7,8]` |
| Paired optional-child rule 5 | `null`, `[]`, `[null]`, `[4,null]` | `[0,0,0,0,1]` | `[0,1,2,3,2]` | `[4]` |
| Extended optional-child rule 5 | `null`, `[]`, `[null]`, `[5,null,6]` | `[0,0,0,0,1,1]` | `[0,1,2,3,2,3]` | `[5,6]` |
| Direct LIST-of-MAP key | `null`, `[]`, `[{}]`, `[{1=>10,1=>20},{},{2=>30}]` | `[0,0,0,0,2,1,1]` | `[0,1,2,3,3,2,3]` | `[1,1,2]` |
| Direct LIST-of-MAP value | same rows | `[0,0,0,0,2,1,1]` | `[0,1,2,3,3,2,3]` | `[10,20,30]` |

The binding rule-3 schema is the Parquet 2.13 compatibility example: the
repeated inner group itself carries a LIST annotation and contains one repeated
primitive. An otherwise identical unannotated group has an ordinary group type
whose field is an unannotated repeated list. It is a separate serialized control,
not the binding `LIST<LIST<INT32>>` golden. An external reader may report a looser
unwrapped interpretation only as diagnostic evidence.

The golden manifest stores these arrays plus exact schema elements, leaf paths,
and file hashes. Rule 4 and rule 5 intentionally include identical physical
streams under different wrapper names. Their different semantic shapes prove
that the name rule is applied.

### MAP grammar matrix

Generate the complete accepted grammar and its rejected neighbors. A normal MAP
outer group is required or optional and has exactly one repeated group child.
The accepted outer marker is a modern logical MAP, a legacy converted MAP, or a
standalone converted `MAP_KEY_VALUE`. A modern MAP annotation takes precedence
over any converted annotation, including a conflict. An unknown modern annotation
blocks converted fallback and leaves an ordinary group. Any other winning modern
annotation makes the field not a MAP and is classified under that annotation;
it rejects only when that annotation is itself illegal on the group or topology.
Matching and conflicting dual annotations are explicit cases.

The repeated entry is a group with one or two children. Its converted annotation
is absent or `MAP_KEY_VALUE`. Standalone outer `MAP_KEY_VALUE` plus an inner
`MAP_KEY_VALUE` marker is accepted as a tolerant read layout but is never emitted.
An absent entry annotation and an unknown future annotation are treated as
unmarked. A known non-MAP logical or converted annotation rejects the MAP layout.

The first entry child is the key. It is required or compatibility-optional, but
never repeated. The second child is the value. It is required, optional, or
absent, but never repeated. Entry, key, and value names do not control
interpretation. The matrix serializes every accepted combination of outer marker,
outer repetition, entry marker, key repetition, value presence, and canonical or
arbitrary names. It adds one minimized rejected case for every neighboring
topology or annotation rule.

This truth table is binding:

| Location | Metadata or shape | Result |
|---|---|---|
| Outer | logical MAP, any converted value | Accept as modern MAP; the modern annotation wins. |
| Outer | no logical annotation, converted MAP | Accept as legacy MAP. |
| Outer | no logical annotation, converted `MAP_KEY_VALUE` | Accept as standalone legacy MAP alias. |
| Outer | unknown modern annotation plus converted MAP or `MAP_KEY_VALUE` | Do not apply converted fallback; compile as an ordinary future group. |
| Outer | no logical or converted annotation | Not a MAP; compile as an ordinary struct. |
| Outer | no logical annotation plus unknown converted annotation | Not a MAP; compile as an ordinary future-compatible group and preserve metadata. |
| Outer | modern LIST plus converted MAP or `MAP_KEY_VALUE` | Not a MAP; classify as LIST because modern metadata wins. |
| Outer | modern VARIANT, EMPTY, or other valid future group metadata plus converted MAP | Not a MAP; classify as the winning ordinary/future group. |
| Outer | primitive-only modern annotation on a group | Reject because the winning annotation is illegal on a group. |
| Outer | no logical annotation plus converted LIST | Not a MAP; classify as LIST. |
| Outer | no logical annotation plus primitive-only known converted annotation | Reject because the winning annotation is illegal on a group. |
| Outer | required or optional repetition | Accept. |
| Outer | repeated repetition outside a parent LIST compatibility rule | Reject. |
| Outer | repeated repetition owned by a parent LIST rule 3 | Accept as the tolerant direct LIST-of-MAP form. |
| Entry | repeated group with one or two children | Accept. |
| Entry | primitive, non-repeated, zero-child, or three-or-more-child form | Reject. |
| Entry | no annotation | Accept as unmarked. |
| Entry | converted `MAP_KEY_VALUE` | Accept as marked. |
| Entry | an explicitly empty logical annotation | Accept as unmarked. |
| Entry | unknown future logical annotation, including one paired with converted `MAP_KEY_VALUE` | Accept as unmarked; modern unknown metadata blocks the converted marker and is preserved. |
| Entry | unknown converted annotation with no logical annotation | Accept as unmarked and preserve metadata. |
| Entry | any known logical annotation, including logical MAP, LIST, or VARIANT | Reject at the entry position. |
| Entry | any known converted annotation other than `MAP_KEY_VALUE` | Reject at the entry position. |
| Key | first child, required | Accept as specified MAP. |
| Key | first child, optional | Accept only as the named existing-file compatibility exception; every physical key must still be present. |
| Key | repeated or absent | Reject. |
| Value | second child, required or optional | Accept. |
| Value | absent | Accept as key-only MAP. |
| Value | repeated or followed by another child | Reject. |
| Names | canonical or arbitrary | Accept by position; names do not select roles. |

Keys and values may be primitive or recursive group shapes. Their internal schema
must independently satisfy the normal struct, LIST, MAP, repetition, and leaf
rules. Matching dual outer MAP annotations, conflicting converted metadata under
a modern MAP, and unknown-modern blocking each have serialized controls.

Every accepted layout gets a serialized value case. Every rejected neighboring
layout gets a schema failure case. Required fixed cases are:

- Standard optional MAP with optional values and duplicate keys.
- Positional MAP fields with arbitrary names.
- Standalone outer `MAP_KEY_VALUE`.
- Key-only MAP with an omitted value field.
- Compatibility-optional keys with every physical key present.
- A mutation with an actual null key, which must fail eagerly.
- A direct legacy LIST-of-MAP.

Accepted value cases include null and empty maps when the outer repetition
allows them, required and null values when allowed, duplicate keys, and arbitrary
field names. Actual null keys always fail. The physical ordered-pair result and
the logical last-value-wins projection are checked separately.

The following MAP rows and streams are binding:

- Standard optional MAP rows are `null`, `{}`, `{a=>null}`,
  `{a=>1,a=>2,b=>3}`, and `{c=>4}`. Repetition is
  `[0,0,0,0,1,1,0]`. Key definitions are `[0,1,2,2,2,2,2]` with
  dense keys `["a","a","a","b","c"]`. Value definitions are
  `[0,1,2,3,3,3,3]` with dense values `[1,2,3,4]`.
- A required key-only MAP has rows `{}`, `{k1}`, and `{k2,k2}`. Repetition
  is `[0,0,0,1]`. Key definitions are `[0,1,1,1]` with dense keys
  `["k1","k2","k2"]`.
- A compatibility-optional-key MAP has rows `null`, `{}`, `{a=>1}`, and
  `{b=>2,c=>3}`. Repetition is `[0,0,0,0,1]`. Key definitions are
  `[0,1,3,3,3]` with dense keys `["a","b","c"]`. Required-value
  definitions are `[0,1,2,2,2]` with dense values `[1,2,3]`. Changing one
  present key definition from `3` to `2` creates an actual null key and must
  fail eagerly.

Direct repeated MAP inside a compatibility LIST is a tolerant package extension.
The binding integer-key Julia case owns its exact ordered pairs and streams. A
separately identified Java fixture with the same LIST/MAP topology and required
UTF-8 keys must produce an inferred Avro `LIST<MAP>` schema and exact normalized
rows. This authorizes the disputed nesting only. The Java Avro type system cannot
authorize the integer-key domain, which remains low-level physical evidence.

N5-A gate:

- Every mandatory LIST and MAP layout has an exact V1 and V2 file test.
- Independent streams match production pre-encode writer leaf streams.
- The test-owned wire decoder recovers those streams from serialized pages.
- Production reader assembly matches the independent inverse assembler.
- Julia reads every fixture to the expected semantic value.
- Schema-bearing rewrites preserve the exact source schema and raw unknown
  fields.
- Repeated writes of package-owned fixtures are byte deterministic.

## N5-B: deterministic recursive properties

Generate 256 bounded valid cases with seed `0x4e355f4c49535435`.

- Depth is 1 through 6.
- Width is 1 through 4.
- Row count is 0 through 24.
- Collection length is 0 through 5.
- One case has at most 64 AST nodes, 16 physical leaves, 2,048 level entries,
  2,048 dense values, and 256 KiB of uncompressed primitive payload.
- Struct, every LIST rule, every accepted MAP form, and primitive leaves can
  occur recursively where the format allows them.
- Every optional container emits null, empty, and present states when row count
  permits.
- Every optional element or value emits null and present states when row count
  permits.
- Map batches include duplicate keys.

The generator records coverage counters. The test fails if any required node,
rule, repetition, null state, empty state, duplicate-key state, V1/V2 page form,
or row boundary is absent.

Candidate case IDs are the fixed integers 0 through 4,095. Each candidate derives
its stream from the binding seed and its ID. A candidate that exceeds a hard cap
is rejected without partial fixture output. The first 256 accepted IDs are the
suite. Failure to obtain exactly 256 cases is an error. This fixed attempt schedule
prevents runtime timing or rejection order from changing the suite.

For each case:

1. Generate the semantic rows and schema AST.
2. Shred them with the independent model.
3. Assemble those streams with the independent inverse model.
4. Require `rows == independent_assemble(independent_shred(rows))`.
5. Compare exact production pre-encode writer leaf streams.
6. Serialize and independently decode the page streams.
7. Read with the production reader and require its normalized table to equal the
   independent assembly.
8. Rewrite a schema-bearing table and compare exact source schema and values.
9. Repeat the write and compare bytes for deterministic package-owned output.

All cases run with alternating V1 and V2 pages on Julia 1.10 and current stable.
Mandatory goldens run with both page versions. A stable 32-case subset runs with
all six supported writer codecs.

N5-B gate:

- All 256 cases pass on both Julia versions.
- Coverage counters prove every required category ran.
- The six-codec subset passes exact Julia reads and external secondary checks.
- Failure output is sufficient to replay one case without the generator.
- The ordered canonical manifest for all 256 cases has one checked SHA-256 digest.
  Julia 1.10 and current stable must both reproduce it exactly.

## N5-C: mutation and resource hardening

Add hostile package-vector and Tables sources that mutate one fact between
inspection, count, validation, and emission. Cover:

- Length and axes.
- Child identity, order, count, and names.
- Validity bits, ranks, offsets, and terminal offsets.
- List and map entry counts.
- Map key presence and pair order.
- Payload sizes and logical conversions.
- Schema topology, annotations, repetitions, row counts, and leaf paths.
- Page entry counts, dense counts, fixed widths, and row-group counts.
- Dictionary ordering and advertised dictionary, data, and index offsets.

Add serialized mutations for invalid LIST and MAP child counts and repetitions,
zero-field repeated wrappers, annotation conflicts, null optional keys,
continuations above the parent repetition level, sibling boundary and occurrence
disagreement, dense underflow and overflow, split rows, and schema-bearing
rewrite topology changes.

Page-boundary tests follow the wire contract. A V2 page cannot split a logical
row. A V1 page must start at a row boundary when an OffsetIndex advertises it;
without an OffsetIndex, a legacy V1 continuation is accepted. Correctly framed
and checksum-valid INDEX_PAGE and unknown page frames before data are accepted
controls only in a chunk without a dictionary or when they occur after its
dictionary. Truncation, checksum failure, overlap, type/subheader mismatch, or a
false advertised offset rejects. A dictionary must be the first physical frame,
and every positive explicit dictionary offset remains exact.

The error policy is:

- Detectable structural mutation or inconsistent source objects throw
  `ArgumentError`. Same-shape, same-size value changes between passes are outside
  the promised mutation-detection contract.
- Malformed files throw `FormatError`.
- Resource exhaustion throws `LimitError` for the exact resource.
- No raw `BoundsError`, `OverflowError`, `InexactError`, `MethodError`, or
  other package-originated implementation error escapes validation.
- Failed private operations restore caller-visible buffers and operation-owned
  live-byte budgets to their entry state.

Mutation detection applies at observable package access boundaries. A successful
user callback must not mutate an unrelated object that the callback did not
return. A transient change that is fully restored before any dependent package
access is outside the detection contract. For an arbitrary `AbstractDict`, the
writer first materializes its complete ordered `Pair` sequence. It snapshots
directly exposed package vectors and package-owned view backings as they become
observable. After the terminal iterator callback returns `nothing`, every such
base or dynamically discovered source must still match its snapshot before the
writer traverses dependent values. A persistent mismatch throws `ArgumentError`.
The writer does not use `length(dict)` as dictionary authority.

Exceptions deliberately thrown by user `Tables`, vector, IO, or sink methods may
propagate unchanged. Package-detected source, format, and resource failures occur
before bytes are offered to a public sink. Once a sink method starts, arbitrary
short writes, disk failures, and user sink errors are not promised rollback.

Error precedence is deterministic. A directly visible negative, contradictory,
or impossible structural field produces `FormatError` before dependent resource
work. A valid nonnegative declared size or count above a limit produces
`LimitError` before payload parsing or allocation. A detectable source invariant
mismatch produces `ArgumentError` before a later resource request. If detection
itself requires an allocation, the reserve-before-allocation `LimitError` wins.
Tests combine malformed-plus-over-limit and mutation-plus-over-limit inputs to
bind these rules.

Test exact-limit success and limit-minus-one failure for materialized bytes,
metadata depth, container and schema width, physical leaf count, list and map
entries, prefix arrays, page count, row groups, and checked row, level, dense,
payload, offset, and frame arithmetic. Use virtual vectors and synthetic metadata
for overflow tests. Do not allocate multi-gigabyte inputs. The pinned large MAP
fixture proves rejection before large decompression or value materialization.
Wire-size and container-count limits use fixed expected values. Live materialized
memory minima are derived independently on each Julia runtime and then tested at
the derived minimum and one byte below it. Object-layout constants from one Julia
version are never used as a cross-version expectation.

Every `Limits` field has an applicable read and write matrix:

| Limit | Required paths |
|---|---|
| `max_footer_bytes` | Footer read, completed writer footer, preserved nested metadata. |
| `max_page_header_bytes` | V1/V2/dictionary/index/unknown page header read and writer header construction. |
| `max_page_bytes` | Compressed and uncompressed nested page read, page retry, dictionary page, and writer payload. |
| `max_page_index_bytes` | Cumulative nested OffsetIndex preflight, decode, and writer section. |
| `max_materialized_bytes` | Whole-table nested read, ordinary and provenance writes, test-model adapter, and rollback. |
| `max_schema_name_bytes` | Footer schema read, interning, ordinary write, and provenance rewrite. |
| `max_string_bytes` | Nested string/binary/logical JSON and BSON leaf read and write. |
| `max_decimal_bytes` | Nested DECIMAL conversion and fixed/byte-array read and write. |
| `max_container_elements` | Schema nodes, physical leaves, rows, entries, levels, pages, row groups, and indexes. |
| `max_metadata_depth` | Recursive schema, metadata, value conversion, reference model, reader, and writer. |

Each applicable cell proves reserve-before-allocation, exact-bound success,
one-smaller failure, and cleanup. Tests also cover Thrift Int32 page/count fields,
Int16 row-group ordinals, and checked Int64 rows, offsets, ranges, sizes, and sums.
Exact schema-name registry tests run in isolated Julia subprocesses so prior global
interning cannot change their boundary.

Production fixes follow minimized failing tests. Likely ownership is private
validation in `write_nested.jl`, shared with provenance validation only when the
same invariant applies. N5-C adds no public type or export.

N5-C gate:

- Every named mutation fails with the specified error class.
- Every exact boundary succeeds and its next smaller bound fails.
- Budget and output rollback tests pass on both Julia versions.
- Seeded hostile cases do not escape raw implementation errors.

## N5-D: durable Java and Rust oracles

Use this repository layout:

```text
test/conformance/n5/manifest.toml
test/conformance/n5/golden/parquet-java/
test/conformance/n5/golden/arrow-rs/
test/conformance/n5/expected/
test/conformance/n5/oracles/parquet-java/
test/conformance/n5/oracles/arrow-rs/
```

Each case records producer, version, commit, file SHA-256, flattened physical
schema, logical schema AST, ordered semantic pairs, leaf paths, maximum levels,
and exact repetition, definition, and dense-value streams.

The external tools run in one Linux/amd64-only OCI image. Its base is Eclipse
Temurin 11.0.28+6 at Linux/amd64 manifest digest
`sha256:ab2527b3c9b7c15bc88f60dec19b2aa39939a6e0045fb8f538eeecbd7af59c69`.
The bootstrap installs Apache Maven 3.9.8 from the official archive whose tarball
SHA-512 is
`7d171def9b85846bf757a2cec94b7529371068a0670df14682447224e57983528e97a6d1b850327e4ca02b139abaab7fcb93c4315119e6f0ffb3f0cbc0d0b9a2`.
It installs Rust 1.96.1 from the channel manifest whose SHA-256 is
`87eb76c53073e72b766083bed5530820694253b832a762d8385bda5759f03975`.

A networked bootstrap build resolves every Java GAV, records every artifact hash,
vendors the complete Arrow Rust source and crate graph, and records dependency
trees and checksums. It places the populated Maven repository and Cargo vendor
tree in the image. `test/conformance/n5/oracles.lock` records the resulting image
digest, Maven archive, Java artifact manifest, Rust channel manifest, Cargo vendor
manifest, and all upstream revisions. Updating it is a reviewed maintenance
action.

The locked image reference is
`ghcr.io/juliaio/parquet-jl-n5-oracles@sha256:<digest>`, where `<digest>` is the
required 64-hex image value stored in `oracles.lock`. A clean CI host may use the
network only to pull that exact public reference. It verifies the pulled
`RepoDigest` against the lock before execution. It then runs the container with
`--network=none`. Thus image acquisition is networked, while Maven, Cargo, fixture
generation, and oracle execution are offline.

The actual gate runs that verified image by digest with the network disabled. Maven uses
`--offline` and its image-owned repository. Cargo uses the checked `Cargo.lock`,
checked toolchain, `.cargo/config.toml` vendor replacement, `--offline`, and
`--locked`. A wrapper or lock file without the corresponding offline content is
not sufficient evidence.

The bootstrap and gate interfaces are fixed:

```sh
test/conformance/n5/bootstrap-oracles.sh --output n5-oracles.lock
test/conformance/n5/run-oracles.sh --lock n5-oracles.lock --network none
```

The bootstrap command may use the network and must reproduce the recorded
dependency manifests before a new digest is accepted. The run command must fail
if the image digest, corpus commit, fixture hash, toolchain, or offline dependency
is missing or different.

The N5 workflow pins every GitHub Action by full commit SHA. It uses a named
Linux runner only to start the locked container; all oracle processes and tools
run inside the container. Pull requests run the offline locked gate. A weekly and
manual job performs a clean networked bootstrap, compares its dependency manifests
and image contents with the lock, and then runs the offline gate. Every job uploads
the manifest, logs, schemas, case IDs, and failure evidence with `if: always()`.

The Java harness uses parsed physical schemas and raw `Group` verification for
legacy physical rows. It uses `AvroParquetReader<GenericRecord>` with
`AvroReadSupport` as its concrete high-level LIST/MAP semantic API. Inferred-schema
Avro checks run with
`-Dparquet.avro.add-list-element-records=false`; the default changes rule-5
wrappers into record elements and does not match the binding scalar-element rows.
The setting is recorded in oracle evidence. The binding rule-3 fixture retains
the inner LIST annotation required by the specification example. Both inferred
Java Avro and Arrow Rust `RecordBatch` reads must return the exact nested-list
rows without an explicit schema that changes the physical interpretation. An
unannotated near-neighbor is reported under a different case ID; Java/Rust
high-level differences for that non-binding control are diagnostic. The harness
also uses
low-level column and page readers to report
exact repetition levels, definition levels, dense values, page versions, and row
counts. Avro maps require UTF-8 string keys. The Java harness therefore owns a
separately identified UTF-8-key direct LIST-of-MAP fixture whose inferred Avro
schema and rows prove the legacy LIST-over-MAP nesting. The independent Julia
golden retains the binding integer keys and exact integer streams. Java does not
authorize the integer-key domain; its high-level projection is used only for the
nested shape and has the specified last-value-wins map behavior. Logical map
adapters are otherwise secondary because they can collapse duplicate keys or
reject optional-key and key-only compatibility cases.

The Rust harness uses its low-level Parquet writer for explicit repetition and
definition levels. Its low-level page and column reader reports the same physical
evidence. Arrow `RecordBatch` verification separately checks high-level schema and
row semantics where the Arrow type system can represent the layout.

Both harnesses:

1. Generate their owned fixtures.
2. Compare generated hashes and manifests with checked evidence.
3. Read Julia canonical files and schema-bearing rewrites.
4. Verify exact schemas, repetition and definition streams, dense values, page
   versions, ordered physical values, logical values where representable, row
   counts, and null and empty distinctions.
5. Emit machine-readable evidence with tool versions and case IDs.

Oracle responsibilities are separate:

| Fact | Required authority |
|---|---|
| Parquet 2.13 Thrift coverage, unknown fields, and raw-field re-emission | Julia generated-IDL and mutation tests. Parquet Java 1.17.1 embeds format 2.12 and is not an authority for post-2.12 fields. |
| Exact physical schema, page version, row count, repetition, definition, and dense streams | Independent Julia wire decoder plus Java and Rust low-level page/column evidence for their owned fixtures. |
| Canonical LIST/MAP logical rows | Java and Rust high-level readers where representable. |
| Optional-key, key-only, duplicate-key, and arbitrary-name physical layouts | Java and Rust low-level evidence plus ordered semantic manifests. |
| A disputed compatibility meaning such as direct LIST-of-MAP | At least one independent high-level Java or Rust interpretation; low-level agreement alone is insufficient. |
| Julia schema-bearing rewrite preservation | Julia exact metadata comparison plus external low-level schema and stream evidence. |

The manifest binds external ownership for every mandatory compatibility family:

| Mandatory cases | Required independent producer | Required readers and result |
|---|---|---|
| LIST rules 1 and 2 and both required/optional rule-5 forms | Parquet Java low-level fixture writer | Julia independent wire and production readers; Java low-level; Rust low-level where supported; Java Avro high-level normalized rows. |
| LIST rule 3 | Parquet Java low-level fixture writer using the specified inner LIST annotation, with an Arrow Rust low-level fixture as a cross-check | Julia independent wire and production readers; Java and Rust low-level; inferred Java Avro and Rust `RecordBatch` high-level exact rows. The unannotated near-neighbor has a separate case ID and diagnostic high-level outcomes. |
| Rule-4 `array` and `<outer>_tuple` | Parquet Java low-level fixture writer | Julia independent wire and production readers; Java and Rust low-level where supported; Java Avro high-level one-tuple rows. |
| Standard MAP and arbitrary entry/key/value names | Parquet Java low-level fixture writer | Julia independent wire and production readers; Java and Rust low-level; Java Avro high-level normalized rows. |
| Standalone outer `MAP_KEY_VALUE` | Parquet Java low-level fixture writer | Julia, Java, and Rust low-level readers where supported; Java Avro high-level map rows. |
| Duplicate keys | Arrow Rust low-level fixture writer | Julia, Java, and Rust low-level ordered pairs; Rust `RecordBatch` preserves entry order; last-value-wins projection checked separately. |
| Key-only MAP | Parquet Java low-level fixture writer | Julia, Java, and Rust low-level key order where supported; Julia checks the specified key-set or all-null-map meaning; Java Avro rejection and other high-level outcomes are diagnostic. |
| Compatibility-optional key with every key present | Pinned `incorrect_map_schema.parquet` plus Arrow Rust low-level fixture writer | Julia, Java, and Rust low-level where supported; high-level rejection or acceptance is recorded as diagnostic. |
| Direct legacy LIST-of-MAP | Parquet Java low-level fixture writer for both the binding integer-key physical case and a separately identified UTF-8-key semantic-authority case | Julia, Java, and Rust low-level where supported; inferred Java Avro on the UTF-8-key case must report `LIST<MAP>` with exact null, empty, empty-map, duplicate-key last-value-wins, and present-map rows. The Julia integer-key golden retains exact ordered pairs and streams; Java does not authorize its key domain. |
| Actual-null optional key mutation | Test-owned mutation of the external optional-key fixture | Julia must reject eagerly; Java and Rust low/high-level outcomes are diagnostic and do not make the invalid file valid. |

Every valid serialized gap has at least one external producer that does not use
the Julia reference model. A harness API that cannot expose one physical form is
recorded as unsupported evidence for that harness, not silently passed. The other
required producer and reader obligations remain binding.

Julia reads every Java and Rust fixture. Java and Rust read every Julia fixture
that their public or low-level API can represent. Low-level success proves the
physical contract only. A compatibility case that claims a disputed high-level
meaning also needs one independent high-level oracle to report that meaning; it
otherwise remains open.

Oracle jobs run in CI with the locked OCI image and networking disabled. They do
not silently skip when Java, Rust, the corpus, or a fixture is absent. PyArrow
25.0.1 and DuckDB 1.5.5 remain secondary triage oracles, not substitutes for the
required Java and Rust evidence.

N5-D gate:

- All required Java and Rust fixtures have checked hashes and semantic manifests.
- Julia reads and rewrites them exactly.
- Both external harnesses verify Julia output without an absence skip.
- CI preserves case IDs, versions, schemas, hashes, and failure evidence.

## Final N5 gate

N5 is complete only when:

- N5-A through N5-D are green on their required platforms.
- The thirteen materializable pinned nested fixtures pass exact read and rewrite
  checks, and the large MAP fixture passes its bounded schema, small-leaf, and
  pre-allocation rejection gate.
- Julia 1.10 and current stable pass the complete suite with the pinned corpus.
- The external oracle jobs pass from clean environments.
- Independent review finds no open high-severity correctness or resource issue.
- Only the Stage 4 nested-data ledger row moves to complete.

Passing N5 closes nested conformance. It does not make the complete Parquet 1.0
roadmap release-ready.
