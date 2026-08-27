# Stage 4 nested data agreement

This document defines the complete nested reader and writer design for Parquet.jl
1.0. It refines the Stage 4 roadmap. It is an implementation agreement, not a
claim that the work is complete.

The format target is Apache Parquet 2.13.0. The annotated tag ultimately peels to
source commit `c47e2a66e88943fc46fde1b028a9432f14fdf5c0`. The reader accepts all
specified legacy nested encodings. New ordinary writes use canonical modern
schemas.

## Review disposition

The design has four independent inputs:

1. The Parquet 2.13 nested-type rules and pinned Apache test corpus.
2. A reader review of recursive Dremel reconstruction.
3. A writer review of schema ownership and recursive shredding.
4. A representation review against the Arrow rewrite and Tables.jl.

A fresh Claude Fable 5 Max review was requested. The CLI returned `Credit balance
is too low`. No fresh Claude approval is claimed for this phase. Independent Codex
reviews supply the available adversarial gate. Production edits start only after
those reviewers accept this exact plan.

## Fixed decisions

- Physical page decoding continues to return one `LeafStream` per physical leaf.
- Raw `SchemaNode` trees remain an exact physical view.
- A second, context-aware plan normalizes LIST, MAP, struct, and repeated forms.
- Nested storage is package-owned and columnar.
- Nested scalar access uses non-allocating package-owned views.
- Plain Julia containers remain first-class writer input.
- `missing` is the only null value. `nothing` is not a null alias.
- Modern logical annotations take precedence over legacy converted annotations.
- New output uses canonical three-level LIST and MAP schemas.
- A schema-bearing `Parquet.Table` preserves every representable source schema
  when rewritten. Zero-field groups remain read-only until an independent writer
  accepts them.
- Nested vectors are structurally read-only in this phase.
- No new names are exported.

## Schema normalization

Add private semantic plan nodes for leaves, structs, lists, and maps. Each plan
stores its source node, parent and present definition thresholds, repeated-entry
level when present, ordered children, and contiguous descendant-leaf range.

Plan compilation happens before page reads or output allocation. It validates the
whole topology under `Limits`. It does not mutate the raw schema.

The message root and primitive nodes cannot carry LIST, MAP, or MAP_KEY_VALUE.
Reject those placements before group classification. Any modern annotation,
including an unknown future member, blocks every legacy fallback. This includes
legacy MAP_KEY_VALUE fallback.

The reader accepts an otherwise ordinary message root marked REQUIRED because
the pinned parquet-cpp corpus uses that historical form. It rejects OPTIONAL,
REPEATED, and unknown root repetition markers. Canonical output omits the marker.

Group annotation classification follows these rules:

| Group metadata | Semantic result |
| --- | --- |
| Modern LIST | LIST; validate LIST topology. |
| Modern MAP | MAP; validate MAP topology. |
| Other known modern type plus legacy LIST or MAP | Modern metadata wins; reject an invalid group/type combination. |
| Unknown modern type plus legacy LIST, MAP, or MAP_KEY_VALUE | Preserve as an ordinary group; do not apply legacy fallback. |
| No modern type plus converted LIST | Legacy LIST. |
| No modern type plus converted MAP | Legacy MAP. |
| Standalone converted MAP_KEY_VALUE | MAP compatibility alias. |
| Unannotated REPEATED outside LIST or MAP | Required list of required elements. |

Nested duplicate struct field names remain ordered and valid. Name-based access is
ambiguous, but positional access and schema-preserving rewrites remain exact.
Top-level duplicate names remain unsupported by `Parquet.Table` because its Tables
column object is a `NamedTuple`. Low-level schema and leaf APIs can still inspect
such a file.

A required leafless struct can be synthesized from its observable parent
occurrence count. An optional or repeated leafless group is rejected because its
presence or cardinality is not represented in any leaf stream.

A LIST, MAP, or MAP_KEY_VALUE group cannot normally be REPEATED. A repeated LIST
or MAP-compatible group, including a standalone MAP_KEY_VALUE alias, is accepted
only when it is the direct element selected by a legacy two-level LIST rule. Its
REPEATED marker belongs to the parent list, so the nested collection is a required
logical element. Reject every other repeated annotated collection.

One separate case is required inside an already classified MAP. Its sole entry
group must be REPEATED and may carry converted MAP_KEY_VALUE. That annotation is
only an entry marker. It does not create another map and is not subject to the
standalone placement rule.

### LIST compatibility

The repeated child of a LIST group is interpreted in this exact order:

1. A primitive is the required element.
2. A group with two or more fields is the required struct element.
3. A group with one REPEATED child is the required element. This preserves nested
   legacy lists.
4. A one-field group named exactly `array` is the required one-field struct
   element.
5. A one-field group named exactly `<outer-name>_tuple` is the required one-field
   struct element.
6. Any other one-field group is unwrapped. Its child repetition controls element
   nullability.

A zero-field repeated wrapper is invalid. Compatibility names are exact and
case-sensitive only in rules 4 and 5. Other wrapper and element names are ignored.

The canonical writer emits:

```text
<required-or-optional> group field (LIST) {
  repeated group list {
    <required-or-optional> element;
  }
}
```

The outer group carries modern LIST and converted LIST metadata.

### MAP compatibility

A MAP has one repeated entry group. The first entry child is the key. The optional
second child is the value. Names are not semantic.

- The key must be REQUIRED in canonical data and ordinary inferred output.
- An OPTIONAL key schema is accepted for the documented Presto, Trino, and Athena
  compatibility case.
- An actual null key is always invalid.
- A compatible OPTIONAL-key source still exposes a nonmissing key element type.
  Optionality stays only in source-schema provenance.
- A REPEATED key or value is invalid.
- A value may be REQUIRED, OPTIONAL, or omitted.
- An omitted value is exposed as `missing` while the schema retains that the child
  did not exist.
- Duplicate encoded keys remain ordered and preserved.
- Explicit conversion to `Dict` applies last-encoded-value-wins behavior.
- Entry groups with zero or more than two children are invalid.

The canonical writer emits:

```text
<required-or-optional> group field (MAP) {
  repeated group key_value {
    required key;
    <required-or-optional> value;
  }
}
```

The outer group carries modern MAP and converted MAP metadata. The canonical
middle group does not carry MAP_KEY_VALUE.

Ordinary writing rejects every zero-field group, including `NamedTuple{()}` and an
empty message. A schema-bearing rewrite also rejects a zero-field group until an
independent implementation proves an interoperable write. The reader may still
synthesize a required leafless struct when its occurrence count is observable.

## Runtime vectors

The package owns three read-only columnar containers:

- `ListVector`: zero-based offsets, optional validity, and one child vector.
- `StructVector`: ordered runtime names, an optional compact rank map, and child
  vectors aligned to present struct occurrences.
- `MapVector`: zero-based offsets, optional validity, a key vector, and an optional
  value vector.

Offsets use `Int32` until the flattened child count requires `Int64`. Constructors
check nonnegative monotonic offsets, terminal offsets, validity lengths, child
lengths, and checked conversion to `Int`. A null list or map has an empty span. A
present empty list or map also has an empty span. Validity is the only distinction.

An optional `StructVector` stores a zero-based prefix-rank vector with one entry
per logical row plus one. Each adjacent difference is zero for a null struct or
one for a present struct. Every child length equals the last rank. A required
`StructVector` omits the rank vector and every child length equals the struct
length. This keeps required child element types exact. It never fabricates a
required value under a null ancestor. Constructors, row-group concatenation, and
schema-bearing writes use this same compact invariant. The rank starts at zero and
uses the same checked `Int32`-to-`Int64` promotion as list and map offsets.

Indexing returns:

- `missing` for a null container or struct;
- `ListValue <: AbstractVector` for a present list;
- `StructValue` for a present struct; and
- `MapValue <: AbstractVector{<:Pair}` for a present map.

The views retain their owner and do not allocate the nested row. `collect` and
`copy` follow normal shallow Julia semantics. They remove only the outer package
view. A nested child view stays a view until the caller also collects it.

`StructValue` supports positional access. String or Symbol access succeeds only
when one field has that name. It throws for an absent or duplicate name. Ordered
pair iteration preserves empty and duplicate names. Explicit conversion to a
`NamedTuple` requires unique valid names.

`MapValue` is an ordered physical view. It indexes encoded entry positions and
iterates ordered pairs. It does not claim multimap semantics. It is not an
`AbstractDict`, because that contract cannot retain duplicate keys. The namespaced,
unexported `Parquet.maplookup(value, key[, default])` compares key content and
returns the last encoded match. Array `getindex` and `get` remain positional, so an
integer logical key cannot conflict with an encoded entry index.

`Dict(value)` is an explicit logical conversion. Package views implement recursive
content-based `isequal` and `hash` where the logical key has stable Julia hash
semantics. This includes supported scalar keys and recursive list or struct keys
whose descendants have stable content semantics. Conversion rejects a map-valued
key or any other unsupported composite key shape with an actionable error while
the ordered view remains lossless. Byte-array keys are copied before insertion.
Mutating a key obtained from the returned dictionary has Julia's normal unsafe
dictionary-key behavior.

Nested wrapper types stay namespaced and unexported. Tables.jl treats them as
scalar column elements. `Tables.columns(table)` remains the top-level `NamedTuple`.
`Tables.schema(table)` reports the exact nested wrapper element types, including
missingness. Nested wrappers do not themselves claim a Tables table or row contract.

## Reader algorithm

The reader has three layers:

1. Compile the raw schema into semantic plans.
2. Decode every descendant leaf once into a `LeafStream`.
3. Zip the aligned streams into package-owned vectors.

The zipper uses one cursor per descendant leaf. It does not trust one leaf as an
unchecked structural driver. For a node whose parent repetition level is `P`, a
level `rep > P` continues the current parent occurrence and `rep <= P` starts the
next occurrence. Deeper repeated levels are projected away before siblings are
compared. The zipper runs two passes:

1. Validate occurrence boundaries, optional presence, repetition projection,
   counts, limits, and final offsets.
2. Convert dense scalar values and fill exact-sized buffers.

State tests are relative to the parent plan:

- A definition below the parent threshold is an absent-ancestor placeholder.
- An optional node below its present threshold is null.
- A list or map below its repeated-entry threshold is empty.
- A list or map at or above that threshold has one or more entries.
- Required descendants under a null ancestor are placeholders, not corrupt data.
- A null or empty collection has one placeholder and no continuation.

For every shared struct or collection, all descendant leaves must agree on the
collapsed occurrence boundaries and node presence. Deeper repeated nodes are
collapsed before sibling repetition is compared.

Final validation proves:

- every top-level row begins at repetition zero;
- the metadata row count was assembled;
- every level entry was consumed once;
- every dense physical value was consumed once;
- no continuation follows a null or empty collection; and
- an optional-schema map key is present for every encoded entry.

The existing flat single-leaf path remains the fast path. Nested assembly happens
per row group. Parts concatenate once with checked offset rebasing.

## Writer architecture

The writer separates file schema ownership from physical leaf chunks. One
`WritePlan` owns:

- the flattened schema exactly once;
- the parsed `Schema` self-check;
- the top-level row count; and
- a `Vector{WriteRowGroupPlan}`. Each row group contains one chunk per physical
  leaf. N1 emits one row group for nonzero input and none for zero-row input.

The schema root child count is the number of top-level fields, not the number of
leaves. Leaf order is `schema.leaves` order.

Private recursive writer plans mirror leaf, struct, list, and map nodes. Ordinary
input inference uses declared types only:

1. Recognize supported scalar and logical types first.
2. Infer a struct only from a concrete `NamedTuple`.
3. Recognize package `StructVector`/`StructValue`, `ListVector`/`ListValue`, and
   `MapVector`/`MapValue` by semantic kind at every recursion depth. Map recognition
   occurs before the general vector rule.
4. Infer a map from a concrete `AbstractDict{K,V}`.
5. Infer a list from a concrete `AbstractVector{E}`, except byte vectors.
6. Use `Union{Missing,T}` for optionality at every non-key level.
7. Require a nonmissing MAP key type and reject every actual missing key.
8. Reject `Any`, unresolved abstract containers, heterogeneous structural unions,
   and unparameterized containers.
9. Do not inspect present values to guess structure.

This makes zero-row and all-null typed input deterministic. Scalar metadata that
requires aggregation, such as decimal precision, may still scan values. Ambiguous
all-null nested logical leaves use one explicit, namespaced recursive schema
wrapper in N3. That API gets a focused review and adds no exports.

### Recursive shredding

Allocate one mutable builder per physical leaf. Use a count pass followed by an
emit pass. The count pass performs checked arithmetic and all possible validation
before allocation. Concurrent mutation is unsupported. The emit pass revalidates
every safety-critical shape, count, scalar constraint, and resource bound. It
rejects a mutation that violates those facts. It does not claim to detect a
same-shape value change.

- A missing required node is an error.
- A missing optional node emits one absent marker to every descendant leaf.
- A present optional node increments definition before descent.
- A required struct descends into every child.
- An empty repeated node emits one empty marker to every descendant leaf at the
  current definition level, before the repeated increment.
- Every present repeated item increments definition once before descent.
- The first repeated item keeps the incoming repetition level.
- Later siblings use that repeated node's repetition level.
- A required leaf emits one level entry and one dense value.
- A missing optional leaf emits only its level entry.
- A present optional leaf increments definition and emits its dense value.

Every null or empty ancestor emits a marker to every descendant leaf. Each finished
builder passes through `LeafStream(...; expected_rows=rows)`.

### Schema-bearing rewrites

`Parquet.write(table::Parquet.Table)` treats `table.metadata.schema` as the
authoritative flattened schema. At the start of each write, it reparses those
elements into a fresh operation-owned `Schema`. It recursively compares that
fresh tree with `table.schema`, including every element, path, level, leaf
ordinal, child edge, and leaf ordering. It rejects any disagreement. All later
binding, path selection, level calculation, and shredding use only the fresh
tree. This prevents mutable `SchemaNode` vectors from changing the physical
write plan.

The provenance path does not re-infer legacy structure from element types. It
preserves:

- exact repetition shape and physical paths;
- source names and field IDs;
- logical and converted annotations;
- unknown Thrift fields and unknown modern annotations;
- legacy two-level and special-name LIST forms;
- unannotated repeated fields;
- standalone and middle MAP_KEY_VALUE forms;
- optional-key schema compatibility; and
- omitted map values.

The writer revalidates the vector tree against the fresh schema before both
passes. `table.rows` must equal every top-level vector length in both passes. A
legacy zero `FileMetaData.num_rows` sentinel is accepted only as source history;
the new footer records the actual `table.rows` value. Top-level duplicate names
remain outside the `NamedTuple` table boundary. Duplicate nested struct names
remain ordered and representable.

Each logical leaf converts with its exact preserved source `SchemaElement`.
Neither schema construction nor value conversion calls `_canonicalwriteelement`.
The count pass validates the exact logical-to-physical conversion and payload
size. The emit pass allocates and charges the exact dense physical vector and
each copied variable-width payload. Any semantic group with no physical leaf is
rejected before a level or dense buffer allocation.

`Parquet.write(Tables.columntable(table))` is an ordinary detached write and emits
canonical schemas.

### Leaf encoding and page boundaries

Internal leaf identity is always the physical schema leaf ordinal plus its exact
emitted path. A public encoding override may use an exact path tuple only when that
path selects one leaf. An ambiguous path is an error. A positive integer selects a
physical leaf ordinal and remains available when duplicate sibling names create
identical paths. For example:

```julia
(:orders, :list, :element, :price) => :delta_binary_packed
(:attributes, :key_value, :value) => :dictionary
```

Path tuple segments may be Symbols or Strings and normalize to Strings. Reject two
override keys that normalize to the same selector. Symbol and String keys remain
valid for flat one-segment columns. A top-level group alias is accepted only when
the group has one leaf. Dotted strings are not path syntax because field names may
contain dots. Schema-bearing rewrites match raw emitted physical paths, not
normalized semantic paths.

Schema inference happens once. Each physical leaf owns three zero-based prefix
arrays with `rows + 1` entries: level-entry offsets, dense-value offsets, and raw
physical payload-byte offsets. Every array starts at zero, is monotonic, and ends
at its exact final count. Every top-level row adds at least one level entry to
every leaf. Its first entry has repetition zero. After emit and before encoding,
the writer revalidates all three arrays against the level streams and dense
values. Every page slice uses matching entry, dense, and payload ranges.

`rowgroupsize` is a positive maximum count of
top-level rows. Its default is 1,048,576. `nothing` requests one row group. Row
groups split only at top-level row boundaries. Each leaf stores per-row
level-entry and dense-value offsets. Pages may have different boundaries between
leaves, but every nonempty data page begins at a row boundary with repetition
zero. Row-group ordinals are emitted only when every ordinal fits in `Int16`.

For a V2 page:

- `num_values` is the level-entry count;
- `num_nulls` counts definitions below the leaf maximum; and
- `num_rows` is verified against the actual zero-repetition count in the page,
  not copied from the requested row slice.

`pagesize` is a positive soft uncompressed-byte target with a 1 MiB default.
`nothing` requests one candidate page per leaf chunk. The estimate includes
levels and raw physical values. One complete row may exceed the soft target if it
remains under the hard page limit. An encoded candidate that exceeds a hard byte
or Int32 count limit splits in half at a top-level row boundary and retries. One
row that still exceeds the hard limit fails. `rowgroupsize` and `pagesize` never
split a nested row.

Whole-column counts retain checked global container and Julia index limits, but
they do not apply page `Int32` or `max_page_bytes` limits. The count pass tracks
each row's leaf-entry count and an encoding-specific safe lower bound for its
uncompressed payload. It resolves the permitted encoding candidates first. It
rejects a row before complete dense-buffer allocation when its entry count cannot
fit `Int32` or when every permitted encoding candidate has a lower bound above
the hard page limit. Final hard checks still use the actual encoded page.

Candidate encoding reports a private page-capacity failure only for page-byte or
page-count overflow. The recursive splitter catches only that failure. Every
failed attempt releases all temporary reservations before it splits. Other
`LimitError`, `ArgumentError`, validation, codec, and allocation failures
propagate unchanged.

Dictionary planning is per leaf chunk and row group. A chunk emits at most one
dictionary page, and it is the first page. Dictionary fallback is decided for the
whole leaf chunk. Data-page slices use their dense-value ranges to slice dictionary
indexes. A data-page split never rebuilds or changes that dictionary. Public
`:dictionary` and `dictionary=true` remain adaptive: an oversized dictionary page
may discard the complete dictionary candidate and use PLAIN. No public forced
dictionary mode exists in N4. Any future internal forced mode must fail instead
of falling back. Footer accounting sums `num_values`, compressed bytes, and uncompressed
bytes correctly. `num_values` sums level entries across data pages only. Compressed
and uncompressed chunk sizes include every dictionary and data page header and
payload. Row-group byte sizes sum those complete column-chunk totals. The footer
points `data_page_offset` at the first data page, sets `dictionary_page_offset`
only when present, records one dictionary page when present, and records the actual
count for each data page type and encoding in `PageEncodingStats`. Every row group
keeps physical column chunks in schema leaf order.

A zero-row table emits its schema and no row groups. It emits no column chunks,
dictionary pages, or data pages. PyArrow and DuckDB must accept this form before
the zero-row writer gate closes. A nonzero row group never emits an empty data
page. `pageindex=true` is the default. `pageindex=false` omits the index section
and both offset-index footer fields.

An offset-index reader requires offset and length to be both present or both
absent. A present length is positive. Checked offset-plus-length arithmetic must
end at or before the footer offset. Column-index offsets and lengths also form a
pair, and a column index is invalid without an offset index. Its range receives
structural validation but N4 does not decode it. All physical chunks, offset
indexes, and column indexes occupy one globally nonoverlapping set of ranges.
The cumulative page-index budget and shared live-byte budget are reserved before
reading or decoding. Compact Thrift decoding consumes the exact range with no
trailing byte.

Every `PageLocation` describes one data page and no dictionary page. Its offset is
an absolute file offset. Its positive `compressed_page_size` is the complete
serialized header-plus-compressed-payload frame and fits `Int32`. Locations are
strictly ordered, nonoverlapping, and contained in the column chunk. Their
`first_row_index` values start at zero, strictly increase, and remain below the
row-group count. Bounded header decoding at every recorded offset must confirm a
V1 or V2 data page and an exact frame end. When
`unencoded_byte_array_data_bytes` is present, it has one nonnegative value per
location and is legal only for a physical `BYTE_ARRAY` leaf. Readers validate
every advertised offset index before table values materialize. Writers serialize
indexes without padding in row-group then physical-leaf order.

The bounded validator walks the complete physical chunk. It requires positive
explicit dictionary and index-page offsets to identify their exact first matching
frames. The data offset also identifies the first data frame, except for the pinned
parquet-mr 1.10 legacy form: when the dictionary offset is absent or zero and the
first frame at `data_page_offset` is a dictionary, that data offset is a chunk-start
hint and the next data frame is the first data page. A dictionary is the first
physical frame. `INDEX_PAGE` and unknown page types are framed, checksum-validated,
and skipped when matching page locations. Offset-index locations remain exact for
this legacy form. Every V1/V2 data frame matches exactly one location, and all page
value counts sum to the column chunk `num_values`.

## Resource and error policy

Add `Limits.max_materialized_bytes::Int64` with a finite 2 GiB default. One shared
per-operation live-byte budget applies to the whole `Table` read or write. It does
not reset for each leaf or row group.

Add `Limits.max_page_index_bytes::Int64` with a finite 64 MiB default. Every
serialized or decoded offset index is charged to this limit and the shared live-
byte budget.

Budgeting begins with metadata decoding and semantic plan construction. Before the
first leaf allocation, preflight the checked aggregate sizes that metadata makes
predictable. Reserve schema nodes, semantic plans, object and array headers, empty
buffers, every repetition and definition array, dense buffer, page payload, offset
or rank array, validity bitmap, child vector, logical string or byte copy, and
row-boundary index before allocation. Variable-width data charges its actual
decoded size through the same shared budget. Temporary buffers release
reservations only after they are no longer live. Final materialized buffers remain
charged through construction.

Add `Limits.max_schema_name_bytes::Int64` with a finite 1 MiB default as a
process-wide cumulative intern ceiling. A module-owned registry, guarded by one
lock, tracks every unique top-level name that Parquet.jl has interned. Before any
`Symbol` construction, atomically reserve all new names with conservative charges
for string bytes, registry slots, object overhead, and symbol storage. Reservations
never release because Julia symbols never release. A caller may explicitly raise
the ceiling for a trusted large schema. The low-level `File`, raw `Schema`, and leaf
APIs do not intern file names and remain the safe non-interning path.

Compare and reject duplicate top-level names as Strings before reserving or
interning them. Reject a top-level name containing a NUL code point before intern
reservation because Julia cannot represent it as a `Symbol`; low-level access
remains available. All schema depth, node counts, leaf counts, flattened entries,
offsets, dense values, byte lengths, and allocation sizes also use checked
arithmetic and the other `Limits`. Invalid file structures raise `FormatError`.
User data that cannot satisfy the declared writer schema raises `ArgumentError`. A
configured resource boundary raises `LimitError` before the expensive allocation.

The footer-size bound walks the completed `FileMetaData`, including every row
group, column chunk, encoding statistic, index field, preserved schema field, and
unknown field. The writer reserves that bound before `Thrift.encode` can allocate
its footer output.

The implementation rejects malformed sibling alignment, incomplete stream
consumption, illegal continuations, actual null map keys, invalid LIST or MAP child
counts, invalid repetition kinds, and unobservable optional or repeated leafless
groups.

## Implementation slices and gates

### N1: plan and storage foundations

- Add semantic schema plans and the full LIST/MAP compatibility matrix.
- Add list, struct, and map vectors plus scalar views.
- Add the aggregate materialization and schema-name budgets.
- Refactor writer schema ownership without changing supported output.

Gate: focused schema/vector tests pass and every current test remains green.

### N2: recursive reader

- Add the two-pass multi-leaf zipper.
- Integrate all structs, lists, maps, and unannotated repeated forms.
- Concatenate nested row-group parts.

Gate: all pinned nested corpus fixtures except the deliberate multi-gigabyte limit
fixture match recorded values. Malformed alignment and optional-key null cases fail.

### N3: recursive canonical writer

- Add typed schema inference.
- Add recursive count and emit passes.
- Add canonical structs, lists, maps, and recursive combinations.
- Add exact leaf-path encoding selection.
- Add one namespaced recursive schema wrapper for empty or all-null nested ENUM,
  DECIMAL, TIME, and TIMESTAMP leaves.

Gate: Julia reads every generated file exactly. PyArrow and DuckDB read the values
and schemas for V1 and V2, required and optional states, and supported codecs and
encodings. Empty and all-null explicit logical leaves retain their exact annotation,
unit, adjustment flag, precision, and scale.

### N4: provenance, legacy rewrite, and splitting

- Preserve exact nested schemas through `Parquet.Table` rewrites.
- Add row-group and page splitting with row boundary indexes.
- Emit one Parquet `OffsetIndex` per physical leaf chunk. Each data page gets
  one `PageLocation` with its absolute file offset, its compressed header-plus-
  payload size, and its row-group-relative `first_row_index`. Locations increase
  by offset and strictly increase by first row. Each `ColumnChunk` records the
  serialized index through exact `offset_index_offset` and `offset_index_length`
  fields. Indexes are serialized after all column chunks and before the footer.
  They are excluded from column-chunk and row-group page-byte totals and charged to
  both page-index and shared allocation limits.
- Decode indexes only through their paired footer fields and verify every
  location against the bounded physical page frame.
- Validate source-schema and vector-tree agreement.

Gate: legacy physical schemas, field IDs, unknown fields, optional-key metadata,
omitted values, and duplicate entries survive schema-bearing rewrites. Every
nonempty data page starts at a row boundary with repetition zero. Every data page
has an exact independently verified `PageLocation`. The gate locates and decodes
each index only through its `ColumnChunk` footer offset and length.

### N5: full conformance and hardening

The detailed implementation agreement is in [`n5-plan.md`](n5-plan.md).

- Add generated cases for all five LIST rules and every MAP compatibility form.
- Add recursive property generation and mutation tests.
- Add allocation, depth, width, overflow, and hostile-input tests.
- Add parquet-java and arrow-rs checks for legacy cases not covered by PyArrow.

Gate: the complete Stage 4 nested ledger is green on Julia 1.10 and current stable.
No package-local round trip alone closes the gate.

## Required evidence matrix

Tests cover structs, lists, maps, lists of lists, lists of structs, structs with
collections, maps of structs, and lists of maps. Every depth includes null, empty,
and present states where the schema permits them. Required and optional leaves,
zero rows, all-null typed values, duplicate names, duplicate keys, omitted values,
and required structs whose optional leaves are all null are explicit cases.

Malformed cases cover annotations on primitives and the message root, unknown
modern annotations combined with every legacy collection annotation, repeated
LIST or MAP outside the legacy parent-list exception, zero-field writes, null map
keys, invalid child counts, and inconsistent sibling streams. Generated valid
cases include a legacy `LIST<MAP>`, complex map keys, all five LIST rules, and every
MAP entry layout.

The pinned Apache fixtures include `list_columns.parquet`,
`null_list.parquet`, `datapage_v2.snappy.parquet`,
`old_list_structure.parquet`, `nested_lists.snappy.parquet`,
`nested_maps.snappy.parquet`, `repeated_primitive_no_list.parquet`,
`repeated_no_annotation.parquet`, `nullable.impala.parquet`,
`nonnullable.impala.parquet`, `map_no_value.parquet`,
`incorrect_map_schema.parquet`, and `nested_structs.rust.parquet`.

For reader compatibility, a zero `FileMetaData.num_rows` is treated as an
unknown legacy sentinel when row groups contain rows. The checked sum of
`RowGroup.num_rows` becomes the table row count. Every nonzero footer count must
match that sum exactly. This narrow exception is required by Apache's
`repeated_no_annotation.parquet`, which was written by parquet-rs 0.3.0 with a
zero footer count and six declared row-group rows.

Use PyArrow and DuckDB for canonical interoperability. Use parquet-java for the
five legacy LIST interpretation rules. Use arrow-rs for optional map-key and
key-only map compatibility. Generated fixtures record producer versions and exact
expected schemas. The multi-gigabyte map fixture is a resource-limit test, not a
routine value-materialization test.
