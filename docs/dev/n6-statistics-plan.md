# N6-A trusted statistics and column order plan

N6-A implements one private truth layer for row-group column statistics and
adds conforming statistics to new files. It does not prune any data. Column
indexes, bloom filters, scan predicates, and page pruning remain separate
slices.

## Review state

Two independent read-only audits selected this slice as the next actionable
Stage 5 boundary. Both found that later pruning work needs one trusted order and
comparison model first. They also agreed that connecting an unreviewed model to
pruning could silently remove matching rows.

The requested Claude Fable 5 Max review remains unavailable because the account
reported an insufficient credit balance. No Claude approval is claimed. This
plan must receive an exact read-only review before production files change.

## Prerequisites and incomplete evidence

The local schema and footer hardening phase is complete. The full package suite
passes on Julia 1.10 and 1.12. The canonical offline Java and Rust gate passes
352 files with 254 paired mappings.

This does not close the durable N5 release gate. No published oracle digest or
`oracles.lock` exists, CI does not run the locked oracle image, and the feature
ledger still records nested data as in progress. Publishing an image and
accepting a lock require separate authorization. N6-A may proceed without
changing frozen N5 evidence, but no Stage 4 release-complete claim is allowed.

LZO is also a separate format-completeness blocker. The available registered
implementation is not license-compatible with this MIT core. N6-A does not
change that boundary.

## Pinned authority

The normative source is Apache Parquet format 2.13.0 at commit
`c47e2a66e88943fc46fde1b028a9432f14fdf5c0`.

The implementation follows these rules from the pinned IDL and logical-type
specification:

- `min_value` and `max_value` have no defined meaning without a complete,
  leaf-aligned `column_orders` vector.
- Deprecated `min` and `max` always use signed comparison, independent of
  `column_orders`.
- Bounds use PLAIN bytes. A BYTE_ARRAY bound omits its length prefix.
- STRING, ENUM, UUID, JSON, BSON, raw BYTE_ARRAY, and raw fixed byte arrays use
  unsigned byte-wise order.
- Signed INTEGER, DATE, TIME, TIMESTAMP, and the matching physical integers use
  signed order. Unsigned INTEGER uses unsigned numeric order.
- DECIMAL compares the represented signed unscaled value. Fixed and variable
  byte-array DECIMAL values therefore do not use raw unsigned byte order.
- INTERVAL, UNKNOWN, VARIANT, GEOMETRY, GEOGRAPHY, LIST, MAP, and stable INT96
  have no type-defined min/max order.
- FLOAT, DOUBLE, and FLOAT16 may use `TYPE_ORDER`, but writers should use
  `IEEE_754_TOTAL_ORDER` for defined NaN, signed-zero, and payload ordering.
- A floating writer always emits `nan_count`. With IEEE total order, mixed
  bounds exclude NaNs. An all-NaN non-null set uses its total-order NaN extrema.
- Missing counts remain unknown. A present zero remains known.

Producer-version trust is compatibility policy, not wire-format authority. The
first checked rule is parquet-mr PARQUET-251. It applies only to physical
BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY bounds. Null, empty, or wholly unparsable
`created_by` values are untrusted for those bounds. A parsed non-parquet-mr
application is unaffected. A parquet-mr value without a semantic version is
untrusted. A parquet-mr version below 1.8.0 is untrusted, except for versions
greater than or equal to `1.5.0-cdh5.5.0` and less than `1.5.0`. Version 1.8.0
and later passes this rule. Parse failure is nonfatal, and counts remain
independent.

N6-A deliberately applies this conservative PARQUET-251 decision to both
modern and deprecated bound families. Pinned parquet-java applies the check in
its deprecated-bound fallback. Applying it to both families cannot create a
false exclusion and avoids trusting unusual modern bounds carrying an affected
producer identity. More producer rules enter only with pinned primary-source
evidence and direct boundary tests. A remembered cutoff is not sufficient.

A second compatibility rule is pinned to Apache Arrow commit
`515410b2a14ac766258e00b07eab9e5ee2692a62`, `cpp/src/parquet/metadata.cc`.
Parquet-cpp before 1.3.0 and parquet-mr before 1.10.0 produced unsafe
statistics when the selected comparator order was not signed; IEEE total order
is also non-signed for this rule. For either producer, a missing usable version
is conservatively in the affected range. Pinned Arrow compares only the numeric
version triplet. N6-A is deliberately stricter and treats prereleases of 1.3.0
and 1.10.0 as affected. Both bounds in the selected family become unknown
unless both are present with identical raw bytes, where order cannot change the
result. Apply this rule to both bound families. Apply PARQUET-251 independently
as the stronger rule for affected BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY bounds.
The equality exception never overrides PARQUET-251. Test both release-candidate
and final cutoff strings, including an affected producer paired with IEEE order.

## Scope

### Reader truth layer

Add `src/statistics.jl`. It defines private order, bound, count, and trust
states. It must not export a new name.

The layer receives the validated schema, file `created_by`, the leaf-aligned
column order, and `ColumnMetaData`. It returns these
facts independently:

- lower bound;
- upper bound;
- null count;
- NaN count;
- distinct count;
- bound exactness;
- declared order;
- producer trust decision and reason.

One absent or unusable fact does not erase unrelated valid facts. For example,
an absent lower bound does not erase a known null count.

Use these count and order rules:

- Validate `null_count`, `nan_count`, and `distinct_count` independently as
  nonnegative and no greater than `ColumnMetaData.num_values`. Never compare a
  nested leaf count with the row-group row count.
- `nan_count` is legal only for FLOAT, DOUBLE, and FLOAT16. On another leaf it
  is malformed metadata. When null and NaN counts are both present, add them
  with checked arithmetic and require the sum to be no greater than
  `num_values`.
- When null and distinct counts are both present, require `distinct_count` to
  be no greater than `num_values - null_count`.
- A negative count, an invalid checked count relationship, a short or long
  `column_orders` vector, IEEE total order on a non-floating leaf, or an invalid
  fixed physical width is malformed metadata and raises `FormatError` during
  explicit statistics validation.
- An absent `column_orders` vector makes modern bounds unknown. It does not make
  the file unreadable.
- An unknown future `ColumnOrder` member is preserved by the generated metadata
  layer and makes only that leaf's modern bounds unknown.
- `TYPE_ORDER` is a legal leaf-aligned placeholder when a leaf has no defined
  order. Its bounds are ignored. LIST and MAP annotations do not suppress the
  independently defined order of their descendant physical leaves.
- A producer known to have corrupt statistics makes the affected bounds
  unknown. Counts that are independently valid stay available.

Use these floating rules:

- A count arithmetic or domain violation always raises `FormatError`; it never
  degrades only the bounds. When known counts prove that no non-null value
  exists, every bound is unknown.
- Under floating `TYPE_ORDER`, ignore each NaN bound independently. Widen a
  `+0.0` lower bound to `-0.0`, and widen a `-0.0` upper bound to `+0.0`.
  Downgrade exactness on each widened side.
- Under `TYPE_ORDER`, counts that prove a nonempty all-NaN set make any present
  bound a format contradiction and invalidate both bounds in the selected
  family. With insufficient counts, ignore a NaN bound independently and keep
  an independently valid non-NaN bound.
- Under `IEEE_754_TOTAL_ORDER`, use a raw-bit total-order key. Do not use Julia
  `isless`. For only `+0.0`, both extrema are `+0.0`. For only `-0.0`, both are
  `-0.0`. When both occur, the lower bound is `-0.0` and the upper bound is
  `+0.0`.
- IEEE NaN bounds are trusted only when known null and NaN counts prove that a
  nonempty set of non-null values is entirely NaN. That state permits only NaN
  bounds. The extrema are the smallest and largest raw NaN patterns actually
  present. Never synthesize sentinel NaNs.
- An IEEE state with at least one proven non-NaN value permits only non-NaN
  bounds. A bound-kind contradiction in either side invalidates both bounds in
  the selected family. With insufficient counts, ignore each NaN bound
  independently and keep an independently valid non-NaN bound.

Use these bound-family and value rules:

- Treat modern and deprecated bounds as separate families. If either modern
  side is present, use only modern fields and never fill a missing modern side
  from deprecated metadata. Use deprecated fields only when both modern fields
  are absent, their signed order matches the leaf meaning, and the producer is
  trusted. A contradiction invalidates both bounds in the selected family.
- Validate one-sided bounds independently. An absent bound ignores its
  exactness flag. A present modern bound with an absent, false, or true flag has
  unknown, inexact, or exact status, respectively.
- Require exact PLAIN widths for BOOLEAN, INT32/FLOAT, INT64/DOUBLE, INT96,
  FLOAT16, and fixed byte arrays. A BOOLEAN byte must encode zero or one.
- Raw BYTE_ARRAY and fixed-byte bounds may contain arbitrary bytes. STRING and
  ENUM require valid UTF-8. JSON and BSON require valid documents. UUID and
  FLOAT16 require exactly 16 and 2 bytes. DECIMAL must fit its declared
  precision. Annotated integer bounds must fit their declared bit width. TIME
  bounds must fit the declared daily domain.
- A semantically invalid optional logical bound becomes unknown while its raw
  metadata stays preserved. A structurally wrong fixed width remains a
  `FormatError`. No unusable bound becomes evidence for exclusion.
- INT96 order remains undefined in N6-A. The format's recommended legacy
  comparator does not make it a stable type-defined order.

Add `Limits.max_statistics_value_bytes::Int64 = 4096`. It is a per-raw-bound
interpretation and writer-emission limit. Equality succeeds. A reader bound one
byte over remains footer-owned and preserved, but becomes unknown without a
copy, semantic parse, or `LimitError`. A writer bound one byte over omits both
bounds, but retains counts and column order. Check fixed-width structure before
the policy limit. Check an oversized variable bound before UTF-8, JSON, BSON,
or DECIMAL work. A negative configured maximum fails deterministically. Zero
disables nonempty bounds. A negative value raises `ArgumentError`. Validate it
at the reader and writer entry points before reader work, writer callbacks, or
destination mutation. `max_materialized_bytes` remains the cumulative live
allocation limit.

Implement PLAIN bound decoding without constructing a full column. Fixed
physical widths must match exactly. Variable BYTE_ARRAY bounds are the raw
bytes. Keep reader byte bounds as footer-owned references. Avoid BigInt in
DECIMAL comparison by comparing normalized signed two's-complement bytes.
Precharge each reader scratch allocation. Use one production comparator seam
shared by the reader, writer, and later ColumnIndex work. The independent test
model must use separate code, types, and decoding logic.

### Writer statistics

Add `src/write_statistics.jl` and a `statistics::Bool=true` keyword to both
public `Parquet.write` methods and `_encodefile`.

Compute each summary from the already validated, row-group-sliced
`WriteLeafPlan`. Do not call the source table, user vectors, map callbacks, or
conversion hooks again. Work is linear in present values. Fixed-width
accumulation allocates no memory per value after warm-up. Start the summary pass
only after `_nestedwritefields` has completed its terminal source barrier. The
row-group leaf values are then operation-owned and safe from later source
mutation.

When `statistics=true`:

- always emit an exact `null_count` as the leaf-entry count minus its dense
  non-null value count;
- emit `nan_count` for FLOAT, DOUBLE, and FLOAT16, including zero;
- emit modern `min_value` and `max_value` with exactness flags when the leaf has
  a defined order and at least one value that the selected order can bound;
- use `IEEE_754_TOTAL_ORDER` for FLOAT, DOUBLE, and FLOAT16;
- use `TYPE_ORDER` for all other leaves in the complete leaf-aligned
  `column_orders` vector;
- emit no min/max for undefined-order leaves;
- emit no deprecated `min` or `max` fields;
- omit both bounds when an exact encoded bound exceeds
  `max_statistics_value_bytes`; keep valid counts and column order;
- encode zero as the actual extrema under raw-bit IEEE total order. Only
  `+0.0` produces two `+0.0` bounds. Only `-0.0` produces two `-0.0` bounds.
  Both signs produce `-0.0` below `+0.0`;
- count NaNs only among dense non-null values. For an all-NaN non-null set,
  emit the smallest and largest NaN bit patterns actually present under IEEE
  754 total order;
- pass the live writer budget through statistics and `column_orders`
  construction. Reserve before retained bound-vector and metadata allocations,
  and restore the exact starting charge on failure.

When `statistics=false`, preserve the current no-statistics file behavior and
omit `column_orders`. Skip the summary scan and all related allocations. The
central N5 `n5productionencodedbytes` options and every frozen N5 output path
must pass this option explicitly. Add a forbidden-default scan. Do not rewrite
frozen fixtures or manifests.

Page-header statistics remain absent. N6-B will add complete ColumnIndex page
bounds and will decide whether page-header duplication provides enough value.
`SizeStatistics`, histograms, bloom filters, and geospatial statistics are not
part of N6-A.

## Tests

Add `test/statistics.jl` and `test/write_statistics.jl`. Include both from
`test/runtests.jl`. Add a separate `test/conformance/n6/` area. Do not edit the
N5 manifests, expected evidence, or golden files.

The focused matrix covers:

- every supported physical type and stable logical order;
- signed and unsigned integer disagreements;
- DECIMAL in INT32, INT64, BYTE_ARRAY, and FIXED_LEN_BYTE_ARRAY;
- raw and logical byte-wise types, embedded NUL, invalid UTF-8 raw bytes, and
  exact fixed widths;
- all 65,536 FLOAT16 bit patterns plus sampled Float32 and Float64 infinities,
  signs, zeros, subnormals, quiet and signaling NaNs, and distinct payloads;
- only positive zero, only negative zero, both zeros, mixed finite and NaN,
  all-NaN, missing-count, and contradictory-count states;
- empty, all-null, all-NaN, and mixed null, NaN, and ordinary values;
- absent, one-sided, inexact, oversized, contradictory, and wrong-width bounds;
- absent, short, long, unknown, and illegal column orders;
- absent, zero, negative, excessive, and contradictory counts;
- valid and invalid `distinct_count`, and illegal non-floating `nan_count`;
- trusted, affected, fixed, malformed, absent, and unrelated `created_by`
  strings, including exact 1.8.0 and CDH endpoints and prereleases;
- the cross-product of modern and deprecated family presence, parquet-cpp
  1.3.0 and parquet-mr 1.10.0 cutoffs, PARQUET-251, signed versus non-signed
  order, and identical versus distinct encoded bounds;
- nested leaf-entry counts and row-group slices;
- negative, zero, exact-limit, and one-byte-over statistics limits, including
  fixed-width-before-limit and variable-limit-before-semantic precedence;
- mutation between planning and emission;
- budget rollback and unchanged IO/path destinations on failure.

Add two-row-group no-pruning fixtures whose page bodies are identical while
statistics are absent, trusted, producer-untrusted, oversized, and semantically
unusable. Full `Table` values and pre-footer body-range reads must be identical
for every variant. Capture an instrumented production-source range trace and
read count, not only byte equality. N6-A must perform no statistics-based
row-group or page selection and must add no page-header statistics.

Freeze the independent model and case manifest before production code changes.
The model must not call the production comparator or bound decoder. Add a scan
that forbids such calls. For every emitted file, it proves that each applicable
decoded value is contained by its bounds and that each exact bound equals the
independent extremum. Seeded metadata mutations may fail or become unknown.
They must never produce a false trusted bound.

Add warmed allocation probes for zero and many row groups. Fixed-width summary
work must not allocate per value after warm-up. Retained growth may be
output-sized only.

Use the pinned corpus statistics fixtures, including:

- `floating_orders_nan_count.parquet`;
- `nan_in_stats.parquet`;
- `single_nan.parquet`;
- `float16_zeros_and_nans.parquet`;
- `float16_nonzeros_and_nans.parquet`;
- `binary_truncated_min_max.parquet`;
- `int96_timestamp_order.parquet`;
- the INT32, INT64, byte-array, fixed-byte, and DECIMAL statistics fixtures.

## Independent interoperability

Create N6-owned fixtures and evidence. Do not extend or regenerate frozen N5
evidence in place.

- Freeze an N6 capability matrix, source/toolchain manifest, fixture manifest,
  and evidence schema before production code changes. This is local reviewed
  test evidence, not publication of an oracle image or the repository
  `oracles.lock`.
- Generate a test-owned non-Julia Java Compact-Thrift footer scanner directly
  from the exact pinned Parquet 2.13 IDL with an Apache Thrift compiler and
  runtime whose versions and artifact hashes are fixed by the N6 manifest.
  Keep it separate from Parquet.jl's generated metadata, comparator, and
  production decoder. Run it in a separate JVM process with its own classpath
  so its generated `org.apache.parquet.format` classes cannot collide with
  parquet-java's embedded 2.12 classes. It inspects raw `column_orders`, bounds,
  exactness flags, counts, signed-zero bits, and NaN payload bits. Record these
  raw assertions separately from parquet-java's embedded 2.12 semantic API.
  Label this as raw 2.13 wire evidence, not parquet-java semantic support.
- Combine the raw scanner with the frozen independent model and pinned Apache
  2.13 statistics corpus for IEEE semantic evidence. No unsupported result may
  count as a pass.
- Parquet Java generates and checks readability, values, TYPE_ORDER, legacy
  bounds, and producer-version compatibility. Its pinned high-level API does
  not authorize IEEE total order or `nan_count`.
- Arrow Rust generates and checks readability, values, and supported
  TYPE_ORDER metadata. Its pinned metadata layer treats IEEE order as unknown
  and lacks field 9, so it does not authorize IEEE order, `nan_count`, or NaN
  payloads.
- PyArrow verifies logical values and its exposed row-group statistics.
- DuckDB verifies file readability and its Parquet metadata view.
- Every producer, toolchain, source revision, fixture, and evidence file is
  pinned and hashed.

The existing offline N5 gate is rerun unchanged after N6-A. The new N6 oracle
gate remains separate until its own image and lock are reviewed and authorized.

## Acceptance gates

1. Obtain two read-only plan reviews with no open P0 or P1 disagreement.
2. Freeze and review the independent model, case manifest, capability matrix,
   raw 2.13 scanner pins, and evidence schema. Do not edit production files
   before this gate passes.
3. Implement the reader truth layer and pass focused Julia 1.10 and 1.12 tests.
4. Obtain an exact-hash read-only review of the reader layer.
5. Implement writer statistics and pass focused Julia 1.10 and 1.12 tests.
6. Obtain an exact-hash read-only review of the writer layer.
7. Pass the independent N6 model and capability-specific Java, Rust, PyArrow,
   DuckDB, raw 2.13, and Apache corpus evidence.
8. Pass the complete N5 and package suites on Julia 1.10 and 1.12.
9. Pass no-pruning/read-count evidence, bounds-checking, generator freshness,
   warmed allocation probes,
   `git diff --check`, and forbidden-copy scans.
10. Rerun the canonical offline N5 Java and Rust gate with no change to frozen
   evidence.

Document the public `statistics` keyword and
`Limits.max_statistics_value_bytes`, including its 4096-byte default, before
N6-A closes.

## Completion boundary

N6-A closes trusted row-group statistics and writer column-order production
only. It does not close Stage 5. ColumnIndex content, bloom filters,
`Tables.Scan`, row-group or page pruning, LZO, encryption, Variant, geospatial
support, Dataset behavior, documentation beyond the required N6 API additions,
performance, CI publication, PkgEval, and release gates remain incomplete.
