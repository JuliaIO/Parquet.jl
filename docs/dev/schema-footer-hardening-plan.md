# Schema and footer hardening plan

This plan closes the remaining source, schema-topology, and footer resource
boundaries found during N5-C review. It adds no public type or export. Each
slice is accepted independently, but all four slices must pass before this
phase is complete.

## Review decision

Two read-only design reviews agreed on the defects and the exact-read
contract. They disagreed on source ownership and slice size. A third read-only
review resolved both points:

- Keep the existing adopt-on-success rule. A failed `File(custom_source)`
  leaves the source open. After `File` returns, it owns the source. A later
  `Table` failure closes it exactly once.
- Implement the work as four reviewed slices. Do not claim the phase complete
  until all four pass.

The requested Claude Fable 5 Max review was retried before this plan. It did
not start because the account reported an insufficient credit balance. No
Claude approval is claimed.

## Error and resource contract

Use this precedence:

1. Propagate an exception thrown by a user source callback unchanged.
2. Reject an invalid object returned by a source callback with `ArgumentError`.
3. Reject a directly visible malformed Parquet structure with `FormatError`.
4. Reject a valid declared size or count above a configured limit with
   `LimitError` for that resource.
5. A materialization limit may win only when validation needs that allocation.

An impossible footer length is malformed, even when it also exceeds
`max_footer_bytes`. All failed private operations restore their operation-owned
live-byte budget to its entry value.

## Slice 1: exact source reads and ownership cleanup

Add one private exact-read seam. It validates an authoritative source length,
checked nonnegative `Int64` ranges, an `AbstractVector{UInt8}` return, the exact
requested length, and `Base.OneTo` axes. It calls the adapter once and does not
copy the returned bytes. Route every internal source read through this seam:

- leading magic, one eight-byte trailer snapshot, and footer bytes;
- page-header windows and page payloads;
- offset-index bytes.

Check leading magic before trailer work. Check footer containment before the
footer byte limit. Close a path handle if file sizing or mmap setup fails.
Copied IO buffers retain their budget charge while their source is live and
release it on idempotent close. Caller IO remains open.

Tests cover short, long, wrong-element, shifted-axis, changing, and throwing
sources at every read site. They bind callback counts, exception identity,
path cleanup, copied-IO rollback, and all ownership states.

## Slice 2: iterative reader schema planning

Replace recursive flattened-schema parsing and semantic nested-plan compilation
with charged explicit work stacks. Preserve preorder validation, paths, leaf
ordinals, LIST compatibility order, MAP compatibility, and leaf ranges.
`_nestedplan` independently enforces `max_metadata_depth`; it does not trust the
limits used to create `Schema`. Temporary frames are released on success. Every
failure restores all operation-owned charges.

Tests bind exact depth and node limits, malformed-before-limit precedence,
precharged-budget rollback, and 50,000-node schemas without
`StackOverflowError` on Julia 1.10 and 1.12.

## Slice 3: names and writer topology

Validate all top-level names before permanent registry charging. Registry
updates are atomic and `_tablenames` releases temporary storage on failure.
Replace every reachable unbounded recursive writer topology walker, including
ordinary shape inference, canonical schema construction, semantic binding, and
schema-bearing provenance comparison and binding. Use operation-owned charged
frames with nonrecursive parameter types.

Tests cover duplicate and NUL precedence, isolated-process exact name limits,
deep ordinary and schema-bearing zero-row and one-row writes, source exception
identity, and exact budget rollback.

## Slice 4: exact bounded footer encoding

Count the exact Compact Thrift footer size. Check the `UInt32` wire bound,
then enforce `max_footer_bytes` before allocating the output. Reserve exact
storage, encode into fixed-size storage, and require the final position to
equal the count. Preserve raw unknown fields byte-for-byte.

Tests cover the exact byte limit and one byte below it, unknown fields, large
row-group metadata, unchanged output on failure, and exact budget rollback.

## Acceptance gates

For every slice:

- run focused tests on Julia 1.10 and 1.12;
- run `git diff --check` and forbidden raw-read or recursion scans;
- obtain an independent exact-hash read-only review;
- run the complete N5 suite on both Julia versions.

After slice 4, run the full package suite on both Julia versions and the fresh
offline Java and Rust oracle gate. Green unit tests alone do not establish
release readiness.

## Execution status

All four slices are closed as of 2026-08-23. Each slice passed focused tests on
Julia 1.10 and 1.12 and an independent exact-hash review. The final Slice 4
review found and resolved two P1 issues, then agreed with no open P0, P1, or P2
finding. The reviewed exact-footer files were `src/thrift.jl` at
`dde322ee5e58d329858527b5b9d0bf5b448e034a8e164388e37fe395a1357148`
and `src/write.jl` at
`36a504698047e6362aa781b83500896a611a9156db66c43fe191d276a4bc741e`.

The complete N5 suite passed 43,513 of 43,513 checks on both Julia versions.
The full package suite then passed on Julia 1.10 and 1.12 with the pinned
parquet-testing checkout. The Julia 1.10 validation exposed an undeclared TOML
test dependency only when it reached the external-fixture manifest. TOML is now
declared in the package test target, and the complete suite passed on the fresh
rerun.

The fresh canonical Java and Rust oracle gate ran in a Linux/amd64 container
with networking disabled and read-only repository and corpus mounts. It passed
all 352 input files: Java supported 286 and recorded 66 expected unsupported
files, Rust supported 348 and recorded 4 expected unsupported files, and 254
of 256 mappings had paired external success. No image was published and no
oracle lock was written.

The final Slice 3 review covered atomic name publication, iterative writer
topology, early signed metadata validation, bounded `O(n log n)` physical and
auxiliary range overlap checks, and primary source-exception preservation.
The reviewed ordinary writer files were `src/write_nested.jl` at
`834e633a4f3886db1e6098e2628541cd7480208881f8c1a3a1de412b2f509b99`
and `test/write_nested.jl` at
`fede0d5efeac436b97e4382be8b825fa36ee4722d59b2c25477591e2ebc93ad7`.
The final table and page-index files were `src/table.jl` at
`16facc1135f21e7d5ef8f835e25f5ffd0d70d0fd91665fbce37230478b3b554b`
and `src/page_index.jl` at
`94d9d292b0c50419b7a3e1602afc234065d65c8182ca6e9ab6aae81a6f0d671d`.
