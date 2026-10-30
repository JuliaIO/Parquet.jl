# Contributing to Parquet.jl

## Architecture

The reader turns validated byte ranges into typed columns. The writer turns a
Tables.jl source into row groups, then writes the footer on close.

| Layer | Source | Responsibility |
|:--|:--|:--|
| Input and ownership | `source.jl`, `footer.jl` | Local mappings, owned buffers, ranged reads, footer framing, and cleanup |
| Wire metadata | `thrift.jl`, `metadata/parquet.jl` | Compact Protocol and generated Parquet metadata |
| Schema | `schema.jl`, `nested_schema.jl` | Physical validation and the logical struct/list/map interpretation |
| Values and storage | `logical*.jl`, `vectors.jl`, `arrow_schema.jl` | Exact scalar conversions, nested columns, and Arrow schema hints |
| Pages and encodings | `page.jl`, `column.jl`, `plain.jl`, `rle.jl`, `delta.jl`, `bss.jl`, `codecs.jl` | Page framing, decompression, and column decoding |
| Read planning | `table.jl`, `pushdown.jl`, `statistics.jl`, `bloom.jl`, `dataset.jl` | Selection, filters, row windows, and multi-file schema unification |
| Output | `writer.jl`, `write*.jl` | Schema inference, shredding, encoding, row-group streaming, and partitioned output |
| Optional packages | `ext/` | Package-specific conversions and storage adapters |

The generated metadata file is not edited by hand. Its generator and pinned format
definition are documented in [`thrift/README.md`](thrift/README.md).

## Contracts to preserve

- A table owns its decoded columns. Closing a file or returning from a read must not
  invalidate strings, nested rows, or columns already returned to the caller.
- Specialize kernels on column types, not whole file schemas. Keep names and column
  types out of the public table's type parameters to reduce schema-specific compilation.
- A filter must give the same rows as filtering the complete decoded table. Statistics,
  dictionaries, bloom filters, and indexes can exclude a range only when they prove it
  cannot match. Preserve missing-value semantics and distrust known producer bugs.
- Validate file-derived sizes before allocation or unchecked access. Reserve owned
  buffers and decoded output against the same operation budget, including concurrent
  tasks and dataset files. The default unlimited budget must remain cheap.
- Preserve exact values and units. A conversion that loses precision or exceeds the
  destination type must fail with a useful error.
- Regular-file path writes publish completed files. A failed single-file write must
  leave the previous destination intact and remove temporary output. Partitioned writes have
  file-level atomicity; they are not dataset transactions.
- Public APIs are namespaced. Tables.jl and DataAPI provide common table and metadata
  interfaces; add optional extensions only for behavior those interfaces cannot cover.

## Validation

Run tests with the pinned Apache corpus. An existing checkout can instead be supplied
through `PARQUET_TESTING_DIR`.

```sh
git clone https://github.com/apache/parquet-testing test/parquet-testing
git -C test/parquet-testing checkout 09f3cdbde45302f0f0c689c950e465e98a9df960
julia --project -e 'using Pkg; Pkg.test()'
julia --project --check-bounds=yes -e 'using Pkg; Pkg.test()'
julia thrift/generate.jl --check
```

The CI matrix covers the minimum Julia version, current release, prerelease, and
nightly, with Linux, Windows, Intel macOS, and Apple silicon jobs. Not every version
runs on every architecture. The interop workflow runs `test/interop/interop.py` and
`test/interop/interop.jl` in both directions with pinned PyArrow, Polars, and DuckDB.
For format changes, add a small external fixture or extend that interop suite; a
writer/reader round trip alone can conceal matching bugs.

Measure first-call compilation separately from warmed throughput. Report input size,
types, compression, thread count, Julia and comparison-tool versions, allocation and
peak-memory measurements, and machine load. Compare identical work and verify the
result before interpreting a timing change.
