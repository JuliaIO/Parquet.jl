# Parquet.jl 1.0 architecture

Parquet.jl 1.0 is a new implementation. It keeps the registered package identity and repository history. It does not keep the old runtime.

The detailed support policy and acceptance gates are in [`roadmap.md`](roadmap.md).

## Contract

The stable contract is Apache Parquet format 2.13.0 at peeled source commit `c47e2a66e88943fc46fde1b028a9432f14fdf5c0`. The release tag object is not used as a commit pin. Post-tag items such as ALP remain experimental. The reader accepts the stable encodings and codecs it supports, including deprecated encodings. The writer emits nondeprecated stable encodings and codecs. LZO and INT96 are out of scope in both directions and are not a release gate: the GPL-2 LibLZO package cannot be part of this MIT core, and INT96 is deprecated in the format. Complete format coverage is not claimed.

Every support claim needs four facts:

1. A valid file can be read.
2. A valid file can be written when the feature is writable.
3. Invalid input fails safely under explicit resource limits.
4. An independent implementation confirms interoperability.

## Layers

- `source.jl`: bounded byte-range input and explicit ownership.
- `thrift.jl`: pure Julia Compact Protocol with unknown-field preservation.
- `metadata/`: immutable code generated from the pinned Parquet IDL.
- `schema.jl`: physical, logical, and Dremel level interpretation.
- `plain.jl`, `rle.jl`, `delta.jl`, `bss.jl`, `dictionary.jl`: typed encoding kernels.
- `codecs.jl`: exact-size decompression charged to a resource budget first.
- `page.jl`: page framing, standard CRC32, and Data Page V1/V2.
- `logical.jl`, `logical_temporal.jl`, `logical_binary.jl`,
  `logical_decimal.jl`, `logical_json.jl`, and `logical_bson.jl`: validated
  physical-to-logical scalar conversion and bounded embedded-document syntax.
  `logical_decimal.jl` maps DECIMAL to the isbits `Decimals.Decimal{P,S,T}` and
  converts big-endian two's complement to machine integers without a bignum.
- `dremel.jl` and `vectors.jl`: nested assembly and owned or borrowed vectors.
- `read.jl` and `scan.jl`: bounded parallel decode and residual-safe pushdown.
- `write.jl`, `write_logical.jl`, and `logical_column.jl`: schema-aware row-group,
  page, and logical-value production.
- `dataset.jl`: partition discovery and schema unification.
- `crypto.jl`: Parquet encryption framing and key-provider interfaces.
- `variant.jl` and `geo.jl`: complete Variant and geospatial modules.

The core does not depend on Arrow.jl. Arrow interoperation belongs in an extension. The package has no exports. Users call the narrow API through `Parquet`.

## Correctness rules

- Preserve unknown enum values, unknown union members, field 32767, and unknown pages.
- Trust page headers. Do not treat footer encoding or offset lists as complete.
- Use standard CRC32. CRC32C is a different polynomial.
- Decode selected columns and columns referenced by filters.
- Prune only with a proof. Keep the exact filter as residual work.
- Treat absent statistics as unknown, not zero or empty.
- Apply the Parquet float, NaN, signed-zero, binary truncation, and `created_by` trust rules.
- Start nested V2 pages at repetition level zero.
- Preserve an existing table's scalar schema when a Julia runtime type cannot carry
  Parquet annotation details such as a time unit, UTC flag, or declared precision.
- Bound all metadata-directed allocation before it occurs.
- Never follow a `file_path` outside the opened dataset root.

## Delivery stages

1. Repository, IDL, feature ledger, CI, and architecture.
2. Compact Protocol, generated metadata, and mutation tests.
3. Primitive PLAIN/RLE reader and writer vertical slice.
4. All encodings and compression codecs.
5. Logical types, Dremel nesting, and nested writer.
6. Statistics, indexes, bloom filters, and `Tables.Scan`.
7. Modular encryption.
8. Variant and geospatial data.
9. Dataset behavior, performance, fuzzing, docs, and release gates.

Green unit tests do not make a release. Parquet 1.0 also requires the pinned parquet-testing corpus, PyArrow and DuckDB bidirectional checks, nightly parquet-java and arrow-rs checks, fuzzing, allocation checks, docs, licensing, and registered `Tables.Scan` support.
