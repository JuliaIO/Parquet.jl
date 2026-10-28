# Limits and errors

## Errors

Reads and writes throw one of three package errors, or an `ArgumentError` for arguments
that cannot work (an unknown column in `select`, a directory passed to `Parquet.File`,
conflicting column types across a dataset's files):

- [`Parquet.FormatError`](@ref): the input is not a valid Parquet file, such as corrupt
  or inconsistent metadata, a page that does not decode, or (with `checksum=true`) a
  page CRC32 mismatch.
- [`Parquet.UnsupportedFeatureError`](@ref): the file is valid but uses something this
  package does not read, or holds a value the Julia type cannot represent. The message
  names it and, where there is one, the way to read the file.
- [`Parquet.LimitError`](@ref): a size or count exceeded a cap in `Parquet.Limits`. Its
  `resource` names the cap (for example `:page_bytes` for `max_page_bytes`), and the
  message names the field to raise.

```@example limits
using Parquet

bytes = take!(Parquet.write(IOBuffer(), (id=collect(1:10_000),)))
try
    Parquet.Table(bytes[1:(end - 100)])
catch err
    err
end
```

## Resource limits

Pass a [`Parquet.Limits`](@ref) to `Parquet.Table`, `Parquet.File`, `Parquet.Dataset`, or
`Parquet.write`:

| field | default | caps |
|:--|:--|:--|
| `max_footer_bytes` | 64 MiB | the footer |
| `max_page_header_bytes` | 16 MiB | one page header |
| `max_page_bytes` | 1 GiB | one page, compressed or not |
| `max_page_index_bytes` | 64 MiB | one column or offset index |
| `max_statistics_value_bytes` | 4096 | one written minimum or maximum |
| `max_string_bytes` | 256 MiB | one string or byte-array value |
| `max_decimal_bytes` | 1 MiB | one DECIMAL value |
| `max_schema_name_bytes` | 64 MiB | the new column names one operation may intern |
| `max_metadata_depth` | 128 | schema and nested-value depth |
| `max_materialized_bytes` | unlimited | the bytes one read allocates |
| `max_container_elements` | unlimited | row and element counts |

Counts a file declares (values, rows, list and map lengths) are checked against the bytes
that back them before memory is reserved for them, and declared page sizes are bounded by
`max_page_bytes`, so corrupt metadata fails with a `FormatError` instead of a large
allocation. Highly repetitive data, such as long runs of nulls or of one dictionary
value, still decodes to its full size; for untrusted files, set `max_container_elements`
or `max_materialized_bytes`. `max_materialized_bytes` counts what this package
allocates: the copy of an `IO` source, decompressed pages, and decoded values, but not a
byte vector or mapped path it reads in place, nor memory a custom source allocates.

```@example limits
try
    Parquet.Table(bytes; limits=Parquet.Limits(max_container_elements=1_000))
catch err
    err
end
```

Column names become `Symbol`s, which last for the rest of the process, so
`max_schema_name_bytes` bounds the new names one operation interns. Each counts 256 bytes
plus twice its length, so the default admits about 240,000 short names, more than a
file within `max_footer_bytes` usually holds.

Schema parsing does not recurse and accepts deep schemas under a raised
`max_metadata_depth`. Nested reading and writing recurse once per level: a read rejects
a schema deeper than 1024 levels with a `LimitError` (`:nested_read_depth`), while a
write is bounded only by `max_metadata_depth`, so it can produce a file the reader
declines. Ordinary nesting is far below either bound.

## Statistics

Reads use statistics, the page index, bloom filters, and dictionary pages only to skip
data a filter cannot match, and only where the column's sort order defines them. Bounds
that fail validation (physical width, logical range, or the known statistics bugs of a
writer) are ignored, as are the statistics of a column whose annotation its physical
type cannot carry. The writer bugs are binary statistics from parquet-mr before 1.8;
from parquet-mr before 1.10 and parquet-cpp before 1.3, statistics of unsigned integers
and of byte arrays, decimals stored as bytes among them, unless the minimum equals the
maximum; and Polars' float statistics.

## Not supported

Complete coverage of the format is not a goal. Reading these throws
`Parquet.UnsupportedFeatureError` rather than reporting an invalid file:

- Encrypted files (Parquet modular encryption).
- Column chunks stored in another file than the one read (a footer whose chunks name a
  `file_path`); read the file the chunk names.
- LZO compression, in either direction: every implementation is GPL-2 and this package
  is MIT.
- TIME values of 24:00:00, INTERVAL values past 2^31 - 1 months or days, INT96 values
  outside the chosen resolution's range, and DECIMAL values with more digits than their
  precision that the column's storage integer cannot hold ([Type mapping](@ref)).

These are read or written in a reduced form:

- VARIANT, GEOMETRY, and GEOGRAPHY values read as their storage (VARIANT's `metadata` and
  `value` bytes and any shredded `typed_value`; WKB bytes).
- INT96 is read but not written (a rewrite stores INT64 timestamps), and the deprecated
  LZ4 codec and BIT_PACKED encoding are read but not written; new files use `:lz4_raw`
  and RLE.
- `_metadata` and `_common_metadata` summary files are skipped, with every name that
  starts with `_` or `.`.

A data page encoding newer than the format version this package implements (2.13), such
as ALP, cannot be told from a corrupt page: it is a `FormatError` that names the column,
which can be dropped to read the rest.
