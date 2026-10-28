# Migrating from 0.8

Parquet.jl 1.0 is a new implementation. It reads files that 0.8 cannot, such as
pyarrow 25's, writes every common Parquet type, and has a smaller, namespaced API. Only
`read_parquet` and `write_parquet` are still exported; they forward to the new functions
with a deprecation warning and will be removed in 2.0. `read_parquet` still takes 0.8's
`rows` (a range of rows), `use_threads`, and `batchsize` (ignored).

## Names

| 0.8 | 1.0 |
|:--|:--|
| `read_parquet(path)` | `Parquet.Table(path)` (a file or a dataset directory) |
| `read_parquet(path; rows, batchsize, use_threads)` | `Parquet.Table(path; scan=Tables.Scan(offset, limit), ntasks)`; `Parquet.File(path)` iterates row groups |
| `write_parquet(path, table; compression_codec="SNAPPY")` | `Parquet.write(path, table; compress=:snappy)` |
| `Parquet.File(path)` with `nrows`, `ncols`, `colnames`, `schema` | `Parquet.File(path)` with `DataAPI.nrow`, `Tables.columnnames`, `Tables.schema`; `Parquet.metadata` for the footer |
| `rowgroups`, `columns`, `pages`, `bytes`, `values` | `for table in Parquet.File(path)` (one table per row group) |
| `RecordCursor` | `Tables.rows(Parquet.Table(path))` |
| `BatchedColumnsCursor` | `for table in Parquet.File(path)` |
| `logical_timestamp`, `logical_string` | not needed: timestamps and strings read as typed values |
| `is_par_file(path)` | none; `Parquet.File(path)` throws `Parquet.FormatError` for a file that is not Parquet |
| `Parquet.use_mmap(false)` | none; `Parquet.Table` reads a path and releases it before returning |

## Writing changes

- `Parquet.write` returns the sink and writes any Tables.jl table: every type in the
  [type mapping](@ref "Type mapping") table, nested columns, `missing` anywhere, and
  pooled columns as dictionaries. 0.8 wrote `Bool`, `Int32`, `Int64`, `Float32`,
  `Float64`, and `String` columns.
- Files are written with pyarrow's defaults: snappy, dictionary encoding with a PLAIN
  fallback, statistics, page indexes, and row groups of 1,048,576 rows. 0.8 wrote no
  dictionaries or page indexes.
- `compression_codec="ZSTD"` is now `compress=:zstd`, and `compresslevel` sets the level.
- `Parquet.Writer` appends tables to one file, and `partitionby` writes a Hive-partitioned
  dataset; see [Writing](@ref).
- `DateTime` columns write as TIMESTAMP(MILLIS) not adjusted to UTC and read back as
  `Durations.Timestamp{Millisecond}`, which compares equal to the `DateTime`.

## Reading changes

- Strings read as `DataStrings.DataString`, timestamps as `Durations.Timestamp{P}` or
  `Durations.ZonedTimestamp{P,Z}`, INT96 as `Durations.Timestamp{Nanosecond}`, decimals as
  `DataDecimals.Decimal`, and INTERVAL as `Durations.Duration`.
- Lists read as `AbstractVector` rows, structs as `NamedTuple` rows, and maps as ordered
  `AbstractVector{<:Pair}` rows (`Dict(row)` makes a dictionary).
- Files written by 0.8 read as they are. 0.8 stored a page CRC of 0; reads check page
  CRCs only with `checksum=true`, which skips the zeros in files 0.8 wrote.
- `Parquet.Table` reads eagerly and releases the file before returning; `close` is not
  needed. `Parquet.read(path, DataFrame)` hands the columns to a sink without a copy.
