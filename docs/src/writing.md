# Writing

## Write a table

`Parquet.write(sink, table)` writes any Tables.jl source to a path or an `IO` and returns
the sink. `Parquet.write(sink)` returns a function, so a table can be piped into it.

```julia
using Parquet, CSV

Parquet.write("trips.parquet", table)
CSV.File("trips.csv") |> Parquet.write("trips.parquet")
Parquet.write("trips.parquet", table; compress=:zstd, rowgroupsize=500_000)
```

A path is written to a temporary file in the same directory and moved over the path at
the end, so a failed write leaves an existing file intact. An `IO` is left open.

The keywords and their defaults, which match pyarrow's:

| keyword | default | |
|:--|:--|:--|
| `compress` | `:snappy` | `:zstd`, `:gzip`, `:lz4` (LZ4_RAW), `:brotli`, or `nothing` |
| `compresslevel` | `nothing` | the codec's level |
| `dictionary` | `true` | dictionary encoding, falling back to PLAIN (below) |
| `encoding` | `nothing` | an encoding for every column, or per column (below) |
| `rowgroupsize` | `1_048_576` | rows per row group; `nothing` for one per partition |
| `pagesize` | `1_048_576` | bytes per data page, which also holds at most 20,000 rows; `nothing` for one page per column chunk |
| `pageversion` | `:v1` | `:v2` writes data page V2 |
| `statistics` | `true` | min, max, and null counts per column chunk and page |
| `pageindex` | `true` | column and offset indexes, so readers skip pages |
| `checksum` | `false` | page CRC32 checksums |
| `bloomfilter` | `false` | `true`, or the columns that get bloom filters |
| `metadata`, `colmetadata` | the table's | file and column key-value metadata |
| `partitionby` | `nothing` | columns to split a Hive-partitioned dataset by |
| `ntasks` | `Threads.nthreads()` | tasks that encode column chunks |
| `limits` | `Parquet.Limits()` | resource limits |

## Column types

Each column's element type decides its Parquet type, including nested and `missing`
values; [Type mapping](@ref) lists every type in both directions.

## Compression and encodings

A column chunk is dictionary encoded while its dictionary stays under 1 MiB and makes the
chunk's first page smaller; otherwise its values are written PLAIN. A `PooledArray` or
`CategoricalArray` column writes the pool entries its rows use as the dictionary, without
hashing its values again.

`encoding` takes a Symbol for every column (`:plain`, `:delta_binary_packed`,
`:delta_length_byte_array`, `:delta_byte_array`, `:byte_stream_split`, `:rle`, or
`:dictionary`), or a `Pair`, `NamedTuple`, or dictionary from column names (or leaf path
tuples such as `("tags", "list", "element")`) to encodings. An explicit encoding replaces
dictionary encoding for that column.

```julia
Parquet.write("out.parquet", table;
    encoding=(id=:delta_binary_packed, price=:byte_stream_split), pageversion=:v2)
```

## Statistics, page indexes, and bloom filters

Each column chunk and each page records its minimum, maximum, and null count, and the
page index stores the per-page bounds where readers find them without reading pages.
`Parquet.Table(path; filter)`, pyarrow, DuckDB, and Polars use them to skip row groups and
pages. Float bounds follow the rules pyarrow writes by: NaN is never a bound (a chunk of
only NaN has none, and a chunk with a page of only NaN has no column index), a zero
minimum is -0.0, and a zero maximum is +0.0. `statistics=false` leaves out the
statistics, the column orders, and the column index; `pageindex=false` leaves out the
page index. A bound longer than `Parquet.Limits().max_statistics_value_bytes` (4096
bytes) drops that chunk's minimum and maximum, and page-index bounds of BYTE_ARRAY
columns are truncated to 64 bytes (strings between characters).

`bloomfilter=true` writes a split-block bloom filter for every column chunk except
BOOLEAN ones, and `bloomfilter=[:customer_id]` for the named columns. A reader uses a filter
to skip row groups that cannot hold a value it looks for. Each filter is sized from its
chunk's distinct values for a 1% false-positive rate.

## Write a file in parts

`Parquet.Writer` writes one file from several tables, such as batches that do not fit in
memory together. Each table adds one or more row groups; `close` writes the footer.

```julia
Parquet.Writer("out.parquet"; compress=:zstd) do writer
    for batch in batches
        Parquet.write(writer, batch)
    end
end
```

The first table fixes the column names, order, and types. Every column is nullable unless
`nullable=false`. Later tables convert to the first table's types exactly: an `Int32`
column fits an `Int64` one, but a value that would round, such as 2^53 + 1 into a
`Float64` column or 0.1 into a `Float32` one, is an error. `Parquet.write` streams a
`Tables.partitions` source the same way, one partition at a time, so memory holds one
partition (one `CSV.Chunks` chunk, for example) rather than the whole table.

## Partitioned datasets

`partitionby` writes a directory in the Hive layout that pyarrow, Polars, DuckDB, and
`Parquet.Table(dir)` read as one table:

```julia
Parquet.write("trips", table; partitionby=[:year, :month])
# trips/year=2024/month=1/part-0.parquet, trips/year=2024/month=2/part-0.parquet, ...
```

Each distinct key gets one file holding its rows, without the key columns. Keys are
integers, floats, Bools, `Date`s, timestamps, or strings; a timestamp is written as
pyarrow writes it (`2024-01-01 12:30:00.000`, `Z` when adjusted to UTC) and, as with
pyarrow, reads back as a string. Values are percent-encoded as pyarrow does
(all but `A-Z a-z 0-9 - . _ ~`), and `missing` is written as
`__HIVE_DEFAULT_PARTITION__`; a literal string with that reserved value is rejected.
Column names are also percent-encoded, including an initial `.` or `_` so dataset
readers do not skip them as hidden directories. An empty string is written as an empty value (`key=`) and
reads back as `""` in Parquet.jl, pyarrow, Polars, and DuckDB. Readers type key columns
from the values (`Parquet.Table` reads integers as `Int64`).
Every file has the first file's column types.

Parquet.jl and pyarrow decode escaped partition column names. DuckDB and Polars currently
leave those names encoded; use ordinary names without punctuation for the same names in
all four readers.

A directory that is not empty is an error, so a write never mixes old and new files.
`existing=:delete_matching` first deletes the partitions the write produces and keeps
the others, which replaces some partitions of a dataset. Each input partition (for
example each `CSV.Chunks` chunk) adds row groups to its keys' files. At most 128 files
are open at once, so with more keys than that a key's rows may continue in
`part-1.parquet` and on. Each file appears complete or not at all, but a failed write
can leave some partitions written.
Symbolic links below the output root are rejected before a partition is replaced.

## ARROW:schema

The writer ends the file's key-value metadata with an `ARROW:schema` entry generated from
the written columns, as pyarrow does. It carries what Parquet types cannot: the time zone
of a `Durations.ZonedTimestamp` column and the unit of a `Dates` period column. pyarrow,
Polars, and `Parquet.Table` restore those types from it. Writing a table read by
`Parquet.Table` keeps the file's schema and key-value metadata, but regenerates
`ARROW:schema` and drops `pandas` and Spark's row metadata, which describe the old
schema.

## Memory and threads

Row groups are encoded and written one at a time, so a write needs the input table plus
about one row group's encoded size. The column chunks of a row group are encoded on
`ntasks` tasks (default `Threads.nthreads()`, `ntasks=1` encodes on the calling task); the
bytes written do not depend on `ntasks`. The writer reads each value once and does not
copy the input's columns, except to narrow an abstractly typed column. Chunked columns
(SentinelArrays' `ChainedVector`, as Arrow.jl returns for multi-batch tables) are read
chunk by chunk.
