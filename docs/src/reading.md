# Reading

## Read a table

`Parquet.Table(source)` reads a Parquet file into columns. A table is a
`Tables.AbstractColumns`: `table.price`, `table[:price]`, `table["price"]`, and
`Tables.getcolumn(table, :price)` return a column, `names(table)` lists them, and any
Tables.jl sink accepts it. `Tables.columns(table)` returns the table itself; use
`Tables.columntable(table)` when you need a `NamedTuple` of its columns.
The table owns its columns: nothing it returns refers to the file, the byte vector, or
the source it was read from. `Parquet.read(source, sink)` reads the same way and hands
the columns to a sink such as `DataFrame` without copying them.

```@example reading
using Parquet, Tables, DataFrames

dir = mktempdir()
path = joinpath(dir, "trips.parquet")
Parquet.write(path, (id=1:6, fare=[7.5, 12.0, 3.25, 40.0, 9.5, 21.0],
    zone=["A", "B", "A", "C", "B", "A"]); rowgroupsize=3)

table = Parquet.Table(path)
table.fare
```

```@example reading
df = Parquet.read(path, DataFrame)
```

A path, a byte vector, an `IO`, a [`Parquet.AbstractSource`](@ref), or (with
CloudStore.jl loaded) a `CloudStore.Object` is a source; a directory, a glob pattern, or
a vector of sources reads a [dataset](@ref Datasets). Column types are listed under
[Type mapping](@ref). Columns that share a name read as `a`, `a_1`, `a_2`, and so on, as
CSV.jl and DuckDB name them.

Strings read as `DataStrings.DataString`s, which refer to the column's decoded bytes;
`stringtype=String` reads them as `String`s, nested values included, at the cost of one
allocation per value. `copy(table)` gives a table with new, mutable columns.

```@example reading
Parquet.Table(path; stringtype=String).zone
```

## Choose columns and rows

`select` and `drop` choose columns (names, indices, a regex, a vector of them, or a
function `(i, name) -> Bool` as in CSV.jl), `filter` keeps the rows where a predicate is
`true`, and `limit` caps the row count. Only the columns a read selects or filters on are
decoded; the other column chunks are never read, so a column this package cannot decode
can be dropped to read the rest.

```@example reading
subset = Parquet.Table(path; select=[:id, :fare], filter=Parquet.col(:fare) > 10, limit=2)
Tables.columntable(subset)
```

Predicates are the `Tables.col` family: [`Parquet.col`](@ref) with `<`, `<=`, `>`, and
`>=` against numbers, strings, and characters, combined with `&`, `|`, and `!`;
[`Parquet.colcmp`](@ref) for equality and for other values such as dates and timestamps;
[`Parquet.colin`](@ref) for membership; and [`Parquet.isnull`](@ref) for missing values.

A literal must be of its column's kind: comparing a string column with a number is an
`ArgumentError` that names both types. A `Date`, `DateTime`, or `Durations.Timestamp`
compared with a UTC timestamp column (pandas' time-zone-aware data, DuckDB's
TIMESTAMPTZ) is that time in UTC. A column in another zone needs a
`Durations.ZonedTimestamp`, and the error says how to write one.

```@example reading
using Dates, Durations
events = joinpath(dir, "events.parquet")
Parquet.write(events, (id=1:4,
    at=[Durations.ZonedTimestamp{Microsecond,:UTC}(2025, 1, day, 12) for day in 1:4]))
recent = Parquet.Table(events; filter=Parquet.colcmp(>=, Parquet.col(:at), DateTime(2025, 1, 3)))
recent.id
```

A filter skips the row groups that column statistics, bloom filters, or dictionary pages
show cannot match, and the pages that the page index or page statistics rule out. It
decodes the filter columns first and the other columns only for the rows that match. A
`limit` decodes only the row groups and pages it reaches. Skipping never changes the
result: the filter still runs on every row that is read.

For renames or an offset, pass a `Tables.Scan` as `scan` instead of the keywords:

```@example reading
renamed = Parquet.Table(path; scan=Tables.Scan(select=(:id, :fare => :amount), offset=1,
    limit=3))
Tables.columntable(renamed)
```

## Read row groups

`Parquet.File(source)` decodes the footer and reads no column data. It takes the
keywords of `Parquet.Table`. `Tables.schema(file)` gives the columns a read returns and
`DataAPI.nrow(file)` the row count. Iterating the file reads one `Parquet.Table` per row
group, so a file is its own `Tables.partitions`, `file[i]` (or `file[end]`) reads row
group `i`, and `Parquet.Table(file)` reads them all; with a filter, row groups whose
statistics rule it out are skipped. A path stays mapped until `close(file)`.

```@example reading
file = Parquet.File(path; select=[:zone, :fare])
[sum(part.fare) for part in file]
```

```@example reading
close(file)
```

## Threads

Column chunks decode on up to `ntasks` tasks (default `Threads.nthreads()`);
`ntasks=1` reads on the calling task. The result is the same for every `ntasks`.
A source decodes on several tasks only when [`Parquet.concurrentreads`](@ref) allows it;
paths, byte vectors, and `IO`s do.

## Nested columns

Lists, structs, and maps read as typed rows, in [`Parquet.ListVector`](@ref),
[`Parquet.StructVector`](@ref), and [`Parquet.MapVector`](@ref) columns:

- A list row is a [`Parquet.ListValue{T}`](@ref Parquet.ListValue), a read-only
  `AbstractVector{T}` view of the column's elements; `copy(row)` gives a `Vector{T}`.
- A struct row is a `NamedTuple` of its fields, so `row.field` is type-stable. A struct
  whose field names repeat or number more than 64 reads as a
  [`Parquet.StructValue`](@ref) of `name => value` pairs instead; `row["name"]` looks one
  up.
- A map row is a [`Parquet.MapValue{K,V}`](@ref Parquet.MapValue), a read-only
  `AbstractDict{K,V}`: it iterates the map's `key => value` pairs in file order,
  duplicate keys included, and `row[key]`, `get`, and `haskey` find the last entry with
  that key, as `Dict(row)` keeps it.

Inside containers and DataFrame cells, list and map rows print as the plain values they
hold, `[1, missing]` and `Dict("a"=>1)`, without their element types.

Each level of a typed row nests one more type, and Julia compiles deep ones slowly: the
first read of structs 17 levels deep took 11 s with typed rows, and 32 levels over 15
minutes. A column nested more than 8 levels deep therefore reads with untyped rows at
every level, as Arrow.jl's do: structs as `StructValue`, lists as `ListValue{Any}`, and
maps as `MapValue{Any,Any}` (a first read of any depth then takes well under a second).

`T` includes `Missing` only when the schema allows null elements, and a null list,
struct, or map reads as `missing`. Copies and subsets of a nested column (`copy`,
`column[rows]`, `vcat`, `DataFrame(table)`) are plain `Vector`s of rows, so `push!` and
`setindex!` take plain vectors, named tuples, and dictionaries.

```@example reading
S = NamedTuple{(:name, :score),Tuple{String,Union{Missing,Int32}}}
nested = (
    profile=Union{Missing,S}[missing, S(("Ada", Int32(7))), S(("Ben", missing))],
    samples=[Int32[], Int32[1, 2], Int32[3]],
    attrs=[Dict("a" => 1), Dict{String,Int}(), Dict("b" => 2, "c" => 3)],
)
nestedtable = Parquet.Table(take!(Parquet.write(IOBuffer(), nested)))
(name=nestedtable.profile[2].name, samples=collect(nestedtable.samples[2]),
    c=nestedtable.attrs[3]["c"], attrs=Dict(nestedtable.attrs[3]))
```

The reader also accepts the legacy list and map layouts of older writers.

## Metadata

`DataAPI.metadata(table)` gives the file's key-value metadata, with the schema metadata
of its ARROW:schema (the footer's entry wins where both have a key),
`DataAPI.colmetadata(table, :price)` a column's (from the file's ARROW:schema, as
pyarrow and Polars store it, or from its column chunks), and [`Parquet.metadata`](@ref)
the raw footer: schema, row groups, encodings, statistics. A rewrite keeps both.
[`Parquet.statistics`](@ref) decodes one column's statistics in each row group.

```@example reading
Parquet.metadata(table)
```

```@example reading
Parquet.statistics(Parquet.File(path), :fare)
```

## Sources

- A path is memory mapped while it is read: `Parquet.Table` unmaps and closes it before
  returning, and `Parquet.File` keeps it mapped until `close(file)`. If another process
  truncates a mapped file while it is open, reads of the removed region terminate the
  process; no bounds check can prevent that. A directory is read as a dataset by
  `Parquet.Table` and `Parquet.Dataset`; `Parquet.File` of a directory is an
  `ArgumentError`.
- A byte vector is read in place and an `IO` is read to its end into memory first. Neither
  is closed or modified, and the table does not refer to either.
- A [`Parquet.AbstractSource`](@ref) supplies bytes by range, for storage the others do
  not cover. It implements [`Parquet.sourcelength`](@ref) and
  [`Parquet.readrange`](@ref), and [`Parquet.concurrentreads`](@ref) when ranges may be
  read on several tasks at once. A read takes the footer with one read of the source's
  last 64 KiB, then reads the column chunks it decodes, merging chunks less than 64 KiB
  apart into one range; a filtered or windowed read of a file with a page index reads
  only the pages it decodes. Parquet never closes a source.

```@example reading
struct CountingSource <: Parquet.AbstractSource
    bytes::Vector{UInt8}
    reads::Base.RefValue{Int}
end
Parquet.sourcelength(src::CountingSource) = length(src.bytes)
function Parquet.readrange(src::CountingSource, offset, count)
    src.reads[] += 1
    return src.bytes[(offset + 1):(offset + count)]
end

src = CountingSource(read(path), Ref(0))
Parquet.Table(src; select=[:fare])
src.reads[]
```

With CloudStore.jl loaded, a `CloudStore.Object` reads by ranged requests the same way:

```julia
using CloudStore, Parquet

object = CloudStore.Object(bucket, "data/part-0.parquet"; credentials)
table = Parquet.Table(object; select=[:id, :price])
```
