# Datasets

`Parquet.Table` and `Parquet.read` read many files as one table when the source is a
directory (every file below it), a glob pattern such as `"lake/**/*.parquet"` (`*` and
`?` match within a path segment, `**` across segments), or a vector of paths or sources.
Files are read in path order. Names starting with `.` or `_` (`_SUCCESS`, `.crc` files,
`_metadata`) are skipped in directories and patterns.

`Parquet.Dataset(source)` opens such a source without reading column data: it reads
the footers (on up to `ntasks` tasks) and takes the keywords of `Parquet.Table`.
`Tables.schema(ds)` and `DataAPI.nrow(ds)` come from the footers, iterating yields one
`Parquet.Table` per row group of each file, and `Parquet.Table(ds)` reads it all.

## Hive partitions

A directory named `key=value` below the dataset's root, as pyarrow's
`write_to_dataset`, Polars' `partition_by`, DuckDB's `PARTITION_BY`, and
`Parquet.write(dir, table; partitionby)` write them, gives its files a column `key`:

- The column is `Int64`, `Float64`, `Dates.Date`, or `Bool` (`true` and `false`), the
  first of these that every value of the key parses as, and otherwise a string
  (`DataStrings.DataString`). Values are percent-decoded, `__HIVE_DEFAULT_PARTITION__`
  reads as `missing`, and an empty value is the string `""` (so `year=` next to
  `year=2024` makes `year` a string column), as in pyarrow and DuckDB.
- Partition columns follow the files' columns. A file column of the same name is
  replaced by the partition values, as pyarrow, Polars, and DuckDB do.
- A filter on a partition column skips the files whose values rule it out.
- `partitioning=nothing` reads the files' columns only.

```@example datasets
using Parquet, Tables, DataAPI

lake = joinpath(mktempdir(), "trips")
Parquet.write(lake, (year=[2024, 2024, 2025, 2025], fare=[7.5, 12.0, 3.25, 40.0]);
    partitionby=:year)
readdir(lake)
```

```@example datasets
recent = Parquet.Table(lake; filter=Parquet.colcmp(==, Parquet.col(:year), 2025))
Tables.columntable(recent)
```

```@example datasets
ds = Parquet.Dataset(lake)
(schema=Tables.schema(ds), rows=DataAPI.nrow(ds), parts=length(ds))
```

## Schema unification

Columns match by name across files, in the order they first appear:

- A column some files lack reads as `missing` in their rows.
- A column that is required in one file and optional in another, or all-null in one
  file, reads as `Union{Missing,T}`.
- A column whose files store types that widen without loss reads as the wider type, as
  `pyarrow.dataset` reads it: `Int32` and `Int64` as `Int64` (integers of mixed
  signedness as `Int64`), `Float32` and `Float64` as `Float64`, and millisecond and
  microsecond timestamps as microseconds.
- Other differences in element type are an `ArgumentError` that names the column and
  both files.

```@example datasets
mixed = mktempdir()
Parquet.write(joinpath(mixed, "a.parquet"), (id=[1, 2], name=["x", "y"]))
Parquet.write(joinpath(mixed, "b.parquet"), (id=[3], score=[0.5]))
Tables.columntable(Parquet.Table(mixed))
```

## Object stores

With CloudStore.jl loaded, `Parquet.Dataset(store, prefix; credentials)` lists the
objects whose keys start with `prefix` in a bucket or container and reads them as a
dataset, with partition directories below `prefix` (end it with `/`):

```julia
using CloudStore, Parquet

trips = Parquet.Table(Parquet.Dataset(bucket, "lake/trips/"; credentials);
    filter=Parquet.colcmp(==, Parquet.col(:year), 2025))
```
