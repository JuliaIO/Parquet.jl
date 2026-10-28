# Parquet.jl

A pure-Julia reader and writer for [Apache Parquet](https://parquet.apache.org/) files.
Files read as Tables.jl columns and any Tables.jl table writes, so it works with
DataFrames.jl, CSV.jl, Arrow.jl, and the rest of the ecosystem.

```julia
using Parquet, DataFrames

df = DataFrame(id=1:3, city=["Oslo", "Lima", "Pune"], fare=[7.5, 12.0, 3.25])
Parquet.write("trips.parquet", df)

Parquet.read("trips.parquet", DataFrame)                # every column
Parquet.Table("trips.parquet"; select=[:id, :fare],
    filter=Parquet.col(:fare) > 5)                     # only the rows and columns asked for

lake = mktempdir()
Parquet.write(lake, df; partitionby=:city)             # a Hive-partitioned dataset
Parquet.Table(lake)                                    # every file below a directory
```

- Reads the files pyarrow, Polars, DuckDB, Spark, and parquet-java write: nested lists,
  structs, and maps with nulls at any level, decimals, timestamps with time zones,
  INT96, Data Page V1 and V2, and every encoding and codec but LZO.
- Reads only what a read needs: the selected columns, and the row groups and pages that
  statistics, page indexes, bloom filters, and dictionaries do not rule out, on several
  threads.
- Writes statistics, page indexes, and dictionaries by default, with snappy, zstd, gzip,
  brotli, or lz4 compression, and Hive-partitioned datasets with `partitionby`.
- Reads paths, byte vectors, `IO`s, and (with CloudStore.jl) objects in cloud storage
  through ranged requests.
- Bounds the memory a corrupt file can make a read allocate (`Parquet.Limits`).

The [documentation](https://JuliaIO.github.io/Parquet.jl/stable) covers reading,
datasets, writing, the type mapping, limits and errors, and migrating from 0.8.

## License

Parquet.jl is available under the MIT license.
