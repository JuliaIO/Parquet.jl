
struct Table <: Tables.AbstractColumns
    schema::Tables.Schema
    chunks::Vector
    lookup::Dict{Symbol, Int} # map column name => index
    columns::Vector{AbstractVector}
end

Tables.istable(::Table) = true
Tables.columnaccess(::Table) = true
Tables.columns(t::Table) = Tables.CopiedColumns(t)
Tables.schema(t::Table) = getfield(t, :schema)
Tables.columnnames(t::Table) = getfield(t, :schema).names
Tables.getcolumn(t::Table, nm::Symbol) = Tables.getcolumn(t, getfield(t, :lookup)[nm])
Tables.getcolumn(t::Table, i::Int) = getfield(t, :columns)[i]
Tables.partitions(t::Table) = getfield(t, :chunks)

"""
    read_parquet(path)

Returns the table contained in the parquet file in an Tables.jl compatible format.

Options:
- `rows`: the row range to iterate through, all rows by default.
- `batchsize`: maximum number of rows to read in each batch (default: row count of first row group).
- `use_threads`: whether to use threads while reading the file; applicable only for Julia v1.3 and later and switched on by default if julia processes is started with multiple threads.

One can easily convert the returned object to any Tables.jl compatible table e.g.
DataFrames.DataFrame via

```
using DataFrames
df = DataFrame(read_parquet(path; copycols=false))
```
"""
function read_parquet(path;
    rows::Union{Nothing,UnitRange}=nothing,
    batchsize::Union{Nothing,Signed}=nothing,
    use_threads::Bool=(nthreads() > 1))

    parquetfile = Parquet.File(path);
    kwargs = Dict{Symbol,Any}(:use_threads => use_threads, :reusebuffer => false)
    (rows === nothing) || (kwargs[:rows] = rows)
    (batchsize === nothing) || (kwargs[:batchsize] = batchsize)

    # read all the chunks
    chunks = [chunk for chunk in BatchedColumnsCursor(parquetfile; kwargs...)]
    sch = Tables.schema(chunks[1])
    N = length(sch.names)
    columns = length(chunks) == 1 ? AbstractVector[chunks[1][i] for i = 1:N] : AbstractVector[ChainedVector([chunks[j][i] for j = 1:length(chunks)]) for i = 1:N]
    return Table(sch, chunks, Dict{Symbol, Int}(nm => i for (i, nm) in enumerate(sch.names)), columns)
end
