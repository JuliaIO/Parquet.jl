
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

One can easily convert the returned object to any Tables.jl compatible table e.g.
DataFrames.DataFrame via

```
using DataFrames
df = DataFrame(read_parquet(path; copycols=false))
```
"""
function read_parquet(path)
    parquetfile = Parquet.File(path);

    # read all the chunks
    chunks = [chunk for chunk in BatchedColumnsCursor(parquetfile)]
    sch = Tables.schema(chunks[1])
    N = length(sch.names)
    columns = length(chunks) == 1 ? AbstractVector[chunks[1][i] for i = 1:N] : AbstractVector[ChainedVector([chunks[j][i] for j = 1:length(chunks)]) for i = 1:N]
    return Table(sch, chunks, Dict{Symbol, Int}(nm => i for (i, nm) in enumerate(sch.names)), columns)
end
